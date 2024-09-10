// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::Layout;
use std::io;
use std::io::IoSlice;
use std::ops::Deref;
use std::ops::DerefMut;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use databend_common_base::runtime::spawn_blocking;
use tokio::fs::File;

/// An aligned buffer used to perform io on a `DmaFile`.
#[derive(Debug)]
pub struct DmaBuffer {
    cap: usize,
    len: usize,
    align: usize,
    data: *mut u8,
}

unsafe impl Send for DmaBuffer {}

impl DmaBuffer {
    /// Allocates an aligned buffer.
    pub(crate) fn new(cap: usize, align: usize) -> DmaBuffer {
        let layout = Layout::from_size_align(cap, align).unwrap();
        let data = unsafe { alloc(layout) };
        Self {
            data,
            cap,
            align,
            len: 0,
        }
    }

    /// Sets the internal length of the buffer. The caller must ensure that the memory is
    /// initialized until `new_len` before calling.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.cap);
        self.len = new_len;
    }

    /// Returns the number of initialized bytes in the buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the capacity for this `DmaBuffer`.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Returns the remining capacity in the buffer.
    pub fn remaining(&self) -> usize {
        self.capacity() - self.len()
    }

    /// Returns a raw pointer to the buffer's data.
    pub fn as_ptr(&self) -> *const u8 {
        self.data as *const _
    }

    /// Returns an unsafe mutable pointer to the buffer's data.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data
    }

    /// Extends `self` with the content of `other`.
    /// Panics if `self` doesn't have enough capacity left to contain `other`.
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        assert!(other.len() <= self.remaining());

        let buf = unsafe { std::slice::from_raw_parts_mut(self.data.add(self.len()), other.len()) };
        buf.copy_from_slice(other);
        self.len += other.len();
    }
}

impl Deref for DmaBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.data, self.len()) }
    }
}

impl DerefMut for DmaBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.len()) }
    }
}

impl Drop for DmaBuffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.cap, self.align).unwrap();
        unsafe { dealloc(self.data, layout) }
    }
}

/// A `DmaFile` is similar to a `File`, but it is openened with the `O_DIRECT` file in order to
/// perform direct IO.
pub struct DmaFile {
    fd: File,
    alignment: usize,
    buf: DmaBuffer,
}

impl DmaFile {
    /// Attempts to open a file in read-only mode.
    // pub async fn open(path: impl AsRef<Path>) -> io::Result<DmaFile> {
    //     let options = OpenOptions::new();
    //     const O_DIRECT: i32 = 0x00040000;

    //     let file = options.read(true).custom_flags(O_DIRECT).open(path).await?;

    //     let statfs = fstatfs(&fd).await?;
    //     // TODO: the actual aligment may differ from the optimal io size? we should probably get
    //     // this information from the the device the file lives on.
    //     let alignment = statfs.f_bsize.max(512) as usize;
    //     Ok(DmaFile { fd, alignment });

    //     OpenOptions::new().read(true).open_dma(path).await
    // }

    /// Opens a file in write-only mode.
    pub async fn create(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .await?;

        open_dma(file).await
    }

    /// Aligns `value` up to the memory alignement requirement for this file.
    pub fn align_up(&self, value: usize) -> usize {
        (value + self.alignment - 1) & !(self.alignment - 1)
    }

    /// Aligns `value` down to the memory alignement requirement for this file.
    pub fn align_down(&self, value: usize) -> usize {
        value & !(self.alignment - 1)
    }

    /// Return the alignement requirement for this file. The returned alignement value can be used
    /// to allocate a buffer to use with this file:
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    pub fn buffer(&self) -> &DmaBuffer {
        &self.buf
    }

    pub fn mut_buffer(&mut self) -> &mut DmaBuffer {
        &mut self.buf
    }

    fn write_direct(&mut self) -> io::Result<usize> {
        let rt = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                self.buf.as_ptr().cast(),
                self.buf.capacity(),
            )
        };
        unsafe { self.buf.set_len(0) }
        if rt >= 0 {
            Ok(rt as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    fn truncate(&self, length: usize) -> io::Result<usize> {
        let rt = unsafe { libc::ftruncate64(self.fd.as_raw_fd(), length as i64) };
        if rt >= 0 {
            Ok(rt as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub async fn close(self) -> io::Result<()> {
        todo!()
    }
}

async fn open_dma(file: File) -> io::Result<DmaFile> {
    let statfs = fstatfs(&file).await?;
    // TODO: the actual aligment may differ from the optimal io size? we should probably get
    // this information from the the device the file lives on.
    let alignment = statfs.f_bsize.max(512) as usize;
    let buf = DmaBuffer::new(alignment, alignment);
    Ok(DmaFile {
        fd: file,
        alignment,
        buf,
    })
}

async fn fstatfs(file: &File) -> io::Result<libc::statfs> {
    let fd = file.as_raw_fd();
    asyncify(move || {
        let mut statfs = std::mem::MaybeUninit::<libc::statfs>::uninit();
        let ret = unsafe { libc::fstatfs(fd, statfs.as_mut_ptr()) };
        if ret == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(unsafe { statfs.assume_init() })
    })
    .await
}

async fn asyncify<F, T>(f: F) -> io::Result<T>
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            "background task failed",
        )),
    }
}

pub async fn dma_write_file_vectored<'a>(
    path: impl AsRef<Path>,
    bufs: &'a [IoSlice<'a>],
) -> io::Result<usize> {
    let mut file = DmaFile::create(path.as_ref()).await?;

    for buf in bufs {
        let mut buf = &buf[..];

        while !buf.is_empty() {
            if file.buffer().remaining() == 0 {
                file = asyncify(move || file.write_direct().map(|_| file)).await?;
            }

            let dst = file.mut_buffer();
            let remaining = dst.remaining();
            let n = buf.len().min(remaining);
            let (left, right) = buf.split_at(n);
            dst.extend_from_slice(left);
            buf = right;
        }
    }

    let file_length = bufs.iter().map(|buf| buf.len()).sum();
    let dst = file.mut_buffer();
    if dst.remaining() > 0 {
        unsafe { dst.set_len(dst.cap) }
        file = asyncify(move || file.write_direct().map(|_| file)).await?;
        asyncify(move || file.truncate(file_length).map(|_| file)).await?;
    }

    Ok(file_length)
}

// fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
//     let data_len = buf.len();
//     if self.n != 0 {
//         let end = self.n + buf.len();
//         if end < self.buf.len() {
//             self.buf[self.n..end].copy_from_slice(buf);
//             self.n = end;
//             return Ok(buf.len());
//         } else {
//             let r = self.buf.len() - self.n;
//             self.buf[self.n..].copy_from_slice(&buf[..r]);
//             let n = self.write_direct(&self.buf)?;
//             assert_eq!(n, self.buf.len());
//             self.n = 0;
//             buf = &buf[r..];
//         }
//     }
//     while buf.len() >= SIZE_OF_BLOCK {
//         let r = buf.len() & ALIGN_SIZE_OF_BLOCK;
//         let n = self.write_direct(&buf[..r])?;
//         buf = &buf[n..];
//     }
//     if !buf.is_empty() {
//         self.buf[0..buf.len()].copy_from_slice(buf);
//         self.n = buf.len();
//     }
//     Ok(data_len)
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_xxx() {
        let data1 = b"aaa";

        let bufs = vec![IoSlice::new(data1)];
        let length = dma_write_file_vectored("./test_file", &bufs).await.unwrap();

        println!("{length}");
    }
}
