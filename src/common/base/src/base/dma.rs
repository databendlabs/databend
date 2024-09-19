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
use std::io::SeekFrom;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Range;
use std::os::fd::BorrowedFd;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use rustix::fs::OFlags;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;

use crate::runtime::spawn_blocking;

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
    fn new(cap: usize, align: usize) -> DmaBuffer {
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

    /// Returns the remaining capacity in the buffer.
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

/// A `DmaFile` is similar to a `File`, but it is opened with the `O_DIRECT` file in order to
/// perform direct IO.
struct DmaFile {
    fd: File,
    alignment: usize,
    buf: Option<DmaBuffer>,
}

impl DmaFile {
    /// Attempts to open a file in read-only mode.
    async fn open(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        let file = File::options()
            .read(true)
            .custom_flags(OFlags::DIRECT.bits() as i32)
            .open(path)
            .await?;

        open_dma(file).await
    }

    /// Opens a file in write-only mode.
    async fn create(path: impl AsRef<Path>) -> io::Result<DmaFile> {
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags((OFlags::DIRECT | OFlags::EXCL).bits() as i32)
            .open(path)
            .await?;

        open_dma(file).await
    }

    fn set_buffer(&mut self, buf: DmaBuffer) {
        self.buf = Some(buf)
    }

    /// Aligns `value` up to the memory alignment requirement for this file.
    pub fn align_up(&self, value: usize) -> usize {
        align_up(self.alignment, value)
    }

    /// Aligns `value` down to the memory alignment requirement for this file.
    #[allow(dead_code)]
    pub fn align_down(&self, value: usize) -> usize {
        align_down(self.alignment, value)
    }

    /// Return the alignment requirement for this file. The returned alignment value can be used
    /// to allocate a buffer to use with this file:
    #[allow(dead_code)]
    pub fn alignment(&self) -> usize {
        self.alignment
    }

    fn buffer(&self) -> &DmaBuffer {
        self.buf.as_ref().unwrap()
    }

    fn mut_buffer(&mut self) -> &mut DmaBuffer {
        self.buf.as_mut().unwrap()
    }

    fn write_direct(&mut self) -> io::Result<usize> {
        let buf = self.buffer();
        match rustix::io::write(&self.fd, &buf) {
            Ok(n) => {
                debug_assert_eq!(n, buf.len());
                unsafe { self.mut_buffer().set_len(0) };
                Ok(n)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn read_direct(&mut self) -> io::Result<usize> {
        let Self { fd, buf, .. } = self;
        let buf = buf.as_mut().unwrap();
        unsafe { buf.set_len(buf.capacity()) };
        match rustix::io::read(fd, buf) {
            Ok(n) => {
                unsafe { buf.set_len(n) };
                Ok(n)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn truncate(&self, length: usize) -> io::Result<()> {
        rustix::fs::ftruncate(&self.fd, length as u64).map_err(|e| e.into())
    }

    async fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.fd.seek(pos).await
    }
}

pub fn align_up(alignment: usize, value: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

pub fn align_down(alignment: usize, value: usize) -> usize {
    value & !(alignment - 1)
}

async fn open_dma(file: File) -> io::Result<DmaFile> {
    let stat = fstatvfs(&file).await?;
    let alignment = stat.f_bsize.max(512) as usize;

    Ok(DmaFile {
        fd: file,
        alignment,
        buf: None,
    })
}

async fn fstatvfs(file: &File) -> io::Result<rustix::fs::StatVfs> {
    let fd = file.as_raw_fd();
    asyncify(move || {
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        rustix::fs::fstatvfs(fd).map_err(|e| e.into())
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
    let buf = DmaBuffer::new(file.alignment, file.alignment);
    file.set_buffer(buf);

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
    let len = file.buffer().len();
    if len > 0 {
        let align_up = file.align_up(len);
        if align_up == len {
            asyncify(move || file.write_direct().map(|_| file)).await?;
        } else {
            let dst = file.mut_buffer();
            unsafe { dst.set_len(align_up) }
            file = asyncify(move || file.write_direct().map(|_| file)).await?;
            asyncify(move || file.truncate(file_length).map(|_| file)).await?;
        }
    }

    Ok(file_length)
}

pub async fn dma_read_file(
    path: impl AsRef<Path>,
    mut writer: impl io::Write,
) -> io::Result<usize> {
    let mut file = DmaFile::open(path.as_ref()).await?;
    let buf = DmaBuffer::new(file.alignment, file.alignment);
    file.set_buffer(buf);

    let mut n = 0;
    loop {
        file = asyncify(move || file.read_direct().map(|_| file)).await?;

        let buf = file.buffer();
        if buf.is_empty() {
            return Ok(n);
        }
        n += buf.len();
        writer.write_all(buf)?;
        let eof = buf.remaining() > 0;
        unsafe { file.mut_buffer().set_len(0) }
        if eof {
            return Ok(n);
        }
    }
}

pub async fn dma_read_file_range(
    path: impl AsRef<Path>,
    range: Range<u64>,
) -> io::Result<(DmaBuffer, Range<usize>)> {
    if range.is_empty() {
        return Ok((DmaBuffer::new(2, 2), 0..0));
    }

    let mut file = DmaFile::open(path.as_ref()).await?;

    let align_start = file.align_down(range.start as usize);
    let align_end = file.align_up(range.end as usize);

    let buf = DmaBuffer::new(align_end - align_start, file.alignment);
    file.set_buffer(buf);

    let offset = file.seek(SeekFrom::Start(align_start as u64)).await?;

    if offset as usize != align_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "range out of range",
        ));
    }

    file = asyncify(move || file.read_direct().map(|_| file)).await?;

    let rt_range = range.start as usize - align_start..range.end as usize - align_start;
    Ok((file.buf.unwrap(), rt_range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_write() {
        let _ = std::fs::remove_file("test_file");

        run_test(0).await.unwrap();
        run_test(100).await.unwrap();
        run_test(200).await.unwrap();

        run_test(4096 - 1).await.unwrap();
        run_test(4096).await.unwrap();
        run_test(4096 + 1).await.unwrap();

        run_test(4096 * 2 - 1).await.unwrap();
        run_test(4096 * 2).await.unwrap();
        run_test(4096 * 2 + 1).await.unwrap();
    }

    async fn run_test(n: usize) -> io::Result<()> {
        let filename = "test_file";
        let want = (0..n).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        let length = dma_write_file_vectored(filename, &bufs).await?;

        assert_eq!(length, want.len());

        let mut got = Vec::new();

        let length = dma_read_file(filename, &mut got).await?;
        assert_eq!(length, want.len());
        assert_eq!(got, want);

        std::fs::remove_file(filename)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_range_read() {
        let filename = "test_file2";
        let _ = std::fs::remove_file(filename);
        let n: usize = 4096 * 2;

        let want = (0..n).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        dma_write_file_vectored(filename, &bufs).await.unwrap();

        let got = dma_read_file_range(filename, 0..10).await.unwrap();
        let got = got.0[got.1].to_vec();
        assert_eq!(&want[0..10], got);

        let got = dma_read_file_range(filename, 10..30).await.unwrap();
        let got = got.0[got.1].to_vec();
        assert_eq!(&want[10..30], got);

        let got = dma_read_file_range(filename, 4096 - 5..4096 + 5)
            .await
            .unwrap();
        let got = got.0[got.1].to_vec();
        assert_eq!(&want[4096 - 5..4096 + 5], got);

        let got = dma_read_file_range(filename, 4096..4096 + 5).await.unwrap();
        let got = got.0[got.1].to_vec();
        assert_eq!(&want[4096..4096 + 5], got);

        let got = dma_read_file_range(filename, 4096 * 2 - 5..4096 * 2)
            .await
            .unwrap();
        let got = got.0[got.1].to_vec();
        assert_eq!(&want[4096 * 2 - 5..4096 * 2], got);

        let _ = std::fs::remove_file(filename);
    }
}
