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

use std::alloc::AllocError;
use std::alloc::Allocator;
use std::alloc::Global;
use std::alloc::Layout;
use std::fmt;
use std::io;
use std::io::IoSlice;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::Range;
use std::os::fd::BorrowedFd;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr;
use std::ptr::NonNull;

use bytes::Bytes;
use rustix::fs::OFlags;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;

use crate::runtime::spawn_blocking;

#[derive(Copy, Clone, PartialEq, Eq)]

pub struct Alignment(ptr::Alignment);

impl Alignment {
    pub const MIN: Self = Self(ptr::Alignment::MIN);

    #[inline]
    pub const fn new(align: usize) -> Option<Self> {
        match ptr::Alignment::new(align) {
            Some(a) => Some(Alignment(a)),
            None => None,
        }
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0.as_usize()
    }

    #[inline]
    pub const fn align_up(self, value: usize) -> usize {
        (value + self.as_usize() - 1) & self.mask()
    }

    #[inline]
    pub const fn align_down(self, value: usize) -> usize {
        value & self.mask()
    }

    #[inline]
    pub const fn align_up_count(self, value: usize) -> usize {
        (value + self.as_usize() - 1) >> self.log2()
    }

    #[inline]
    pub const fn align_down_count(self, value: usize) -> usize {
        value >> self.log2()
    }

    #[inline]
    pub const fn mask(self) -> usize {
        self.0.mask()
    }

    #[inline]
    pub const fn log2(self) -> u32 {
        self.0.log2()
    }
}

impl fmt::Debug for Alignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl TryFrom<usize> for Alignment {
    type Error = std::num::TryFromIntError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Alignment(value.try_into()?))
    }
}

unsafe impl Send for DmaAllocator {}

#[derive(Clone, Copy)]
pub struct DmaAllocator(Alignment);

impl DmaAllocator {
    pub fn new(align: Alignment) -> DmaAllocator {
        DmaAllocator(align)
    }

    fn real_layout(&self, layout: Layout) -> Layout {
        if layout.align() >= self.0.as_usize() {
            layout
        } else {
            Layout::from_size_align(layout.size(), self.0.as_usize()).unwrap()
        }
    }
}

unsafe impl Allocator for DmaAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global {}.allocate(self.real_layout(layout))
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global {}.allocate_zeroed(self.real_layout(layout))
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        Global {}.deallocate(ptr, self.real_layout(layout))
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Global {}.grow(
            ptr,
            self.real_layout(old_layout),
            self.real_layout(new_layout),
        )
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Global {}.grow_zeroed(
            ptr,
            self.real_layout(old_layout),
            self.real_layout(new_layout),
        )
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Global {}.shrink(
            ptr,
            self.real_layout(old_layout),
            self.real_layout(new_layout),
        )
    }
}

type DmaBuffer = Vec<u8, DmaAllocator>;

pub fn dma_buffer_to_bytes(buf: DmaBuffer) -> Bytes {
    if buf.is_empty() {
        return Bytes::new();
    }
    let (ptr, len, cap, alloc) = buf.into_raw_parts_with_alloc();
    // Memory fitting
    let old_layout = Layout::from_size_align(cap, alloc.0.as_usize()).unwrap();
    let new_layout = Layout::from_size_align(len, std::mem::align_of::<u8>()).unwrap();
    let data = unsafe {
        let p = Global {}
            .shrink(NonNull::new(ptr).unwrap(), old_layout, new_layout)
            .unwrap();
        let cap = p.len();
        Vec::from_raw_parts(p.cast().as_mut(), len, cap)
    };
    Bytes::from(data)
}

/// A `DmaFile` is similar to a `File`, but it is opened with the `O_DIRECT` file in order to
/// perform direct IO.
struct AsyncDmaFile {
    fd: File,
    alignment: Alignment,
    buf: Option<DmaBuffer>,
}

impl AsyncDmaFile {
    async fn open_raw(path: impl AsRef<Path>, #[allow(unused)] dio: bool) -> io::Result<File> {
        #[allow(unused_mut)]
        let mut flags = 0;
        #[cfg(target_os = "linux")]
        if dio {
            flags = OFlags::DIRECT.bits() as i32
        }

        File::options()
            .read(true)
            .custom_flags(flags)
            .open(path)
            .await
    }

    async fn create_raw(path: impl AsRef<Path>, #[allow(unused)] dio: bool) -> io::Result<File> {
        #[allow(unused_mut)]
        let mut flags = OFlags::EXCL;
        #[cfg(target_os = "linux")]
        if dio {
            flags |= OFlags::DIRECT;
        }

        File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(flags.bits() as i32)
            .open(path)
            .await
    }

    /// Attempts to open a file in read-only mode.
    async fn open(path: impl AsRef<Path>, dio: bool) -> io::Result<AsyncDmaFile> {
        let file = AsyncDmaFile::open_raw(path, dio).await?;
        open_dma(file).await
    }

    /// Opens a file in write-only mode.
    async fn create(path: impl AsRef<Path>, dio: bool) -> io::Result<AsyncDmaFile> {
        let file = AsyncDmaFile::create_raw(path, dio).await?;
        open_dma(file).await
    }

    fn set_buffer(&mut self, buf: DmaBuffer) {
        self.buf = Some(buf)
    }

    /// Aligns `value` up to the memory alignment requirement for this file.
    pub fn align_up(&self, value: usize) -> usize {
        self.alignment.align_up(value)
    }

    /// Aligns `value` down to the memory alignment requirement for this file.
    pub fn align_down(&self, value: usize) -> usize {
        self.alignment.align_down(value)
    }

    /// Return the alignment requirement for this file. The returned alignment value can be used
    /// to allocate a buffer to use with this file:
    #[expect(dead_code)]
    pub fn alignment(&self) -> Alignment {
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
        match rustix::io::write(&self.fd, buf) {
            Ok(n) => {
                if n != buf.len() {
                    return Err(io::Error::other("short write"));
                }
                self.mut_buffer().clear();
                Ok(n)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn read_direct(&mut self, n: usize) -> io::Result<usize> {
        let Self { fd, buf, .. } = self;
        let buf = buf.as_mut().unwrap();
        if n > buf.capacity() - buf.len() {
            return Err(io::Error::other("buf not sufficient"));
        }
        let start = buf.len();
        unsafe { buf.set_len(buf.len() + n) };
        match rustix::io::read(fd, &mut (*buf)[start..]) {
            Ok(n) => {
                buf.truncate(start + n);
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

async fn open_dma(file: File) -> io::Result<AsyncDmaFile> {
    let stat = fstatvfs(&file).await?;
    let alignment = Alignment::new(stat.f_bsize.max(512) as usize).unwrap();

    Ok(AsyncDmaFile {
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
        Err(_) => Err(io::Error::other("background task failed")),
    }
}

pub struct DmaWriteBuf {
    allocator: DmaAllocator,
    data: Vec<DmaBuffer>,
    chunk: usize,
}

impl DmaWriteBuf {
    pub fn new(align: Alignment, chunk: usize) -> DmaWriteBuf {
        DmaWriteBuf {
            allocator: DmaAllocator::new(align),
            data: Vec::new(),
            chunk: align.align_up(chunk),
        }
    }

    pub fn chunk(&self) -> usize {
        self.chunk
    }

    pub fn size(&self) -> usize {
        if self.data.is_empty() {
            return 0;
        }

        (self.data.len() - 1) * self.chunk + self.data.last().unwrap().len()
    }

    pub async fn into_file(mut self, path: impl AsRef<Path>, dio: bool) -> io::Result<usize> {
        let mut file = AsyncDmaFile {
            fd: AsyncDmaFile::create_raw(path, dio).await?,
            alignment: self.allocator.0,
            buf: None,
        };

        let file_length = self.size();

        let Some(mut last) = self.data.pop() else {
            return Ok(0);
        };

        for buf in self.data {
            debug_assert_eq!(buf.len(), buf.capacity());
            file.set_buffer(buf);
            file = asyncify(move || file.write_direct().map(|_| file)).await?;
        }

        let len = last.len();
        let align_up = file.align_up(len);
        if align_up == len {
            file.set_buffer(last);
            asyncify(move || file.write_direct()).await?;
        } else {
            unsafe { last.set_len(align_up) }
            file.set_buffer(last);
            asyncify(move || {
                file.write_direct()?;
                file.truncate(file_length)
            })
            .await?;
        }
        Ok(file_length)
    }

    pub fn into_data(self) -> Vec<DmaBuffer> {
        self.data
    }

    pub fn write_last<'a>(&mut self, buf: &'a [u8]) -> &'a [u8] {
        let Some(dst) = self.data.last_mut() else {
            return buf;
        };
        if dst.len() == dst.capacity() {
            return buf;
        }

        let remain = dst.capacity() - dst.len();
        Self::full_buffer(buf, dst, remain)
    }

    fn full_buffer<'a>(buf: &'a [u8], dst: &mut DmaBuffer, remain: usize) -> &'a [u8] {
        if buf.len() <= remain {
            dst.extend_from_slice(buf);
            &buf[buf.len()..]
        } else {
            let (left, right) = buf.split_at(remain);
            dst.extend_from_slice(left);
            right
        }
    }

    pub fn need_alloc(&self) -> bool {
        self.data
            .last()
            .map(|dst| dst.len() == dst.capacity())
            .unwrap_or(true)
    }

    pub fn alloc_buffer(&mut self) {
        debug_assert!(self.data.iter().all(|buf| buf.len() == self.chunk));
        self.data
            .push(Vec::with_capacity_in(self.chunk, self.allocator));
    }
}

impl Write for DmaWriteBuf {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();
        while !buf.is_empty() {
            let (dst, remain) = match self.data.last_mut() {
                Some(dst) if dst.len() < dst.capacity() => {
                    let remain = dst.capacity() - dst.len();
                    (dst, remain)
                }
                _ => {
                    self.alloc_buffer();
                    (self.data.last_mut().unwrap(), self.chunk)
                }
            };

            buf = Self::full_buffer(buf, dst, remain);
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub async fn dma_write_file_vectored<'a>(
    path: impl AsRef<Path>,
    bufs: &'a [IoSlice<'a>],
) -> io::Result<usize> {
    let mut file = AsyncDmaFile::create(path.as_ref(), true).await?;

    let file_length = bufs.iter().map(|buf| buf.len()).sum();
    if file_length == 0 {
        return Ok(0);
    }

    const BUFFER_SIZE: usize = 1024 * 1024;
    let buffer_size = BUFFER_SIZE.min(file_length);

    let dma_buf = Vec::with_capacity_in(
        file.align_up(buffer_size),
        DmaAllocator::new(file.alignment),
    );
    file.set_buffer(dma_buf);

    for src in bufs {
        let mut src = &src[..];

        while !src.is_empty() {
            let dst = file.buffer();
            if dst.capacity() == dst.len() {
                file = asyncify(move || file.write_direct().map(|_| file)).await?;
            }

            let dst = file.mut_buffer();
            let remaining = dst.capacity() - dst.len();
            let n = src.len().min(remaining);
            let (left, right) = src.split_at(n);
            dst.extend_from_slice(left);
            src = right;
        }
    }

    let len = file.buffer().len();
    if len > 0 {
        let align_up = file.align_up(len);
        if align_up == len {
            asyncify(move || file.write_direct()).await?;
        } else {
            let dst = file.mut_buffer();
            unsafe { dst.set_len(align_up) }
            asyncify(move || {
                file.write_direct()?;
                file.truncate(file_length)
            })
            .await?;
        }
    }

    Ok(file_length)
}

pub async fn dma_read_file(
    path: impl AsRef<Path>,
    mut writer: impl io::Write,
) -> io::Result<usize> {
    const BUFFER_SIZE: usize = 1024 * 1024;
    let mut file = AsyncDmaFile::open(path.as_ref(), true).await?;
    let buf = Vec::with_capacity_in(
        file.align_up(BUFFER_SIZE),
        DmaAllocator::new(file.alignment),
    );
    file.set_buffer(buf);

    let mut n = 0;
    loop {
        file = asyncify(move || {
            let buf = file.buffer();
            let remain = buf.capacity() - buf.len();
            file.read_direct(remain).map(|_| file)
        })
        .await?;

        let buf = file.buffer();
        if buf.is_empty() {
            return Ok(n);
        }
        n += buf.len();
        writer.write_all(buf)?;
        // WARN: Is it possible to have a short read but not eof?
        let eof = buf.capacity() > buf.len();
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
    let mut file = AsyncDmaFile::open(path.as_ref(), true).await?;

    let align_start = file.align_down(range.start as usize);
    let align_end = file.align_up(range.end as usize);

    let buf = Vec::with_capacity_in(align_end - align_start, DmaAllocator::new(file.alignment));
    file.set_buffer(buf);

    if align_start != 0 {
        let offset = file.seek(SeekFrom::Start(align_start as u64)).await?;
        if offset as usize != align_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range out of range",
            ));
        }
    }

    let mut n;
    loop {
        (file, n) = asyncify(move || {
            let buf = file.buffer();
            let remain = buf.capacity() - buf.len();
            file.read_direct(remain).map(|n| (file, n))
        })
        .await?;
        if align_start + file.buffer().len() >= range.end as usize {
            break;
        }
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
        }
    }

    let rt_range = range.start as usize - align_start..range.end as usize - align_start;
    Ok((file.buf.unwrap(), rt_range))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment() {
        let a = Alignment::new(4).unwrap();

        assert_eq!(8, a.align_up(5));
        assert_eq!(4, a.align_down(5));
        assert_eq!(2, a.align_up_count(5));
        assert_eq!(1, a.align_down_count(5));
    }

    #[tokio::test]
    async fn test_read_write() {
        let _ = std::fs::remove_file("test_file");

        for dio in [true, false] {
            run_test(0, dio).await.unwrap();
            run_test(100, dio).await.unwrap();
            run_test(200, dio).await.unwrap();

            run_test(4096 - 1, dio).await.unwrap();
            run_test(4096, dio).await.unwrap();
            run_test(4096 + 1, dio).await.unwrap();

            run_test(4096 * 2 - 1, dio).await.unwrap();
            run_test(4096 * 2, dio).await.unwrap();
            run_test(4096 * 2 + 1, dio).await.unwrap();

            run_test(1024 * 1024 * 3 - 1, dio).await.unwrap();
            run_test(1024 * 1024 * 3, dio).await.unwrap();
            run_test(1024 * 1024 * 3 + 1, dio).await.unwrap();
        }
    }

    async fn run_test(n: usize, dio: bool) -> io::Result<()> {
        let filename = "test_file";
        let want = (0..n).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        let length = dma_write_file_vectored(filename, &bufs).await?;

        assert_eq!(length, want.len());

        let mut got = Vec::new();

        let length = dma_read_file(filename, &mut got).await?;
        assert_eq!(length, want.len());
        assert_eq!(got, want);

        let file = AsyncDmaFile::open(filename, dio).await?;
        let align = file.alignment;
        drop(file);

        std::fs::remove_file(filename)?;

        let mut buf = DmaWriteBuf::new(align, align.as_usize());
        buf.write_all(&want)?;
        let length = buf.into_file(filename, dio).await?;

        assert_eq!(length, want.len());

        let (buf, range) = dma_read_file_range(filename, 0..length as u64).await?;
        assert_eq!(&buf[range], &want);

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

    #[tokio::test]
    async fn test_read_direct() {
        let filename = "test_file3";
        let _ = std::fs::remove_file(filename);
        let stat = rustix::fs::statvfs(".").unwrap();
        let alignment = 512.max(stat.f_bsize as usize);
        let file_size: usize = alignment * 2;

        let want = (0..file_size).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        dma_write_file_vectored(filename, &bufs).await.unwrap();

        let mut file = AsyncDmaFile::open(filename, true).await.unwrap();
        let buf = Vec::with_capacity_in(file_size, DmaAllocator::new(file.alignment));
        file.set_buffer(buf);

        let got = file.read_direct(alignment).unwrap();
        assert_eq!(alignment, got);
        assert_eq!(&want[0..alignment], &**file.buffer());

        let got = file.read_direct(alignment).unwrap();
        assert_eq!(alignment, got);
        assert_eq!(&want, &**file.buffer());

        let _ = std::fs::remove_file(filename);
    }

    #[test]
    fn test_dma_buffer_to_bytes() {
        let want = (0..10_u8).collect::<Vec<_>>();
        let alloc = DmaAllocator::new(Alignment::new(4096).unwrap());
        let mut buf = DmaBuffer::with_capacity_in(3000, alloc);
        buf.extend_from_slice(&want);

        println!("{:?} {}", buf.as_ptr(), buf.capacity());
        buf.shrink_to_fit();
        println!("{:?} {}", buf.as_ptr(), buf.capacity());
        buf.reserve(3000 - buf.capacity());
        println!("{:?} {}", buf.as_ptr(), buf.capacity());

        // let slice = buf.into_boxed_slice();
        // println!("{:?}", slice.as_ptr());

        let got = dma_buffer_to_bytes(buf);
        println!("{:?}", got.as_ptr());
        assert_eq!(&want, &got);

        let buf = got.to_vec();
        println!("{:?} {}", buf.as_ptr(), buf.capacity());
    }
}
