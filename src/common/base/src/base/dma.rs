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
use std::io::Write;
use std::ops::Range;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr;
use std::ptr::NonNull;

use bytes::Bytes;
use rustix::fs::OFlags;
use tokio::fs::File as AsyncFile;

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
        unsafe { Global {}.deallocate(ptr, self.real_layout(layout)) }
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe {
            Global {}.grow(
                ptr,
                self.real_layout(old_layout),
                self.real_layout(new_layout),
            )
        }
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe {
            Global {}.grow_zeroed(
                ptr,
                self.real_layout(old_layout),
                self.real_layout(new_layout),
            )
        }
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        unsafe {
            Global {}.shrink(
                ptr,
                self.real_layout(old_layout),
                self.real_layout(new_layout),
            )
        }
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

/// A `AsyncDmaFile` is similar to a `File`, but it is opened with the `O_DIRECT` file in order to
/// perform direct IO.
pub struct DmaFile<F> {
    fd: F,
    alignment: Alignment,
    offset: usize,
}

impl<F: AsFd> DmaFile<F> {
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
    pub fn alignment(&self) -> Alignment {
        self.alignment
    }

    fn write_direct(&mut self, buf: &mut DmaBuffer) -> io::Result<usize> {
        let data = buf.as_slice();
        let mut written = 0;
        let offset = self.align_down(self.offset);

        while written < data.len() {
            match rustix::io::pwrite(&self.fd, &data[written..], (offset + written) as _) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write returned zero bytes",
                    ));
                }
                Ok(n) => {
                    written += n;
                    self.offset = offset + written
                }
                Err(err) => {
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err.into());
                }
            }
        }
        buf.clear();
        Ok(written)
    }

    fn inc_offset(&mut self, n: usize) {
        debug_assert!(n >= self.alignment.as_usize());
        debug_assert_eq!(n, self.alignment.align_down(n));
        self.offset = self.align_down(self.offset) + n;
    }

    fn read_direct(&mut self, buf: &mut DmaBuffer, n: usize) -> io::Result<usize> {
        if n > buf.capacity() - buf.len() {
            return Err(io::Error::other("buf not sufficient"));
        }
        let start = buf.len();
        unsafe { buf.set_len(buf.len() + n) };
        match rustix::io::read(&self.fd, &mut (*buf)[start..]) {
            Ok(n) => {
                buf.truncate(start + n);
                Ok(n)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn pread_direct(&mut self, buf: &mut DmaBuffer, n: usize, offset: u64) -> io::Result<usize> {
        if n > buf.capacity() - buf.len() {
            return Err(io::Error::other("buf not sufficient"));
        }
        let start = buf.len();
        unsafe { buf.set_len(buf.len() + n) };
        match rustix::io::pread(&self.fd, &mut (*buf)[start..], offset) {
            Ok(n) => {
                buf.truncate(start + n);
                Ok(n)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn truncate(&mut self, length: usize) -> io::Result<()> {
        rustix::fs::ftruncate(&self.fd, length as u64).map_err(io::Error::from)
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        rustix::fs::fsync(&self.fd).map_err(io::Error::from)
    }

    pub fn size(&self) -> io::Result<usize> {
        Ok(rustix::fs::fstat(&self.fd)?.st_size as _)
    }

    pub fn length(&self) -> usize {
        self.offset
    }
}

pub type AsyncDmaFile = DmaFile<AsyncFile>;

impl AsyncDmaFile {
    async fn open_fd(path: impl AsRef<Path>, dio: bool) -> io::Result<AsyncFile> {
        let flags = flags_direct_or_empty(dio).bits() as i32;
        AsyncFile::options()
            .read(true)
            .custom_flags(flags)
            .open(path)
            .await
    }

    async fn create_fd(path: impl AsRef<Path>, dio: bool) -> io::Result<AsyncFile> {
        let flags = flags_direct_or_empty(dio) | OFlags::EXCL;
        AsyncFile::options()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(flags.bits() as i32)
            .open(path)
            .await
    }

    /// Attempts to open a file in read-only mode.
    pub async fn open(
        path: impl AsRef<Path>,
        dio: bool,
        align: Option<Alignment>,
    ) -> io::Result<AsyncDmaFile> {
        let file = AsyncDmaFile::open_fd(path, dio).await?;
        Self::new(file, align).await
    }

    /// Opens a file in write-only mode.
    async fn create(path: impl AsRef<Path>, dio: bool) -> io::Result<AsyncDmaFile> {
        let file = AsyncDmaFile::create_fd(path, dio).await?;
        Self::new(file, None).await
    }

    async fn new(file: AsyncFile, align: Option<Alignment>) -> io::Result<AsyncDmaFile> {
        let alignment = match align {
            Some(align) => align,
            None => {
                let fd = file.as_raw_fd();
                let stat = asyncify(move || {
                    rustix::fs::fstatvfs(unsafe { BorrowedFd::borrow_raw(fd) })
                        .map_err(io::Error::from)
                })
                .await?;
                Alignment::new(stat.f_frsize.max(512) as usize).unwrap()
            }
        };

        Ok(AsyncDmaFile {
            fd: file,
            alignment,
            offset: 0,
        })
    }

    pub async fn read_range(&mut self, range: Range<u64>) -> io::Result<(DmaBuffer, Range<usize>)> {
        let align_start = self.align_down(range.start as usize);
        let align_end = self.align_up(range.end as usize);

        let mut buf =
            Vec::with_capacity_in(align_end - align_start, DmaAllocator::new(self.alignment));
        if range.is_empty() {
            return Ok((buf, 0..0));
        }

        let fd = self.fd.as_raw_fd();
        let alignment = self.alignment;
        let buf = asyncify(move || {
            loop {
                let remain = buf.capacity() - buf.len();
                let mut file = DmaFile {
                    fd: unsafe { BorrowedFd::borrow_raw(fd) },
                    alignment,
                    offset: 0,
                };
                let offset = align_start + buf.len();
                let n = file.pread_direct(&mut buf, remain, offset as _)?;
                if align_start + buf.len() >= range.end as usize {
                    return Ok(buf);
                }
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
                }
            }
        })
        .await?;

        let rt_range = range.start as usize - align_start..range.end as usize - align_start;
        Ok((buf, rt_range))
    }
}

#[cfg(target_os = "linux")]
fn flags_direct_or_empty(dio: bool) -> OFlags {
    if dio { OFlags::DIRECT } else { OFlags::empty() }
}

#[cfg(not(target_os = "linux"))]
fn flags_direct_or_empty(_dio: bool) -> OFlags {
    OFlags::empty()
}

pub type SyncDmaFile = DmaFile<OwnedFd>;

impl SyncDmaFile {
    fn open_fd(path: impl rustix::path::Arg, dio: bool) -> io::Result<OwnedFd> {
        let flags = OFlags::RDONLY | flags_direct_or_empty(dio);
        rustix::fs::open(path, flags, rustix::fs::Mode::empty()).map_err(io::Error::from)
    }

    fn create_fd(path: impl rustix::path::Arg, dio: bool) -> io::Result<OwnedFd> {
        let flags = OFlags::EXCL
            | OFlags::CREATE
            | OFlags::TRUNC
            | OFlags::WRONLY
            | flags_direct_or_empty(dio);
        rustix::fs::open(path, flags, rustix::fs::Mode::from_raw_mode(0o666))
            .map_err(io::Error::from)
    }

    fn open_dma(fd: OwnedFd) -> io::Result<DmaFile<OwnedFd>> {
        let stat = rustix::fs::fstatvfs(&fd)?;
        let alignment = Alignment::new(stat.f_frsize.max(512) as usize).unwrap();

        Ok(Self {
            fd,
            alignment,
            offset: 0,
        })
    }

    pub fn open(path: impl AsRef<Path>, dio: bool, align: Option<Alignment>) -> io::Result<Self> {
        let fd = Self::open_fd(path.as_ref(), dio)?;
        match align {
            Some(alignment) => Ok(Self {
                fd,
                alignment,
                offset: 0,
            }),
            None => Self::open_dma(fd),
        }
    }

    pub fn create(path: impl AsRef<Path>, dio: bool) -> io::Result<Self> {
        let fd = Self::create_fd(path.as_ref(), dio)?;
        Self::open_dma(fd)
    }

    pub fn read_range(&self, range: Range<u64>) -> io::Result<(DmaBuffer, Range<usize>)> {
        let align_start = self.align_down(range.start as usize);
        let align_end = self.align_up(range.end as usize);

        let mut buf =
            Vec::with_capacity_in(align_end - align_start, DmaAllocator::new(self.alignment));

        let mut file = DmaFile {
            fd: self.fd.as_fd(),
            alignment: self.alignment,
            offset: 0,
        };

        loop {
            let remain = buf.capacity() - buf.len();
            let offset = align_start + buf.len();
            let n = file.pread_direct(&mut buf, remain, offset as _)?;
            if align_start + buf.len() >= range.end as usize {
                break;
            }
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, ""));
            }
        }
        let rt_range = range.start as usize - align_start..range.end as usize - align_start;
        Ok((buf, rt_range))
    }
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
            fd: AsyncDmaFile::create_fd(path, dio).await?,
            alignment: self.allocator.0,
            offset: 0,
        };

        let file_length = self.size();
        let mut data = std::mem::take(&mut self.data);

        let Some(mut last) = data.pop() else {
            return Ok(0);
        };

        file = asyncify(move || {
            for mut buf in data {
                debug_assert_eq!(buf.len(), buf.capacity());
                file.write_direct(&mut buf)?;
            }
            Ok(file)
        })
        .await?;

        let len = last.len();
        let align_up = file.align_up(len);
        if align_up == len {
            let mut buf = last;
            asyncify(move || {
                file.write_direct(&mut buf)?;
                Ok(())
            })
            .await?;
        } else {
            unsafe { last.set_len(align_up) }
            let mut buf = last;
            asyncify(move || {
                file.write_direct(&mut buf)?;
                file.truncate(file_length)
            })
            .await?;
        }
        Ok(file_length)
    }

    pub fn into_data(self) -> Vec<DmaBuffer> {
        self.data
    }

    fn copy<'a>(src: &'a [u8], dst: &mut DmaBuffer, remain: usize) -> &'a [u8] {
        if src.len() <= remain {
            dst.extend_from_slice(src);
            &src[src.len()..]
        } else {
            let (left, right) = src.split_at(remain);
            dst.extend_from_slice(left);
            right
        }
    }

    fn is_last_full(&self) -> bool {
        self.data
            .last()
            .map(|dst| dst.len() == dst.capacity())
            .unwrap_or(true)
    }

    pub fn fast_write(&mut self, buf: &[u8]) -> bool {
        let Some(dst) = self.data.last_mut() else {
            return false;
        };

        if buf.len() > dst.capacity() - dst.len() {
            return false;
        }
        dst.extend_from_slice(buf);
        true
    }

    pub fn alloc_buffer(&mut self) {
        debug_assert!(self.data.iter().all(|buf| buf.len() == self.chunk));
        self.data
            .push(Vec::with_capacity_in(self.chunk, self.allocator));
    }

    pub fn flush_if_full(&mut self, file: &mut SyncDmaFile) -> io::Result<usize> {
        debug_assert_eq!(self.allocator.0, file.alignment);

        if self.size() < self.chunk {
            return Ok(0);
        }

        let data = if self.is_last_full() {
            &self.data
        } else {
            &self.data[..self.data.len() - 1]
        };

        let len = data.len() * self.chunk;

        let mut io_slices: Vec<_> = data.iter().map(|buf| IoSlice::new(buf)).collect();
        let written = writev_all(&file.fd, &mut io_slices, file.align_down(file.offset))?;

        let last = self.data.pop();
        self.data.clear();
        match last {
            Some(last) if last.len() != last.capacity() => {
                self.data.push(last);
            }
            _ => (),
        }

        file.inc_offset(written);

        if written != len {
            Err(io::Error::other("short write"))
        } else {
            Ok(written)
        }
    }

    fn flush_inner(&mut self, file: &mut SyncDmaFile, close: bool) -> io::Result<()> {
        debug_assert_eq!(self.allocator.0, file.alignment);

        if self.data.is_empty() {
            return Ok(());
        }

        let last = self
            .data
            .pop_if(|last| file.align_up(last.len()) > last.len());

        let last = if let Some(mut last) = last {
            if self.data.is_empty() {
                use std::cmp::Ordering::*;
                match (file.offset - file.align_down(file.offset)).cmp(&last.len()) {
                    Equal => return Ok(()),
                    Greater => unreachable!(),
                    Less => {}
                }
            }
            let len = last.len();
            let align_up = file.align_up(len);
            let pad = align_up - len;
            debug_assert!(pad != 0);
            unsafe { last.set_len(align_up) };
            Some((last, len, pad))
        } else {
            None
        };

        let mut slices: Vec<_> = self
            .data
            .iter()
            .map(|buf| IoSlice::new(buf))
            .chain(last.as_ref().map(|last| IoSlice::new(&last.0)))
            .collect();
        let written = writev_all(&file.fd, &mut slices[..], file.align_down(file.offset))?;
        self.data.clear();

        file.inc_offset(written);

        if let Some((last, len, pad)) = last {
            file.offset -= pad;
            file.truncate(file.offset)?;

            if !close {
                self.write_all(&last[file.align_down(len)..(file.align_up(len) - pad)])?;
            }
        }

        Ok(())
    }

    pub fn flush_and_close(&mut self, mut file: SyncDmaFile) -> io::Result<usize> {
        self.flush_inner(&mut file, true)?;
        Ok(file.length())
    }

    pub fn flush(&mut self, file: &mut SyncDmaFile) -> io::Result<()> {
        self.flush_inner(file, false)
    }
}

fn writev_all(fd: impl AsFd, mut slices: &mut [IoSlice<'_>], offset: usize) -> io::Result<usize> {
    let mut written = 0;

    while !slices.is_empty() {
        match rustix::io::pwritev(fd.as_fd(), slices, (offset + written) as _) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "writev returned zero bytes",
                ));
            }
            Ok(n) => {
                written += n;
                IoSlice::advance_slices(&mut slices, n);
            }
            Err(err) => {
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err.into());
            }
        };
    }

    Ok(written)
}

impl io::Write for DmaWriteBuf {
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

            buf = Self::copy(buf, dst, remain);
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

    let mut dma_buf = Vec::with_capacity_in(
        file.align_up(buffer_size),
        DmaAllocator::new(file.alignment),
    );

    for src in bufs {
        let mut src = &src[..];

        while !src.is_empty() {
            if dma_buf.capacity() == dma_buf.len() {
                (file, dma_buf) =
                    asyncify(move || file.write_direct(&mut dma_buf).map(|_| (file, dma_buf)))
                        .await?;
            }

            let remaining = dma_buf.capacity() - dma_buf.len();
            let n = src.len().min(remaining);
            let (left, right) = src.split_at(n);
            dma_buf.extend_from_slice(left);
            src = right;
        }
    }

    let len = dma_buf.len();
    if len > 0 {
        let align_up = file.align_up(len);
        if align_up == len {
            asyncify(move || {
                file.write_direct(&mut dma_buf)?;
                Ok(())
            })
            .await?;
        } else {
            unsafe { dma_buf.set_len(align_up) }
            asyncify(move || {
                file.write_direct(&mut dma_buf)?;
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
    let mut file = AsyncDmaFile::open(path.as_ref(), true, None).await?;
    let mut buf = Vec::with_capacity_in(
        file.align_up(BUFFER_SIZE),
        DmaAllocator::new(file.alignment),
    );

    let mut n = 0;
    loop {
        (file, buf) = asyncify(move || {
            let remain = buf.capacity() - buf.len();
            file.read_direct(&mut buf, remain).map(|_| (file, buf))
        })
        .await?;

        if buf.is_empty() {
            return Ok(n);
        }
        n += buf.len();
        writer.write_all(&buf)?;
        // WARN: Is it possible to have a short read but not eof?
        let eof = buf.capacity() > buf.len();
        buf.clear();
        if eof {
            return Ok(n);
        }
    }
}

pub async fn dma_read_file_range(
    path: impl AsRef<Path>,
    range: Range<u64>,
) -> io::Result<(DmaBuffer, Range<usize>)> {
    let mut file = AsyncDmaFile::open(path.as_ref(), true, None).await?;
    file.read_range(range).await
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Write;

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
            run_test(0, dio).await;
            run_test(100, dio).await;
            run_test(200, dio).await;

            run_test(4096 - 1, dio).await;
            run_test(4096, dio).await;
            run_test(4096 + 1, dio).await;

            run_test(4096 * 2 - 1, dio).await;
            run_test(4096 * 2, dio).await;
            run_test(4096 * 2 + 1, dio).await;

            run_test(1024 * 1024 * 3 - 1, dio).await;
            run_test(1024 * 1024 * 3, dio).await;
            run_test(1024 * 1024 * 3 + 1, dio).await;
        }
    }

    async fn run_test(n: usize, dio: bool) {
        let filename = "test_file";
        let want = (0..n).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        let length = dma_write_file_vectored(filename, &bufs).await.unwrap();

        assert_eq!(length, want.len());

        let mut got = Vec::new();

        let length = dma_read_file(filename, &mut got).await.unwrap();
        assert_eq!(length, want.len());
        assert_eq!(got, want);

        let file = AsyncDmaFile::open(filename, dio, None).await.unwrap();
        let align = file.alignment;
        drop(file);

        let file = SyncDmaFile::open(filename, dio, None).unwrap();
        drop(file);

        std::fs::remove_file(filename).unwrap();

        let mut buf = DmaWriteBuf::new(align, align.as_usize());
        buf.write_all(&want).unwrap();
        let length = buf.into_file(filename, dio).await.unwrap();
        assert_eq!(length, want.len());

        let (buf, range) = dma_read_file_range(filename, 0..length as u64)
            .await
            .unwrap();
        assert_eq!(&buf[range], &want);

        std::fs::remove_file(filename).unwrap();
    }

    #[tokio::test]
    async fn test_range_read() {
        let filename = "test_file2";
        let _ = std::fs::remove_file(filename);
        let n: usize = 4096 * 2 + 10;

        let want = (0..n).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        dma_write_file_vectored(filename, &bufs).await.unwrap();

        let sync_file = SyncDmaFile::open(filename, true, None).unwrap();
        let mut async_file = AsyncDmaFile::open(filename, true, None).await.unwrap();

        for range in [
            0..10,
            4096 - 5..4096 + 5,
            10..30,
            4096..4096 + 5,
            4096 * 2 - 5..4096 * 2,
            4096 + 10..4096 * 2 + 5,
        ] {
            let range_usize = range.start as usize..range.end as _;
            let got = async_file.read_range(range.clone()).await.unwrap();
            let got = got.0[got.1].to_vec();
            assert_eq!(&want[range_usize.clone()], got);

            let got = sync_file.read_range(range.clone()).unwrap();
            let got = got.0[got.1].to_vec();
            assert_eq!(&want[range_usize], got);
        }

        let _ = std::fs::remove_file(filename);
    }

    #[tokio::test]
    async fn test_read_direct() {
        let filename = "test_file3";
        let _ = std::fs::remove_file(filename);
        let stat = rustix::fs::statvfs(".").unwrap();
        let alignment = 512.max(stat.f_frsize as usize);
        let file_size: usize = alignment * 2;

        let want = (0..file_size).map(|i| (i % 256) as u8).collect::<Vec<_>>();

        let bufs = vec![IoSlice::new(&want)];
        dma_write_file_vectored(filename, &bufs).await.unwrap();

        let mut file = AsyncDmaFile::open(filename, true, None).await.unwrap();
        let mut buf = Vec::with_capacity_in(file_size, DmaAllocator::new(file.alignment));

        let got = file.read_direct(&mut buf, alignment).unwrap();
        assert_eq!(alignment, got);
        assert_eq!(&want[0..alignment], buf.as_slice());

        let got = file.read_direct(&mut buf, alignment).unwrap();
        assert_eq!(alignment, got);
        assert_eq!(&want, buf.as_slice());

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

    #[test]
    fn test_write() -> io::Result<()> {
        let filename = "test_write_file";
        let _ = std::fs::remove_file(filename);
        let mut file = SyncDmaFile::create(filename, true)?;

        let mut buf = DmaWriteBuf::new(file.alignment, file.alignment.as_usize() * 2);

        {
            buf.write_all(b"1")?;
            buf.flush(&mut file)?;
            file.fsync()?;

            assert_eq!(file.offset, 1);

            let mut got = Vec::new();
            let mut read = std::fs::File::open(filename)?;
            let n = read.read_to_end(&mut got)?;
            assert_eq!(n, 1);

            assert_eq!(b"1".as_slice(), got.as_slice());
        }

        {
            buf.write_all(b"2")?;
            buf.write_all(b"3")?;
            buf.flush(&mut file)?;
            file.fsync()?;

            assert_eq!(file.offset, 3);

            let mut got = Vec::new();
            let mut read = std::fs::File::open(filename)?;
            let n = read.read_to_end(&mut got)?;
            assert_eq!(n, 3);

            assert_eq!(b"123".as_slice(), got.as_slice());
        }

        {
            let data: Vec<_> = b"123"
                .iter()
                .copied()
                .cycle()
                .take(file.alignment.as_usize() * 3)
                .collect();

            buf.write_all(&data)?;
            buf.flush(&mut file)?;
            file.fsync()?;

            assert_eq!(file.offset, 3 + data.len());

            let mut got = Vec::new();
            let mut read = std::fs::File::open(filename)?;
            let n = read.read_to_end(&mut got)?;
            assert_eq!(n, 3 + data.len());

            let want: Vec<_> = [&b"123"[..], &data].concat();
            assert_eq!(want.as_slice(), got.as_slice());
        }

        let _ = std::fs::remove_file(filename);

        Ok(())
    }
}
