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

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::mem_allocator::JEAllocator;

/// mmap allocator.
/// For better performance, we use jemalloc as the inner allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct MmapAllocator {
    allocator: JEAllocator,
}

impl MmapAllocator {
    pub fn new() -> Self {
        Self {
            allocator: JEAllocator,
        }
    }
}

#[cfg(target_os = "linux")]
pub mod linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::null_mut;
    use std::ptr::NonNull;

    use super::MmapAllocator;
    use crate::runtime::ThreadTracker;

    // MADV_POPULATE_WRITE is supported since Linux 5.14.
    const MADV_POPULATE_WRITE: i32 = 23;

    const THRESHOLD: usize = 64 << 20;

    impl MmapAllocator {
        #[inline(always)]
        fn mmap_alloc(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert!(layout.align() <= page_size());
            ThreadTracker::alloc(layout.size() as i64)?;
            const PROT: i32 = libc::PROT_READ | libc::PROT_WRITE;
            const FLAGS: i32 = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE;
            let addr = unsafe { libc::mmap(null_mut(), layout.size(), PROT, FLAGS, -1, 0) };
            if addr == libc::MAP_FAILED {
                return Err(AllocError);
            }
            let addr = NonNull::new(addr as *mut ()).ok_or(AllocError)?;
            Ok(NonNull::<[u8]>::from_raw_parts(addr, layout.size()))
        }

        #[inline(always)]
        unsafe fn mmap_dealloc(&self, ptr: NonNull<u8>, layout: Layout) {
            debug_assert!(layout.align() <= page_size());
            ThreadTracker::dealloc(layout.size() as i64);
            let result = libc::munmap(ptr.cast().as_ptr(), layout.size());
            assert_eq!(result, 0, "Failed to deallocate.");
        }

        #[inline(always)]
        unsafe fn mmap_grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert!(old_layout.align() <= page_size());
            debug_assert!(old_layout.align() == new_layout.align());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            const REMAP_FLAGS: i32 = libc::MREMAP_MAYMOVE;
            let addr = libc::mremap(
                ptr.cast().as_ptr(),
                old_layout.size(),
                new_layout.size(),
                REMAP_FLAGS,
            );
            if addr == libc::MAP_FAILED {
                return Err(AllocError);
            }
            let addr = NonNull::new(addr as *mut ()).ok_or(AllocError)?;
            if linux_kernel_version() >= (5, 14, 0) {
                libc::madvise(addr.cast().as_ptr(), new_layout.size(), MADV_POPULATE_WRITE);
            }
            Ok(NonNull::<[u8]>::from_raw_parts(addr, new_layout.size()))
        }

        #[inline(always)]
        unsafe fn mmap_shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert!(old_layout.align() <= page_size());
            debug_assert!(old_layout.align() == new_layout.align());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            const REMAP_FLAGS: i32 = libc::MREMAP_MAYMOVE;
            let addr = libc::mremap(
                ptr.cast().as_ptr(),
                old_layout.size(),
                new_layout.size(),
                REMAP_FLAGS,
            );
            if addr == libc::MAP_FAILED {
                return Err(AllocError);
            }
            let addr = NonNull::new(addr as *mut ()).ok_or(AllocError)?;

            Ok(NonNull::<[u8]>::from_raw_parts(addr, new_layout.size()))
        }
    }

    unsafe impl Allocator for MmapAllocator {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            if layout.align() > page_size() {
                return self.allocator.allocate(layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_alloc(layout)
            } else {
                self.allocator.allocate(layout)
            }
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            if layout.align() > page_size() {
                return self.allocator.deallocate(ptr, layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_dealloc(ptr, layout);
            } else {
                self.allocator.deallocate(ptr, layout);
            }
        }

        #[inline(always)]
        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            if layout.align() > page_size() {
                return self.allocator.allocate_zeroed(layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_alloc(layout)
            } else {
                self.allocator.allocate_zeroed(layout)
            }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return self.allocator.grow(ptr, old_layout, new_layout);
            }
            if old_layout.size() >= THRESHOLD {
                self.mmap_grow(ptr, old_layout, new_layout)
            } else if new_layout.size() >= THRESHOLD {
                let addr = self.mmap_alloc(new_layout)?;
                std::ptr::copy_nonoverlapping(
                    ptr.as_ptr(),
                    addr.cast().as_ptr(),
                    old_layout.size(),
                );
                self.allocator.deallocate(ptr, old_layout);
                Ok(addr)
            } else {
                self.allocator.grow(ptr, old_layout, new_layout)
            }
        }

        unsafe fn grow_zeroed(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return self.allocator.grow_zeroed(ptr, old_layout, new_layout);
            }
            if old_layout.size() >= THRESHOLD {
                self.mmap_grow(ptr, old_layout, new_layout)
            } else if new_layout.size() >= THRESHOLD {
                let addr = self.mmap_alloc(new_layout)?;
                std::ptr::copy_nonoverlapping(
                    ptr.as_ptr(),
                    addr.cast().as_ptr(),
                    old_layout.size(),
                );
                self.allocator.deallocate(ptr, old_layout);
                Ok(addr)
            } else {
                self.allocator.grow_zeroed(ptr, old_layout, new_layout)
            }
        }

        unsafe fn shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return self.allocator.shrink(ptr, old_layout, new_layout);
            }
            if new_layout.size() >= THRESHOLD {
                self.mmap_shrink(ptr, old_layout, new_layout)
            } else if old_layout.size() >= THRESHOLD {
                let addr = self.allocator.allocate(new_layout)?;
                std::ptr::copy_nonoverlapping(
                    ptr.as_ptr(),
                    addr.cast().as_ptr(),
                    old_layout.size(),
                );
                self.mmap_dealloc(ptr, old_layout);
                Ok(addr)
            } else {
                self.allocator.shrink(ptr, old_layout, new_layout)
            }
        }
    }

    #[inline(always)]
    fn page_size() -> usize {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;
        const INVALID: usize = 0;
        static CACHE: AtomicUsize = AtomicUsize::new(INVALID);
        let fetch = CACHE.load(Ordering::Relaxed);
        if fetch == INVALID {
            let result = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize };
            debug_assert_eq!(result.count_ones(), 1);
            CACHE.store(result, Ordering::Relaxed);
            result
        } else {
            fetch
        }
    }

    #[inline(always)]
    fn linux_kernel_version() -> (u16, u8, u8) {
        use std::sync::atomic::AtomicU32;
        use std::sync::atomic::Ordering;
        const INVALID: u32 = 0;
        static CACHE: AtomicU32 = AtomicU32::new(INVALID);
        let fetch = CACHE.load(Ordering::Relaxed);
        let code = if fetch == INVALID {
            let mut uname = unsafe { std::mem::zeroed::<libc::utsname>() };
            assert_ne!(-1, unsafe { libc::uname(&mut uname) });
            let mut length = 0usize;

            // refer: https://semver.org/, here we stop at \0 and _
            while length < uname.release.len()
                && uname.release[length] != 0
                && uname.release[length] != 95
            {
                length += 1;
            }
            // fallback to (5.13.0)
            let fallback_version = 5u32 << 16 | 13u32 << 8;
            let slice = unsafe { &*(&uname.release[..length] as *const _ as *const [u8]) };
            let result = match std::str::from_utf8(slice) {
                Ok(ver) => match semver::Version::parse(ver) {
                    Ok(semver) => {
                        (semver.major.min(65535) as u32) << 16
                            | (semver.minor.min(255) as u32) << 8
                            | (semver.patch.min(255) as u32)
                    }
                    Err(_) => fallback_version,
                },
                Err(_) => fallback_version,
            };

            CACHE.store(result, Ordering::Relaxed);
            result
        } else {
            fetch
        };
        ((code >> 16) as u16, (code >> 8) as u8, code as u8)
    }
}

#[cfg(not(target_os = "linux"))]
pub mod not_linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use super::MmapAllocator;

    unsafe impl Allocator for MmapAllocator {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.allocator.allocate(layout)
        }

        #[inline(always)]
        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            self.allocator.allocate_zeroed(layout)
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            self.allocator.deallocate(ptr, layout)
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.allocator.grow(ptr, old_layout, new_layout)
        }

        unsafe fn grow_zeroed(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.allocator.grow_zeroed(ptr, old_layout, new_layout)
        }

        unsafe fn shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            self.allocator.shrink(ptr, old_layout, new_layout)
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_semver() {
        let uname_release: Vec<u8> =
            "4.18.0-2.4.3.xyz.x86_64.fdsf.fdsfsdfsdf.fdsafdsf\0\0\0cxzcxzcxzc"
                .as_bytes()
                .to_vec();
        let mut length = 0;
        while length < uname_release.len()
            && uname_release[length] != 0
            && uname_release[length] != 95
        {
            length += 1;
        }
        let slice = unsafe { &*(&uname_release[..length] as *const _) };
        let ver = std::str::from_utf8(slice).unwrap();
        let version = semver::Version::parse(ver);
        assert!(version.is_ok());
        let version = version.unwrap();
        assert_eq!(version.major, 4);
        assert_eq!(version.minor, 18);
        assert_eq!(version.patch, 0);
    }
}
