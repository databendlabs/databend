// Copyright 2021 Datafuse Labs.
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

#![allow(dead_code)]

cfg_if::cfg_if! {
    if #[cfg(target_os = "linux")] {
        pub type Default = self::linux::PopulateGlobal;
        pub type PopulateGlobal = self::linux::PopulateGlobal;
    } else {
        pub type Default = std::alloc::Global;
        pub type PopulateGlobal = std::alloc::Global;
    }
}

#[cfg(target_os = "linux")]
pub mod linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Global;
    use std::alloc::Layout;
    use std::ptr::null_mut;
    use std::ptr::NonNull;
    use std::sync::atomic::AtomicUsize;

    // MADV_POPULATE_WRITE is supported since Linux 5.14.
    const MADV_POPULATE_WRITE: i32 = 23;

    const THRESHOLD: usize = 64 << 20;

    #[derive(Debug, Clone, Copy, Default)]
    pub struct PopulateGlobal;

    impl PopulateGlobal {
        #[inline(always)]
        fn mmap_alloc(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert!(layout.align() <= page_size());
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
            libc::madvise(addr.cast().as_ptr(), new_layout.size(), MADV_POPULATE_WRITE);
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

    unsafe impl Allocator for PopulateGlobal {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            if layout.align() > page_size() {
                return Global.allocate(layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_alloc(layout)
            } else {
                Global.allocate(layout)
            }
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            if layout.align() > page_size() {
                return Global.deallocate(ptr, layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_dealloc(ptr, layout);
            } else {
                Global.deallocate(ptr, layout);
            }
        }

        #[inline(always)]
        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            if layout.align() > page_size() {
                return Global.allocate_zeroed(layout);
            }
            if layout.size() >= THRESHOLD {
                self.mmap_alloc(layout)
            } else {
                Global.allocate_zeroed(layout)
            }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return Global.grow(ptr, old_layout, new_layout);
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
                Global.deallocate(ptr, old_layout);
                Ok(addr)
            } else {
                Global.grow(ptr, old_layout, new_layout)
            }
        }

        unsafe fn grow_zeroed(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return Global.grow_zeroed(ptr, old_layout, new_layout);
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
                Global.deallocate(ptr, old_layout);
                Ok(addr)
            } else {
                Global.grow_zeroed(ptr, old_layout, new_layout)
            }
        }

        unsafe fn shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            if old_layout.align() > page_size() {
                return Global.shrink(ptr, old_layout, new_layout);
            }
            if new_layout.size() >= THRESHOLD {
                self.mmap_shrink(ptr, old_layout, new_layout)
            } else if old_layout.size() >= THRESHOLD {
                let addr = Global.allocate(new_layout)?;
                std::ptr::copy_nonoverlapping(
                    ptr.as_ptr(),
                    addr.cast().as_ptr(),
                    old_layout.size(),
                );
                self.mmap_dealloc(ptr, old_layout);
                Ok(addr)
            } else {
                Global.shrink(ptr, old_layout, new_layout)
            }
        }
    }

    #[inline(always)]
    fn page_size() -> usize {
        use std::sync::atomic::Ordering;
        const INVAILED: usize = 0;
        static CACHE: AtomicUsize = AtomicUsize::new(INVAILED);
        let fetch = CACHE.load(Ordering::Relaxed);
        if fetch == INVAILED {
            let result = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) as usize };
            debug_assert_eq!(result.count_ones(), 1);
            CACHE.store(result, Ordering::Relaxed);
            result
        } else {
            fetch
        }
    }
}
