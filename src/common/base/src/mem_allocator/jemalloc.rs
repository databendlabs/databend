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

/// jemalloc allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct JEAllocator;

impl JEAllocator {
    pub fn name() -> String {
        "jemalloc".to_string()
    }

    pub fn conf() -> String {
        tikv_jemalloc_ctl::config::malloc_conf::mib()
            .and_then(|mib| mib.read().map(|v| v.to_owned()))
            .unwrap_or_else(|e| format!("N/A: failed to read jemalloc config, {}", e))
    }
}

#[cfg(target_os = "linux")]
pub mod linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use libc::c_int;
    use libc::c_void;
    use tikv_jemalloc_sys as ffi;

    use super::JEAllocator;
    use crate::runtime::ThreadTracker;

    #[cfg(any(target_arch = "arm", target_arch = "mips", target_arch = "powerpc"))]
    const ALIGNOF_MAX_ALIGN_T: usize = 8;
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "mips64",
        target_arch = "riscv64",
        target_arch = "s390x",
        target_arch = "sparc64"
    ))]
    const ALIGNOF_MAX_ALIGN_T: usize = 16;

    /// If `align` is less than `_Alignof(max_align_t)`, and if the requested
    /// allocation `size` is larger than the alignment, we are guaranteed to get a
    /// suitably aligned allocation by default, without passing extra flags, and
    /// this function returns `0`.
    ///
    /// Otherwise, it returns the alignment flag to pass to the jemalloc APIs.
    fn layout_to_flags(align: usize, size: usize) -> c_int {
        if align <= ALIGNOF_MAX_ALIGN_T && align <= size {
            0
        } else {
            ffi::MALLOCX_ALIGN(align)
        }
    }

    unsafe impl Allocator for JEAllocator {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            ThreadTracker::alloc(layout.size() as i64)?;

            let data_address = if layout.size() == 0 {
                unsafe { NonNull::new(layout.align() as *mut ()).unwrap_unchecked() }
            } else {
                let flags = layout_to_flags(layout.align(), layout.size());
                unsafe {
                    NonNull::new(ffi::mallocx(layout.size(), flags) as *mut ()).ok_or(AllocError)?
                }
            };
            Ok(NonNull::<[u8]>::from_raw_parts(data_address, layout.size()))
        }

        #[inline(always)]
        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            ThreadTracker::alloc(layout.size() as i64)?;

            let data_address = if layout.size() == 0 {
                unsafe { NonNull::new(layout.align() as *mut ()).unwrap_unchecked() }
            } else {
                let flags = layout_to_flags(layout.align(), layout.size()) | ffi::MALLOCX_ZERO;
                unsafe {
                    NonNull::new(ffi::mallocx(layout.size(), flags) as *mut ()).ok_or(AllocError)?
                }
            };

            Ok(NonNull::<[u8]>::from_raw_parts(data_address, layout.size()))
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            ThreadTracker::dealloc(layout.size() as i64);

            if layout.size() == 0 {
                debug_assert_eq!(ptr.as_ptr() as usize, layout.align());
            } else {
                let flags = layout_to_flags(layout.align(), layout.size());
                ffi::sdallocx(ptr.as_ptr() as *mut _, layout.size(), flags);
            }
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert_eq!(old_layout.align(), new_layout.align());
            debug_assert!(old_layout.size() <= new_layout.size());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            let data_address = if new_layout.size() == 0 {
                NonNull::new(new_layout.align() as *mut ()).unwrap_unchecked()
            } else if old_layout.size() == 0 {
                let flags = layout_to_flags(new_layout.align(), new_layout.size());
                NonNull::new(ffi::mallocx(new_layout.size(), flags) as *mut ()).ok_or(AllocError)?
            } else {
                let flags = layout_to_flags(new_layout.align(), new_layout.size());
                NonNull::new(ffi::rallocx(ptr.cast().as_ptr(), new_layout.size(), flags) as *mut ())
                    .unwrap()
            };

            Ok(NonNull::<[u8]>::from_raw_parts(
                data_address,
                new_layout.size(),
            ))
        }

        unsafe fn grow_zeroed(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert_eq!(old_layout.align(), new_layout.align());
            debug_assert!(old_layout.size() <= new_layout.size());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            let data_address = if new_layout.size() == 0 {
                NonNull::new(new_layout.align() as *mut ()).unwrap_unchecked()
            } else if old_layout.size() == 0 {
                let flags =
                    layout_to_flags(new_layout.align(), new_layout.size()) | ffi::MALLOCX_ZERO;
                NonNull::new(ffi::mallocx(new_layout.size(), flags) as *mut ()).ok_or(AllocError)?
            } else {
                let flags = layout_to_flags(new_layout.align(), new_layout.size());
                // Jemalloc doesn't support `grow_zeroed`, so it might be better to use
                // mmap allocator for large frequent memory allocation and take jemalloc
                // as fallback.
                let raw = ffi::rallocx(ptr.cast().as_ptr(), new_layout.size(), flags) as *mut u8;
                raw.add(old_layout.size())
                    .write_bytes(0, new_layout.size() - old_layout.size());
                NonNull::new(raw as *mut ()).unwrap()
            };

            Ok(NonNull::<[u8]>::from_raw_parts(
                data_address,
                new_layout.size(),
            ))
        }

        unsafe fn shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            debug_assert_eq!(old_layout.align(), new_layout.align());
            debug_assert!(old_layout.size() >= new_layout.size());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            if old_layout.size() == 0 {
                debug_assert_eq!(ptr.as_ptr() as usize, old_layout.align());
                let slice = std::slice::from_raw_parts_mut(ptr.as_ptr(), 0);
                let ptr = NonNull::new(slice).unwrap_unchecked();
                return Ok(ptr);
            }

            let flags = layout_to_flags(new_layout.align(), new_layout.size());
            let new_ptr = if new_layout.size() == 0 {
                ffi::sdallocx(ptr.as_ptr() as *mut c_void, new_layout.size(), flags);
                let slice = std::slice::from_raw_parts_mut(new_layout.align() as *mut u8, 0);
                NonNull::new(slice).unwrap_unchecked()
            } else {
                let data_address =
                    ffi::rallocx(ptr.cast().as_ptr(), new_layout.size(), flags) as *mut u8;
                let metadata = new_layout.size();
                let slice = std::slice::from_raw_parts_mut(data_address, metadata);
                NonNull::new(slice).ok_or(AllocError)?
            };

            Ok(new_ptr)
        }
    }
}

/// Other target fallback to std allocator.
#[cfg(not(target_os = "linux"))]
pub mod not_linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use super::JEAllocator;
    use crate::mem_allocator::StdAllocator;

    unsafe impl Allocator for JEAllocator {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            StdAllocator.allocate(layout)
        }

        #[inline(always)]
        fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            StdAllocator.allocate_zeroed(layout)
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            StdAllocator.deallocate(ptr, layout)
        }

        unsafe fn grow(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            StdAllocator.grow(ptr, old_layout, new_layout)
        }

        unsafe fn grow_zeroed(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            StdAllocator.grow_zeroed(ptr, old_layout, new_layout)
        }

        unsafe fn shrink(
            &self,
            ptr: NonNull<u8>,
            old_layout: Layout,
            new_layout: Layout,
        ) -> Result<NonNull<[u8]>, AllocError> {
            StdAllocator.shrink(ptr, old_layout, new_layout)
        }
    }
}
