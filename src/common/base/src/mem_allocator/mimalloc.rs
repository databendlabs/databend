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
pub struct MIAllocator;

impl MIAllocator {
    pub fn name() -> String {
        "mimalloc".to_string()
    }

    pub fn conf() -> String {
        "".to_string()
    }
}

#[cfg(target_os = "linux")]
pub mod linux {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use libc::c_void;
    use libmimalloc_sys as ffi;

    use super::MIAllocator;
    use crate::runtime::ThreadTracker;

    unsafe impl Allocator for MIAllocator {
        #[inline(always)]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            ThreadTracker::alloc(layout.size() as i64)?;

            let data_address = if layout.size() == 0 {
                unsafe { NonNull::new(layout.align() as *mut ()).unwrap_unchecked() }
            } else {
                unsafe {
                    NonNull::new(ffi::mi_malloc_aligned(layout.size(), layout.align()) as *mut ())
                        .ok_or(AllocError)?
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
                unsafe {
                    NonNull::new(ffi::mi_zalloc_aligned(layout.size(), layout.align()) as *mut ())
                        .ok_or(AllocError)?
                }
            };

            Ok(NonNull::<[u8]>::from_raw_parts(data_address, layout.size()))
        }

        #[inline(always)]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            ThreadTracker::dealloc(layout.size() as i64);

            ffi::mi_free(ptr.as_ptr() as *mut _);
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
                NonNull::new(
                    ffi::mi_malloc_aligned(new_layout.size(), new_layout.align()) as *mut (),
                )
                .ok_or(AllocError)?
            } else {
                NonNull::new(ffi::mi_realloc(ptr.cast().as_ptr(), new_layout.size()) as *mut ())
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
            debug_assert!(old_layout.size() <= new_layout.size());

            ThreadTracker::dealloc(old_layout.size() as i64);
            ThreadTracker::alloc(new_layout.size() as i64)?;

            let data_address = if new_layout.size() == 0 {
                NonNull::new(new_layout.align() as *mut ()).unwrap_unchecked()
            } else if old_layout.size() == 0 {
                NonNull::new(
                    ffi::mi_malloc_aligned(new_layout.size(), new_layout.align()) as *mut (),
                )
                .ok_or(AllocError)?
            } else {
                let raw = ffi::mi_realloc_aligned(
                    ptr.cast().as_ptr(),
                    new_layout.size(),
                    new_layout.align(),
                ) as *mut u8;
                let r = NonNull::new(raw as *mut ()).unwrap();
                raw.add(old_layout.size())
                    .write_bytes(0, new_layout.size() - old_layout.size());
                r
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

            let new_ptr = if new_layout.size() == 0 {
                ffi::mi_free(ptr.as_ptr() as *mut c_void);
                let slice = std::slice::from_raw_parts_mut(new_layout.align() as *mut u8, 0);
                NonNull::new(slice).unwrap_unchecked()
            } else {
                let data_address = ffi::mi_realloc_aligned(
                    ptr.cast().as_ptr(),
                    new_layout.size(),
                    new_layout.align(),
                ) as *mut u8;
                assert!(!data_address.is_null());
                let slice = std::slice::from_raw_parts_mut(data_address, new_layout.size());
                NonNull::new(slice).ok_or(AllocError)?
            };

            Ok(new_ptr)
        }
    }
}
