// Copyright 2022 Datafuse Labs.
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
use std::alloc::Layout;
use std::ptr::NonNull;

use rpmalloc_sys as ffi;

/// rp allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct RpAllocator;

unsafe impl Allocator for RpAllocator {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size();

        let raw_ptr = unsafe { ffi::rpaligned_alloc(layout.align(), size).cast::<u8>() };
        let ptr = NonNull::new(raw_ptr).ok_or(AllocError)?;
        Ok(NonNull::slice_from_raw_parts(ptr, size))
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size();

        let raw_ptr = unsafe { ffi::rpaligned_calloc(layout.align(), 1, size).cast::<u8>() };
        let ptr = NonNull::new(raw_ptr).ok_or(AllocError)?;
        Ok(NonNull::slice_from_raw_parts(ptr, size))
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, _layout: Layout) {
        let ptr = ptr.as_ptr();
        ffi::rpfree(ptr.cast::<ffi::c_void>());
    }
}
