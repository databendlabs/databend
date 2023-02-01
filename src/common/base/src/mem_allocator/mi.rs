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
// Most code copy from:
// https://github.com/purpleprotocol/mimalloc_rust/blob/master/src/lib.rs

use std::alloc::AllocError;
use std::alloc::Allocator;
use std::alloc::Layout;
use std::ffi::c_void;
use std::ptr::NonNull;
extern crate libmimalloc_sys as ffi;
use ffi::*;

// `MI_MAX_ALIGN_SIZE` is 16 unless manually overridden:
// https://github.com/microsoft/mimalloc/blob/15220c68/include/mimalloc-types.h#L22
//
// If it changes on us, we should consider either manually overriding it, or
// expose it from the -sys crate (in order to catch updates)
const MI_MAX_ALIGN_SIZE: usize = 16;

// Note: this doesn't take a layout directly because doing so would be wrong for
// reallocation
#[inline]
fn may_use_unaligned_api(size: usize, alignment: usize) -> bool {
    // Required by `GlobalAlloc`. Note that while allocators aren't allowed to
    // unwind in rust, this is only in debug mode, and can only happen if the
    // caller already caused UB by passing in an invalid layout.
    debug_assert!(size != 0 && alignment.is_power_of_two());

    // This logic is based on the discussion [here]. We don't bother with the
    // 3rd suggested test due to it being high cost (calling `mi_good_size`)
    // compared to the other checks, and also feeling like it relies on too much
    // implementation-specific behavior.
    //
    // [here]: https://github.com/microsoft/mimalloc/issues/314#issuecomment-708541845

    (alignment <= MI_MAX_ALIGN_SIZE && size >= alignment)
        || (alignment == size && alignment <= 4096)
}

/// std system allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct MiAllocator;

impl MiAllocator {
    pub fn name() -> String {
        "mimalloc".to_string()
    }
}

unsafe impl Allocator for MiAllocator {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size();

        let raw_ptr = if may_use_unaligned_api(layout.size(), layout.align()) {
            unsafe { mi_malloc(size) as *mut u8 }
        } else {
            unsafe { mi_malloc_aligned(layout.size(), layout.align()) as *mut u8 }
        };

        let ptr = NonNull::new(raw_ptr).ok_or(AllocError)?;
        Ok(NonNull::slice_from_raw_parts(ptr, size))
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let size = layout.size();

        let raw_ptr = if may_use_unaligned_api(size, layout.align()) {
            unsafe { mi_zalloc(size) as *mut u8 }
        } else {
            unsafe { mi_zalloc_aligned(size, layout.align()) as *mut u8 }
        };

        let ptr = NonNull::new(raw_ptr).ok_or(AllocError)?;
        Ok(NonNull::slice_from_raw_parts(ptr, size))
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, _layout: Layout) {
        mi_free(ptr.as_ptr() as *mut c_void);
    }
}
