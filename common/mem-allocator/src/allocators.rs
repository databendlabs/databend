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

use crate::malloc_size::MallocSizeOf;
use crate::malloc_size::MallocSizeOfOps;
use crate::malloc_size::MallocUnconditionalSizeOf;

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

#[global_allocator]
static ALLOC: Allocator = Allocator;

pub use platform::*;
pub use tikv_jemalloc_sys;

mod platform {
    use std::alloc::GlobalAlloc;
    use std::alloc::Layout;
    use std::os::raw::c_int;
    use std::os::raw::c_void;

    use common_base::ThreadTracker;
    use tikv_jemalloc_sys as ffi;

    use crate::malloc_size::VoidPtrToSizeFn;

    /// Get the size of a heap block.
    pub unsafe extern "C" fn usable_size(ptr: *const c_void) -> usize {
        ffi::malloc_usable_size(ptr as *const _)
    }

    /// No enclosing function defined.
    #[inline]
    pub fn new_enclosing_size_fn() -> Option<VoidPtrToSizeFn> {
        None
    }

    /// Memory allocation APIs compatible with libc
    pub mod libc_compat {
        pub use super::ffi::free;
        pub use super::ffi::malloc;
        pub use super::ffi::realloc;
    }

    pub struct Allocator;

    // The minimum alignment guaranteed by the architecture. This value is used to
    // add fast paths for low alignment values.
    #[cfg(all(any(
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "mipsel",
        target_arch = "powerpc"
    )))]
    const MIN_ALIGN: usize = 8;
    #[cfg(all(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "powerpc64le",
        target_arch = "mips64",
        target_arch = "s390x",
        target_arch = "sparc64"
    )))]
    const MIN_ALIGN: usize = 16;

    fn layout_to_flags(align: usize, size: usize) -> c_int {
        // If our alignment is less than the minimum alignment they we may not
        // have to pass special flags asking for a higher alignment. If the
        // alignment is greater than the size, however, then this hits a sort of odd
        // case where we still need to ask for a custom alignment. See #25 for more
        // info.
        if align <= MIN_ALIGN && align <= size {
            0
        } else {
            // Equivalent to the MALLOCX_ALIGN(a) macro.
            align.trailing_zeros() as _
        }
    }

    unsafe impl GlobalAlloc for Allocator {
        #[inline]
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ThreadTracker::alloc_memory(layout.size() as i64);

            let flags = layout_to_flags(layout.align(), layout.size());
            ffi::mallocx(layout.size(), flags) as *mut u8
        }

        #[inline]
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            ThreadTracker::dealloc_memory(layout.size() as i64);

            let flags = layout_to_flags(layout.align(), layout.size());
            ffi::sdallocx(ptr as *mut _, layout.size(), flags)
        }

        #[inline]
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            ThreadTracker::alloc_memory(layout.size() as i64);

            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                ffi::calloc(1, layout.size()) as *mut u8
            } else {
                let flags = layout_to_flags(layout.align(), layout.size()) | ffi::MALLOCX_ZERO;
                ffi::mallocx(layout.size(), flags) as *mut u8
            }
        }

        #[inline]
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            ThreadTracker::realloc_memory(layout.size() as i64, new_size as i64);

            let flags = layout_to_flags(layout.align(), new_size);
            ffi::rallocx(ptr as *mut _, new_size, flags) as *mut u8
        }
    }
}

/// Get a new instance of a MallocSizeOfOps
pub fn new_malloc_size_ops() -> MallocSizeOfOps {
    MallocSizeOfOps::new(
        platform::usable_size,
        platform::new_enclosing_size_fn(),
        None,
    )
}

/// Extension methods for `MallocSizeOf` trait, do not implement
/// directly.
/// It allows getting heapsize without exposing `MallocSizeOfOps`
/// (a single default `MallocSizeOfOps` is used for each call).
pub trait MallocSizeOfExt: MallocSizeOf {
    /// Method to launch a heapsize measurement with a
    /// fresh state.
    fn malloc_size_of(&self) -> usize {
        let mut ops = new_malloc_size_ops();
        <Self as MallocSizeOf>::size_of(self, &mut ops)
    }
}

impl<T: MallocSizeOf> MallocSizeOfExt for T {}

impl<T: MallocSizeOf> MallocSizeOf for std::sync::Arc<T> {
    fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.unconditional_size_of(ops)
    }
}
