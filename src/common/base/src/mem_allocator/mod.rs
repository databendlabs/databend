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

mod je_allocator;
mod mmap_allocator;
mod stackful_allocator;
mod std_allocator;

use std::alloc::Layout;

pub use je_allocator::JEAllocator;
pub use mmap_allocator::MmapAllocator;
pub use stackful_allocator::StackfulAllocator;
pub use std_allocator::StdAllocator;

#[cfg(feature = "memory-profiling")]
mod profiling;

#[cfg(feature = "memory-profiling")]
pub use profiling::dump_profile;

// A new Allocator trait suits databend
pub unsafe trait Allocator {
    unsafe fn allocx(&mut self, layout: Layout, clear_mem: bool) -> *mut u8;
    unsafe fn deallocx(&mut self, ptr: *mut u8, layout: Layout);
    unsafe fn reallocx(
        &mut self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
        clear_mem: bool,
    ) -> *mut u8 {
        // SAFETY: the caller must ensure that the `new_size` does not overflow.
        // `layout.align()` comes from a `Layout` and is thus guaranteed to be valid.
        let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
        // SAFETY: the caller must ensure that `new_layout` is greater than zero.
        let new_ptr = self.allocx(new_layout, clear_mem);
        if !new_ptr.is_null() {
            // SAFETY: the previously allocated block cannot overlap the newly allocated block.
            // The safety contract for `dealloc` must be upheld by the caller.
            std::ptr::copy_nonoverlapping(ptr, new_ptr, std::cmp::min(layout.size(), new_size));
            self.deallocx(ptr, layout);
        }
        new_ptr
    }
}
