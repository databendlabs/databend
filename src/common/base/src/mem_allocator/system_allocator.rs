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
use std::alloc::System;
use std::ptr::NonNull;

use crate::base::ThreadTracker;

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemAllocator;

unsafe impl Allocator for SystemAllocator {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let p = System.allocate(layout)?;
        ThreadTracker::alloc_memory(layout.size() as i64, &p);
        Ok(p)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        ThreadTracker::dealloc_memory(layout.size() as i64, &ptr);
        System.deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let p = System.allocate_zeroed(layout)?;

        ThreadTracker::alloc_memory(layout.size() as i64, &p);

        Ok(p)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::dealloc_memory(old_layout.size() as i64, &ptr);

        let new_ptr = System.grow(ptr, old_layout, new_layout)?;

        ThreadTracker::alloc_memory(new_layout.size() as i64, &new_ptr);
        Ok(new_ptr)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::dealloc_memory(old_layout.size() as i64, &ptr);

        let new_ptr = System.grow_zeroed(ptr, old_layout, new_layout)?;

        ThreadTracker::alloc_memory(new_layout.size() as i64, &new_ptr);
        Ok(new_ptr)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::dealloc_memory(old_layout.size() as i64, &ptr);

        let new_ptr = System.shrink(ptr, old_layout, new_layout)?;

        ThreadTracker::alloc_memory(new_layout.size() as i64, &new_ptr);
        Ok(new_ptr)
    }
}
