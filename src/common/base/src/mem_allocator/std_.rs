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
use std::alloc::Layout;
use std::alloc::System;
use std::ptr::NonNull;

use crate::runtime::ThreadTracker;

/// std system allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdAllocator;

impl StdAllocator {
    pub fn name() -> String {
        "std".to_string()
    }

    pub fn conf() -> String {
        "".to_string()
    }
}

unsafe impl Allocator for StdAllocator {
    #[inline(always)]
    fn allocate(&self, raw_layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let layout = ThreadTracker::pack_layout(raw_layout);
        ThreadTracker::alloc(layout.size() as i64)?;
        let allocate_ptr = System.allocate(layout)?;
        Ok(ThreadTracker::pack_tracker(raw_layout, allocate_ptr))
    }

    #[inline(always)]
    fn allocate_zeroed(&self, raw_layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let layout = ThreadTracker::pack_layout(raw_layout);
        ThreadTracker::alloc(layout.size() as i64)?;
        let allocate_ptr = System.allocate_zeroed(layout)?;
        Ok(ThreadTracker::pack_tracker(raw_layout, allocate_ptr))
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, raw_layout: Layout) {
        let layout = ThreadTracker::pack_layout(raw_layout);
        ThreadTracker::dealloc(layout.size() as i64);
        System.deallocate(ptr, layout);
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        raw_old_layout: Layout,
        raw_new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_layout = ThreadTracker::pack_layout(raw_old_layout);
        let new_layout = ThreadTracker::pack_layout(raw_new_layout);

        ThreadTracker::dealloc(old_layout.size() as i64);
        ThreadTracker::alloc(new_layout.size() as i64)?;

        let grow_ptr = System.grow(ptr, old_layout, new_layout)?;

        Ok(ThreadTracker::grow_pack(
            raw_old_layout,
            raw_new_layout,
            grow_ptr,
        ))
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        raw_old_layout: Layout,
        raw_new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_layout = ThreadTracker::pack_layout(raw_old_layout);
        let new_layout = ThreadTracker::pack_layout(raw_new_layout);
        ThreadTracker::dealloc(old_layout.size() as i64);
        ThreadTracker::alloc(new_layout.size() as i64)?;

        let grow_ptr = System.grow_zeroed(ptr, old_layout, new_layout)?;
        Ok(ThreadTracker::grow_pack(
            raw_old_layout,
            raw_new_layout,
            grow_ptr,
        ))
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        raw_old_layout: Layout,
        raw_new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_layout = ThreadTracker::pack_layout(raw_old_layout);
        let new_layout = ThreadTracker::pack_layout(raw_new_layout);

        ThreadTracker::shrink_pack(raw_old_layout, raw_new_layout, ptr);

        ThreadTracker::dealloc(old_layout.size() as i64);
        ThreadTracker::alloc(new_layout.size() as i64)?;

        let shrink_ptr = System.shrink(ptr, old_layout, new_layout)?;
        Ok(NonNull::from_raw_parts(
            shrink_ptr.to_raw_parts().0,
            raw_new_layout.size(),
        ))
    }
}
