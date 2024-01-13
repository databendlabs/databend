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
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::ptr::null_mut;
use std::ptr::NonNull;

use crate::mem_allocator::DefaultAllocator;

/// Global allocator, default is JeAllocator.

#[derive(Debug, Clone, Copy, Default)]
pub struct GlobalAllocator;

impl GlobalAllocator {
    pub fn name() -> String {
        DefaultAllocator::name()
    }

    pub fn conf() -> String {
        DefaultAllocator::conf()
    }
}

unsafe impl Allocator for GlobalAllocator {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        DefaultAllocator::default().allocate(layout)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        DefaultAllocator::default().allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        DefaultAllocator::default().deallocate(ptr, layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        DefaultAllocator::default().grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        DefaultAllocator::default().grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        DefaultAllocator::default().shrink(ptr, old_layout, new_layout)
    }
}

unsafe impl GlobalAlloc for GlobalAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if let Ok(ptr) = GlobalAllocator.allocate(layout) {
            ptr.as_ptr() as *mut u8
        } else {
            null_mut()
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let ptr = NonNull::new(ptr).unwrap_unchecked();
        GlobalAllocator.deallocate(ptr, layout);
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if let Ok(ptr) = GlobalAllocator.allocate_zeroed(layout) {
            ptr.as_ptr() as *mut u8
        } else {
            null_mut()
        }
    }

    #[inline(always)]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        use std::cmp::Ordering::*;

        let ptr = NonNull::new(ptr).unwrap_unchecked();
        let new_layout = Layout::from_size_align(new_size, layout.align()).unwrap();
        match layout.size().cmp(&new_size) {
            Less => {
                if let Ok(ptr) = GlobalAllocator.grow(ptr, layout, new_layout) {
                    ptr.as_ptr() as *mut u8
                } else {
                    null_mut()
                }
            }
            Greater => {
                if let Ok(ptr) = GlobalAllocator.shrink(ptr, layout, new_layout) {
                    ptr.as_ptr() as *mut u8
                } else {
                    null_mut()
                }
            }
            Equal => ptr.as_ptr(),
        }
    }
}
