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
use std::ptr::NonNull;
use std::ptr::null_mut;

use crate::mem_allocator::DefaultAllocator;
use crate::mem_allocator::tracker::MetaTrackerAllocator;

pub type DefaultGlobalAllocator = GlobalAllocator<DefaultAllocator>;
pub type TrackingGlobalAllocator = GlobalAllocator<MetaTrackerAllocator<DefaultAllocator>>;

/// Global allocator, default is JeAllocator.

#[derive(Debug, Clone, Copy, Default)]
pub struct GlobalAllocator<T> {
    inner: T,
}

impl GlobalAllocator<MetaTrackerAllocator<DefaultAllocator>> {
    pub const fn create() -> GlobalAllocator<MetaTrackerAllocator<DefaultAllocator>> {
        GlobalAllocator {
            inner: MetaTrackerAllocator::create(DefaultAllocator::create()),
        }
    }

    pub fn name() -> String {
        DefaultAllocator::name()
    }

    pub fn conf() -> String {
        DefaultAllocator::conf()
    }
}

impl GlobalAllocator<DefaultAllocator> {
    pub const fn create() -> GlobalAllocator<DefaultAllocator> {
        GlobalAllocator {
            inner: DefaultAllocator::create(),
        }
    }

    pub fn name() -> String {
        DefaultAllocator::name()
    }

    pub fn conf() -> String {
        DefaultAllocator::conf()
    }
}

unsafe impl<T: Allocator> Allocator for GlobalAllocator<T> {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.allocate(layout)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        self.inner.deallocate(ptr, layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        self.inner.shrink(ptr, old_layout, new_layout)
    }
}

unsafe impl<T: Allocator> GlobalAlloc for GlobalAllocator<T> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        match self.allocate(layout) {
            Ok(ptr) => ptr.as_ptr() as *mut u8,
            Err(_) => null_mut(),
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let ptr = NonNull::new(ptr).unwrap_unchecked();
        self.deallocate(ptr, layout);
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        match self.allocate_zeroed(layout) {
            Ok(ptr) => ptr.as_ptr() as *mut u8,
            Err(_) => null_mut(),
        }
    }

    #[inline(always)]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        use std::cmp::Ordering::*;

        let ptr = NonNull::new(ptr).unwrap_unchecked();
        let new_layout = Layout::from_size_align(new_size, layout.align()).unwrap();
        match layout.size().cmp(&new_size) {
            Equal => ptr.as_ptr(),
            Less => match self.grow(ptr, layout, new_layout) {
                Ok(ptr) => ptr.as_ptr() as *mut u8,
                Err(_) => null_mut(),
            },
            Greater => match self.shrink(ptr, layout, new_layout) {
                Ok(ptr) => ptr.as_ptr() as *mut u8,
                Err(_) => null_mut(),
            },
        }
    }
}
