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
        ThreadTracker::alloc_memory(layout.size() as i64);
        System.allocate(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        ThreadTracker::dealloc_memory(layout.size() as i64);
        System.deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::alloc_memory(layout.size() as i64);
        System.allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::grow_memory(old_layout.size() as i64, new_layout.size() as i64);
        System.grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::grow_memory(old_layout.size() as i64, new_layout.size() as i64);
        System.grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        ThreadTracker::shrink_memory(old_layout.size() as i64, new_layout.size() as i64);
        System.shrink(ptr, old_layout, new_layout)
    }
}
