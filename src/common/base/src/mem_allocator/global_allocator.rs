use std::alloc::AllocError;
use std::alloc::Allocator;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::ptr::null_mut;
use std::ptr::NonNull;

use super::je_allocator::JEAllocator;
use super::system_allocator::SystemAllocator;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

#[derive(Debug, Clone, Copy, Default)]
pub struct GlobalAllocator;

type Fallback = JEAllocator<SystemAllocator>;

unsafe impl Allocator for GlobalAllocator {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Fallback::new(Default::default()).allocate(layout)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Fallback::new(Default::default()).deallocate(ptr, layout)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Fallback::new(Default::default()).allocate_zeroed(layout)
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Fallback::new(Default::default()).grow(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Fallback::new(Default::default()).grow_zeroed(ptr, old_layout, new_layout)
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        Fallback::new(Default::default()).shrink(ptr, old_layout, new_layout)
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

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = NonNull::new(ptr).unwrap_unchecked();
        let new_layout = Layout::from_size_align(new_size, layout.align()).unwrap();
        if layout.size() < new_size {
            if let Ok(ptr) = GlobalAllocator.grow(ptr, layout, new_layout) {
                ptr.as_ptr() as *mut u8
            } else {
                null_mut()
            }
        } else if layout.size() > new_size {
            if let Ok(ptr) = GlobalAllocator.shrink(ptr, layout, new_layout) {
                ptr.as_ptr() as *mut u8
            } else {
                null_mut()
            }
        } else {
            ptr.as_ptr() as *mut u8
        }
    }
}
