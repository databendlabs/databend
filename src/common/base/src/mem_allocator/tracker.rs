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
use std::mem::ManuallyDrop;
use std::ptr::slice_from_raw_parts_mut;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::runtime::GlobalStatBuffer;
use crate::runtime::MemStat;
use crate::runtime::MemStatBuffer;
use crate::runtime::ThreadTracker;

#[derive(Debug, Clone, Copy)]
pub struct MetaTrackerAllocator<T: Allocator> {
    inner: T,
}

impl<T: Allocator> MetaTrackerAllocator<T> {
    pub const fn create(inner: T) -> MetaTrackerAllocator<T> {
        MetaTrackerAllocator { inner }
    }
}

impl<T: Allocator + Default> Default for MetaTrackerAllocator<T> {
    fn default() -> Self {
        MetaTrackerAllocator {
            inner: T::default(),
        }
    }
}

static META_TRACKER_THRESHOLD: usize = 512;

impl<T: Allocator> MetaTrackerAllocator<T> {
    fn alloc(&self, stat: &Arc<MemStat>, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let adjusted_layout = layout.extend_packed(meta_layout).unwrap();

        MemStatBuffer::current()
            .alloc(stat, adjusted_layout.size() as i64)
            .map_err(|_| AllocError)?;

        let Ok(allocated_ptr) = self.inner.allocate(adjusted_layout) else {
            stat.used
                .fetch_sub(adjusted_layout.size() as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = allocated_ptr.as_non_null_ptr();

        unsafe {
            base_ptr
                .add(layout.size())
                .cast::<usize>()
                .write_unaligned(Arc::into_raw(stat.clone()) as usize);

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                layout.size(),
            )))
        }
    }

    fn alloc_zeroed(
        &self,
        stat: &Arc<MemStat>,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let adjusted_layout = layout.extend_packed(meta_layout).unwrap();

        MemStatBuffer::current()
            .alloc(stat, adjusted_layout.size() as i64)
            .map_err(|_| AllocError)?;

        let Ok(allocated_ptr) = self.inner.allocate_zeroed(adjusted_layout) else {
            stat.used
                .fetch_sub(adjusted_layout.size() as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = allocated_ptr.as_non_null_ptr();

        unsafe {
            base_ptr
                .add(layout.size())
                .cast::<usize>()
                .write_unaligned(Arc::into_raw(stat.clone()) as usize);

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                layout.size(),
            )))
        }
    }

    unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout) -> Option<Layout> {
        let meta_layout = Layout::new::<usize>();
        let adjusted_layout = layout.extend_packed(meta_layout).unwrap();

        let mem_stat_address = ptr.add(layout.size()).cast::<usize>().read_unaligned();

        if mem_stat_address == 0 {
            return Some(adjusted_layout);
        }

        let mem_stat = Arc::from_raw(mem_stat_address as *const MemStat);
        MemStatBuffer::current().dealloc(&mem_stat, adjusted_layout.size() as i64);
        self.inner.deallocate(ptr, adjusted_layout);
        None
    }

    unsafe fn move_grow(
        &self,
        ptr: NonNull<u8>,
        stat: &Arc<MemStat>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

        MemStatBuffer::current()
            .alloc(stat, new_adjusted_layout.size() as i64)
            .map_err(|_| AllocError)?;
        GlobalStatBuffer::current().dealloc(old_layout.size() as i64);

        let Ok(grow_ptr) = self.inner.grow(ptr, old_layout, new_adjusted_layout) else {
            GlobalStatBuffer::current().force_alloc(old_layout.size() as i64);
            stat.used
                .fetch_sub(new_adjusted_layout.size() as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = grow_ptr.as_non_null_ptr();

        unsafe {
            base_ptr
                .add(new_layout.size())
                .cast::<usize>()
                .write_unaligned(Arc::into_raw(stat.clone()) as usize);

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )))
        }
    }

    unsafe fn grow_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let old_adjusted_layout = old_layout.extend_packed(meta_layout).unwrap();
        let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

        let address = ptr.add(old_layout.size()).cast::<usize>().read_unaligned();

        if address == 0 {
            let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;

            let Ok(grow_ptr) = self
                .inner
                .grow(ptr, old_adjusted_layout, new_adjusted_layout)
            else {
                GlobalStatBuffer::current().force_alloc(-(diff as i64));
                return Err(AllocError);
            };

            let mut base_ptr = grow_ptr.as_non_null_ptr();
            base_ptr
                .add(new_layout.size())
                .cast::<usize>()
                .write_unaligned(address);

            return Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )));
        }

        let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
        let stat = ManuallyDrop::new(Arc::from_raw(address as *const MemStat));

        MemStatBuffer::current()
            .alloc(&stat, diff as i64)
            .map_err(|_| AllocError)?;

        let Ok(grow_ptr) = self
            .inner
            .grow(ptr, old_adjusted_layout, new_adjusted_layout)
        else {
            stat.used.fetch_sub(diff as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = grow_ptr.as_non_null_ptr();
        base_ptr
            .add(new_layout.size())
            .cast::<usize>()
            .write_unaligned(address);

        Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
            base_ptr.as_mut(),
            new_layout.size(),
        )))
    }

    unsafe fn move_grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        stat: &Arc<MemStat>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

        MemStatBuffer::current()
            .alloc(stat, new_adjusted_layout.size() as i64)
            .map_err(|_| AllocError)?;
        GlobalStatBuffer::current().dealloc(new_adjusted_layout.size() as i64);

        let Ok(grow_ptr) = self.inner.grow_zeroed(ptr, old_layout, new_adjusted_layout) else {
            GlobalStatBuffer::current().force_alloc(old_layout.size() as i64);
            stat.used
                .fetch_sub(new_adjusted_layout.size() as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = grow_ptr.as_non_null_ptr();

        unsafe {
            base_ptr
                .add(new_layout.size())
                .cast::<usize>()
                .write_unaligned(Arc::into_raw(stat.clone()) as usize);

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )))
        }
    }

    unsafe fn grow_zeroed_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let old_adjusted_layout = old_layout.extend_packed(meta_layout).unwrap();
        let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

        let address = ptr
            .add(old_adjusted_layout.size())
            .cast::<usize>()
            .read_unaligned();

        if address == 0 {
            let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;

            let Ok(grow_ptr) =
                self.inner
                    .grow_zeroed(ptr, old_adjusted_layout, new_adjusted_layout)
            else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            let mut base_ptr = grow_ptr.as_non_null_ptr();

            return Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )));
        }

        let alloc_size = new_adjusted_layout.size() - old_adjusted_layout.size();
        let stat = ManuallyDrop::new(Arc::from_raw(address as *const MemStat));

        MemStatBuffer::current()
            .alloc(&stat, alloc_size as i64)
            .map_err(|_| AllocError)?;

        let Ok(grow_ptr) = self
            .inner
            .grow_zeroed(ptr, old_adjusted_layout, new_adjusted_layout)
        else {
            stat.used.fetch_sub(alloc_size as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = grow_ptr.as_non_null_ptr();
        base_ptr
            .add(old_layout.size())
            .cast::<usize>()
            .write_unaligned(0);
        base_ptr
            .add(new_layout.size())
            .cast::<usize>()
            .write_unaligned(address);

        Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
            base_ptr.as_mut(),
            new_layout.size(),
        )))
    }

    unsafe fn move_shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let old_adjusted_layout = old_layout.extend_packed(meta_layout).unwrap();

        let mem_stat_address = ptr.add(old_layout.size()).cast::<usize>().read_unaligned();

        if mem_stat_address == 0 {
            let Ok(reduced_ptr) = self.inner.shrink(ptr, old_adjusted_layout, new_layout) else {
                return Err(AllocError);
            };

            let diff = old_adjusted_layout.size() - new_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            return Ok(reduced_ptr);
        }

        let mem_stat = ManuallyDrop::new(Arc::from_raw(mem_stat_address as *const MemStat));
        MemStatBuffer::current().dealloc(&mem_stat, old_adjusted_layout.size() as i64);
        let Ok(reduced_ptr) = self.inner.shrink(ptr, old_adjusted_layout, new_layout) else {
            mem_stat
                .used
                .fetch_add(old_adjusted_layout.size() as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        GlobalStatBuffer::current().force_alloc(new_layout.size() as i64);
        Ok(reduced_ptr)
    }

    unsafe fn shrink_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let meta_layout = Layout::new::<usize>();
        let old_adjusted_layout = old_layout.extend_packed(meta_layout).unwrap();
        let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

        let address = ptr.add(old_layout.size()).cast::<usize>().read_unaligned();

        if address == 0 {
            let diff = old_adjusted_layout.size() - new_adjusted_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            let Ok(ptr) = self
                .inner
                .shrink(ptr, old_adjusted_layout, new_adjusted_layout)
            else {
                GlobalStatBuffer::current().force_alloc(diff as i64);
                return Err(AllocError);
            };

            let mut base_ptr = ptr.as_non_null_ptr();
            base_ptr
                .add(new_layout.size())
                .cast::<usize>()
                .write_unaligned(0);

            return Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )));
        }

        let alloc_size = old_adjusted_layout.size() - new_adjusted_layout.size();
        let stat = ManuallyDrop::new(Arc::from_raw(address as *const MemStat));
        MemStatBuffer::current().dealloc(&stat, alloc_size as i64);

        let Ok(ptr) = self
            .inner
            .shrink(ptr, old_adjusted_layout, new_adjusted_layout)
        else {
            stat.used.fetch_add(alloc_size as i64, Ordering::Relaxed);
            return Err(AllocError);
        };

        let mut base_ptr = ptr.as_non_null_ptr();
        base_ptr
            .add(new_layout.size())
            .cast::<usize>()
            .write_unaligned(address);

        Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
            base_ptr.as_mut(),
            new_layout.size(),
        )))
    }
}

unsafe impl<T: Allocator> Allocator for MetaTrackerAllocator<T> {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() >= META_TRACKER_THRESHOLD {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return self.alloc(mem_stat, layout);
            }

            let meta_layout = Layout::new::<usize>();
            let adjusted_layout = layout.extend_packed(meta_layout).unwrap();

            GlobalStatBuffer::current()
                .alloc(adjusted_layout.size() as i64)
                .map_err(|_| AllocError)?;
            let Ok(allocated_ptr) = self.inner.allocate(adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(adjusted_layout.size() as i64);
                return Err(AllocError);
            };

            unsafe {
                let mut base_ptr = allocated_ptr.as_non_null_ptr();
                base_ptr
                    .add(layout.size())
                    .cast::<usize>()
                    .write_unaligned(0);

                return Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                    base_ptr.as_mut(),
                    layout.size(),
                )));
            }
        }

        GlobalStatBuffer::current()
            .alloc(layout.size() as i64)
            .map_err(|_| AllocError)?;
        let Ok(allocated_ptr) = self.inner.allocate(layout) else {
            GlobalStatBuffer::current().dealloc(layout.size() as i64);
            return Err(AllocError);
        };

        Ok(allocated_ptr)
    }

    #[inline(always)]
    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() >= META_TRACKER_THRESHOLD {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return self.alloc_zeroed(mem_stat, layout);
            }

            let meta_layout = Layout::new::<usize>();
            let adjusted_layout = layout.extend_packed(meta_layout).unwrap();
            GlobalStatBuffer::current()
                .alloc(adjusted_layout.size() as i64)
                .map_err(|_| AllocError)?;

            let Ok(allocated_ptr) = self.inner.allocate(adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(adjusted_layout.size() as i64);
                return Err(AllocError);
            };

            unsafe {
                return Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                    allocated_ptr.as_non_null_ptr().as_mut(),
                    layout.size(),
                )));
            }
        }

        GlobalStatBuffer::current()
            .alloc(layout.size() as i64)
            .map_err(|_| AllocError)?;
        let Ok(allocated_ptr) = self.inner.allocate_zeroed(layout) else {
            GlobalStatBuffer::current().dealloc(layout.size() as i64);
            return Err(AllocError);
        };

        Ok(allocated_ptr)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() >= META_TRACKER_THRESHOLD {
            if let Some(layout) = self.dealloc(ptr, layout) {
                GlobalStatBuffer::current().dealloc(layout.size() as i64);
                self.inner.deallocate(ptr, layout);
            }

            return;
        }

        GlobalStatBuffer::current().dealloc(layout.size() as i64);
        self.inner.deallocate(ptr, layout);
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            self.grow_impl(ptr, old_layout, new_layout)
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = new_layout.size() - old_layout.size();

            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;
            let Ok(grow_ptr) = self.inner.grow(ptr, old_layout, new_layout) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(grow_ptr)
        } else {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return self.move_grow(ptr, mem_stat, old_layout, new_layout);
            }

            let meta_layout = Layout::new::<usize>();
            let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

            let diff = new_adjusted_layout.size() - old_layout.size();
            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;

            let Ok(grow_ptr) = self.inner.grow(ptr, old_layout, new_adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            let mut base_ptr = grow_ptr.as_non_null_ptr();
            base_ptr
                .add(new_layout.size())
                .cast::<usize>()
                .write_unaligned(0);

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                base_ptr.as_mut(),
                new_layout.size(),
            )))
        }
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            self.grow_zeroed_impl(ptr, old_layout, new_layout)
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = new_layout.size() - old_layout.size();
            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;
            let Ok(grow_ptr) = self.inner.grow_zeroed(ptr, old_layout, new_layout) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(grow_ptr)
        } else {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return self.move_grow_zeroed(ptr, mem_stat, old_layout, new_layout);
            }

            let meta_layout = Layout::new::<usize>();
            let new_adjusted_layout = new_layout.extend_packed(meta_layout).unwrap();

            let diff = new_adjusted_layout.size() - old_layout.size();
            GlobalStatBuffer::current()
                .alloc(diff as i64)
                .map_err(|_| AllocError)?;
            let Ok(grow_ptr) = self.inner.grow_zeroed(ptr, old_layout, new_adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(NonNull::new_unchecked(slice_from_raw_parts_mut(
                grow_ptr.as_non_null_ptr().as_mut(),
                new_layout.size(),
            )))
        }
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            self.shrink_impl(ptr, old_layout, new_layout)
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = old_layout.size() - new_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            let Ok(shrink_ptr) = self.inner.shrink(ptr, old_layout, new_layout) else {
                GlobalStatBuffer::current().force_alloc(diff as i64);
                return Err(AllocError);
            };

            Ok(shrink_ptr)
        } else {
            self.move_shrink(ptr, old_layout, new_layout)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use crate::mem_allocator::tracker::MetaTrackerAllocator;
    use crate::mem_allocator::StdAllocator;
    use crate::runtime::MemStat;
    use crate::runtime::Thread;
    use crate::runtime::ThreadTracker;

    static TAIL_SIZE: i64 = 8;

    #[test]
    fn test_alloc_with_mem_stat() -> Result<(), AllocError> {
        let mem_stat = MemStat::create(String::from("test"));

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.mem_stat = Some(mem_stat.clone());
        let _guard = ThreadTracker::tracking(tracking_payload);

        let allocator = MetaTrackerAllocator::<StdAllocator>::default();

        let mut sum = 0;
        while sum <= 4 * 1024 * 2014 {
            let small_allocate = allocator.allocate(Layout::array::<u8>(511).unwrap())?;
            assert_eq!(small_allocate.len(), 511);
            sum += small_allocate.len() as i64;
        }

        // un-tracking small allocate
        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);

        let mut sum = 0;
        while sum <= 4 * 1024 * 1024 {
            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
            let large_allocate = allocator.allocate(Layout::array::<u8>(512).unwrap())?;
            assert_eq!(large_allocate.len(), 512);
            sum += 512 + TAIL_SIZE;
        }

        assert_eq!(mem_stat.used.load(Ordering::Relaxed), sum);

        let memory_usage = mem_stat.used.load(Ordering::Relaxed);
        let large_allocate = allocator.allocate(Layout::array::<u8>(4 * 1024 * 1024).unwrap())?;
        assert_eq!(large_allocate.len(), 4 * 1024 * 1024);
        assert_eq!(
            mem_stat.used.load(Ordering::Relaxed),
            memory_usage + 4 * 1024 * 1024 + TAIL_SIZE
        );
        Ok(())
    }

    #[test]
    fn test_dealloc_with_same_thread() -> Result<(), AllocError> {
        unsafe {
            let mem_stat = MemStat::create(String::from("test"));

            let mut tracking_payload = ThreadTracker::new_tracking_payload();
            tracking_payload.mem_stat = Some(mem_stat.clone());
            let _guard = ThreadTracker::tracking(tracking_payload);

            let allocator = MetaTrackerAllocator::<StdAllocator>::default();

            let mut sum = 0;
            while sum <= 4 * 1024 * 2014 {
                let small_allocate = allocator.allocate(Layout::array::<u8>(511).unwrap())?;
                assert_eq!(small_allocate.len(), 511);
                allocator.deallocate(
                    small_allocate.as_non_null_ptr(),
                    Layout::array::<u8>(511).unwrap(),
                );
                sum += small_allocate.len() as i64;
            }

            // un-tracking small allocate
            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);

            let mut sum = 0;
            while sum <= 4 * 1024 * 1024 {
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                let large_allocate = allocator.allocate(Layout::array::<u8>(512).unwrap())?;
                assert_eq!(large_allocate.len(), 512);
                allocator.deallocate(
                    large_allocate.as_non_null_ptr(),
                    Layout::array::<u8>(512).unwrap(),
                );
                sum += 512 + TAIL_SIZE;
            }

            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);

            let large_allocate =
                allocator.allocate(Layout::array::<u8>(4 * 1024 * 1024).unwrap())?;
            assert_eq!(large_allocate.len(), 4 * 1024 * 1024);
            assert_eq!(
                mem_stat.used.load(Ordering::Relaxed),
                4 * 1024 * 1024 + TAIL_SIZE
            );
            allocator.deallocate(
                large_allocate.as_non_null_ptr(),
                Layout::array::<u8>(4 * 1024 * 1024).unwrap(),
            );
            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
            assert_eq!(Arc::strong_count(&mem_stat), 2);
            drop(_guard);
            assert_eq!(Arc::strong_count(&mem_stat), 1);
        }

        Ok(())
    }

    #[test]
    fn test_dealloc_with_diff_thread() -> Result<(), AllocError> {
        unsafe {
            let mem_stat = MemStat::create(String::from("test"));

            let mut tracking_payload = ThreadTracker::new_tracking_payload();
            tracking_payload.mem_stat = Some(mem_stat.clone());

            let allocator = Arc::new(MetaTrackerAllocator::<StdAllocator>::default());

            let join_handle = Thread::spawn({
                let tracking_payload = tracking_payload.clone();
                let allocator = allocator.clone();
                move || {
                    let _guard = ThreadTracker::tracking(tracking_payload.clone());

                    let mut small_allocates = Vec::new();

                    let mut sum = 0;
                    while sum <= 4 * 1024 * 2014 {
                        let small_allocate = allocator
                            .allocate(Layout::array::<u8>(511).unwrap())
                            .unwrap();
                        assert_eq!(small_allocate.len(), 511);
                        sum += small_allocate.len() as i64;
                        small_allocates.push(small_allocate.as_non_null_ptr().as_ptr() as usize);
                    }

                    small_allocates
                }
            });

            let small_allocates = join_handle.join().unwrap();
            for small_allocate in small_allocates {
                // un-tracking small allocate
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                allocator.deallocate(
                    NonNull::new(small_allocate as *const u8 as *mut u8).unwrap(),
                    Layout::array::<u8>(511).unwrap(),
                );
            }

            let join_handle = Thread::spawn({
                let tracking_payload = tracking_payload.clone();
                let allocator = allocator.clone();
                move || {
                    let _guard = ThreadTracker::tracking(tracking_payload.clone());

                    let mut large_allocates = Vec::new();

                    let mut sum = 0;
                    while sum <= 4 * 1024 * 1024 {
                        let large_allocate = allocator
                            .allocate(Layout::array::<u8>(512).unwrap())
                            .unwrap();
                        assert_eq!(large_allocate.len(), 512);
                        large_allocates.push(large_allocate.as_non_null_ptr().as_ptr() as usize);
                        sum += 512 + TAIL_SIZE;
                    }

                    (sum, large_allocates)
                }
            });

            let (sum, large_allocates) = join_handle.join().unwrap();
            for large_allocate in large_allocates {
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), sum);
                allocator.deallocate(
                    NonNull::new(large_allocate as *const u8 as *mut u8).unwrap(),
                    Layout::array::<u8>(512).unwrap(),
                );
            }

            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);

            let join_handle = Thread::spawn({
                let allocator = allocator.clone();
                move || {
                    let _guard = ThreadTracker::tracking(tracking_payload.clone());

                    let large_allocate = allocator
                        .allocate(Layout::array::<u8>(4 * 1024 * 1024).unwrap())
                        .unwrap();
                    assert_eq!(large_allocate.len(), 4 * 1024 * 1024);

                    large_allocate.as_non_null_ptr().as_ptr() as usize
                }
            });

            let large_allocate = join_handle.join().unwrap();
            assert_eq!(
                mem_stat.used.load(Ordering::Relaxed),
                4 * 1024 * 1024 + TAIL_SIZE
            );
            allocator.deallocate(
                NonNull::new(large_allocate as *const u8 as *mut u8).unwrap(),
                Layout::array::<u8>(4 * 1024 * 1024).unwrap(),
            );
            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
            assert_eq!(Arc::strong_count(&mem_stat), 1);
        }

        Ok(())
    }
}
