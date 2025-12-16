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
use std::ptr::NonNull;
use std::ptr::slice_from_raw_parts_mut;
use std::sync::Arc;

use crate::runtime::GlobalStatBuffer;
use crate::runtime::MemStat;
use crate::runtime::MemStatBuffer;
use crate::runtime::ThreadTracker;

/// Memory allocation tracker threshold (512 bytes)
static META_TRACKER_THRESHOLD: usize = 512;

/// An allocator wrapper that tracks memory usage statistics through metadata.
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

impl<T: Allocator> MetaTrackerAllocator<T> {
    fn metadata_layout() -> Layout {
        Layout::new::<usize>()
    }

    fn adjusted_layout(base_layout: Layout) -> Layout {
        base_layout.extend_packed(Self::metadata_layout()).unwrap()
    }

    fn with_meta(base: NonNull<[u8]>, layout: Layout, address: usize) -> NonNull<[u8]> {
        let mut base_ptr = base.as_non_null_ptr();

        unsafe {
            base_ptr
                .add(layout.size())
                .cast::<usize>()
                .write_unaligned(address);

            NonNull::new_unchecked(slice_from_raw_parts_mut(base_ptr.as_mut(), layout.size()))
        }
    }

    fn alloc(&self, stat: &Arc<MemStat>, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let adjusted_layout = Self::adjusted_layout(layout);
        MemStatBuffer::current().alloc(stat, adjusted_layout.size() as i64)?;

        let Ok(allocated_ptr) = self.inner.allocate(adjusted_layout) else {
            MemStatBuffer::current().dealloc(stat, adjusted_layout.size() as i64);
            return Err(AllocError);
        };

        let address = Arc::into_raw(stat.clone()) as usize;
        Ok(Self::with_meta(allocated_ptr, layout, address))
    }

    fn alloc_zeroed(
        &self,
        stat: &Arc<MemStat>,
        layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let adjusted_layout = Self::adjusted_layout(layout);
        MemStatBuffer::current().alloc(stat, adjusted_layout.size() as i64)?;

        let Ok(allocated_ptr) = self.inner.allocate_zeroed(adjusted_layout) else {
            MemStatBuffer::current().dealloc(stat, adjusted_layout.size() as i64);
            return Err(AllocError);
        };

        let address = Arc::into_raw(stat.clone()) as usize;
        Ok(Self::with_meta(allocated_ptr, layout, address))
    }

    unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout) -> Option<Layout> {
        let adjusted_layout = Self::adjusted_layout(layout);
        let mem_stat_address = unsafe { ptr.add(layout.size()).cast::<usize>().read_unaligned() };

        if mem_stat_address == 0 {
            return Some(adjusted_layout);
        }

        let mem_stat = unsafe { Arc::from_raw(mem_stat_address as *const MemStat) };
        MemStatBuffer::current().dealloc(&mem_stat, adjusted_layout.size() as i64);
        unsafe { self.inner.deallocate(ptr, adjusted_layout) };
        None
    }

    unsafe fn move_grow(
        &self,
        ptr: NonNull<u8>,
        stat: &Arc<MemStat>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let new_adjusted_layout = Self::adjusted_layout(new_layout);
        MemStatBuffer::current().alloc(stat, new_adjusted_layout.size() as i64)?;
        GlobalStatBuffer::current().dealloc(old_layout.size() as i64);

        let Ok(grow_ptr) = (unsafe { self.inner.grow(ptr, old_layout, new_adjusted_layout) })
        else {
            GlobalStatBuffer::current().force_alloc(old_layout.size() as i64);
            MemStatBuffer::current().dealloc(stat, new_adjusted_layout.size() as i64);
            return Err(AllocError);
        };

        let address = Arc::into_raw(stat.clone()) as usize;
        Ok(Self::with_meta(grow_ptr, new_layout, address))
    }

    unsafe fn grow_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_adjusted_layout = Self::adjusted_layout(old_layout);
        let new_adjusted_layout = Self::adjusted_layout(new_layout);
        let address = unsafe { ptr.add(old_layout.size()).cast::<usize>().read_unaligned() };

        if address == 0 {
            let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
            GlobalStatBuffer::current().alloc(diff as i64)?;

            let Ok(grow_ptr) = (unsafe {
                self.inner
                    .grow(ptr, old_adjusted_layout, new_adjusted_layout)
            }) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            return Ok(Self::with_meta(grow_ptr, new_layout, address));
        }

        let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
        let stat = ManuallyDrop::new(unsafe { Arc::from_raw(address as *const MemStat) });

        MemStatBuffer::current().alloc(&stat, diff as i64)?;

        let Ok(grow_ptr) = (unsafe {
            self.inner
                .grow(ptr, old_adjusted_layout, new_adjusted_layout)
        }) else {
            MemStatBuffer::current().dealloc(&stat, diff as i64);
            return Err(AllocError);
        };

        Ok(Self::with_meta(grow_ptr, new_layout, address))
    }

    unsafe fn move_grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        stat: &Arc<MemStat>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let new_adjusted_layout = Self::adjusted_layout(new_layout);

        MemStatBuffer::current().alloc(stat, new_adjusted_layout.size() as i64)?;
        GlobalStatBuffer::current().dealloc(old_layout.size() as i64);

        let Ok(grow_ptr) =
            (unsafe { self.inner.grow_zeroed(ptr, old_layout, new_adjusted_layout) })
        else {
            GlobalStatBuffer::current().force_alloc(old_layout.size() as i64);
            MemStatBuffer::current().dealloc(stat, new_adjusted_layout.size() as i64);
            return Err(AllocError);
        };

        let address = Arc::into_raw(stat.clone()) as usize;
        Ok(Self::with_meta(grow_ptr, new_layout, address))
    }

    unsafe fn grow_zeroed_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_adjusted_layout = Self::adjusted_layout(old_layout);
        let new_adjusted_layout = Self::adjusted_layout(new_layout);

        let address = unsafe { ptr.add(old_layout.size()).cast::<usize>().read_unaligned() };

        if address == 0 {
            let diff = new_adjusted_layout.size() - old_adjusted_layout.size();
            GlobalStatBuffer::current().alloc(diff as i64)?;

            let Ok(grow_ptr) = (unsafe {
                self.inner
                    .grow_zeroed(ptr, old_adjusted_layout, new_adjusted_layout)
            }) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            return Ok(Self::with_meta(grow_ptr, new_layout, 0));
        }

        let alloc_size = new_adjusted_layout.size() - old_adjusted_layout.size();
        let stat = ManuallyDrop::new(unsafe { Arc::from_raw(address as *const MemStat) });

        MemStatBuffer::current().alloc(&stat, alloc_size as i64)?;

        let Ok(grow_ptr) = (unsafe {
            self.inner
                .grow_zeroed(ptr, old_adjusted_layout, new_adjusted_layout)
        }) else {
            MemStatBuffer::current().dealloc(&stat, alloc_size as i64);
            return Err(AllocError);
        };

        unsafe {
            grow_ptr
                .as_non_null_ptr()
                .add(old_layout.size())
                .cast::<usize>()
                .write_unaligned(0);
        }

        Ok(Self::with_meta(grow_ptr, new_layout, address))
    }

    unsafe fn move_shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_adjusted_layout = Self::adjusted_layout(old_layout);

        let mem_stat_address =
            unsafe { ptr.add(old_layout.size()).cast::<usize>().read_unaligned() };

        if mem_stat_address == 0 {
            let Ok(reduced_ptr) =
                (unsafe { self.inner.shrink(ptr, old_adjusted_layout, new_layout) })
            else {
                return Err(AllocError);
            };

            let diff = old_adjusted_layout.size() - new_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            return Ok(reduced_ptr);
        }

        let Ok(reduced_ptr) = (unsafe { self.inner.shrink(ptr, old_adjusted_layout, new_layout) })
        else {
            return Err(AllocError);
        };

        let mem_stat = unsafe { Arc::from_raw(mem_stat_address as *const MemStat) };
        MemStatBuffer::current().dealloc(&mem_stat, old_adjusted_layout.size() as i64);
        GlobalStatBuffer::current().force_alloc(new_layout.size() as i64);
        Ok(reduced_ptr)
    }

    unsafe fn shrink_impl(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        let old_adjusted_layout = Self::adjusted_layout(old_layout);
        let new_adjusted_layout = Self::adjusted_layout(new_layout);

        let address = unsafe { ptr.add(old_layout.size()).cast::<usize>().read_unaligned() };

        if address == 0 {
            let diff = old_adjusted_layout.size() - new_adjusted_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            let Ok(ptr) = (unsafe {
                self.inner
                    .shrink(ptr, old_adjusted_layout, new_adjusted_layout)
            }) else {
                GlobalStatBuffer::current().force_alloc(diff as i64);
                return Err(AllocError);
            };

            return Ok(Self::with_meta(ptr, new_layout, address));
        }

        let alloc_size = old_adjusted_layout.size() - new_adjusted_layout.size();
        let stat = ManuallyDrop::new(unsafe { Arc::from_raw(address as *const MemStat) });
        MemStatBuffer::current().dealloc(&stat, alloc_size as i64);

        let Ok(ptr) = (unsafe {
            self.inner
                .shrink(ptr, old_adjusted_layout, new_adjusted_layout)
        }) else {
            MemStatBuffer::current().force_alloc(&stat, alloc_size as i64);
            return Err(AllocError);
        };

        Ok(Self::with_meta(ptr, new_layout, address))
    }
}

unsafe impl<T: Allocator> Allocator for MetaTrackerAllocator<T> {
    #[inline(always)]
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.size() >= META_TRACKER_THRESHOLD {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return self.alloc(mem_stat, layout);
            }

            let adjusted_layout = Self::adjusted_layout(layout);

            GlobalStatBuffer::current().alloc(adjusted_layout.size() as i64)?;
            let Ok(allocated_ptr) = self.inner.allocate(adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(adjusted_layout.size() as i64);
                return Err(AllocError);
            };

            return Ok(Self::with_meta(allocated_ptr, layout, 0));
        }

        GlobalStatBuffer::current().alloc(layout.size() as i64)?;
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

            let adjusted_layout = Self::adjusted_layout(layout);
            GlobalStatBuffer::current().alloc(adjusted_layout.size() as i64)?;

            let Ok(allocated_ptr) = self.inner.allocate_zeroed(adjusted_layout) else {
                GlobalStatBuffer::current().dealloc(adjusted_layout.size() as i64);
                return Err(AllocError);
            };

            return Ok(Self::with_meta(allocated_ptr, layout, 0));
        }

        GlobalStatBuffer::current().alloc(layout.size() as i64)?;
        let Ok(allocated_ptr) = self.inner.allocate_zeroed(layout) else {
            GlobalStatBuffer::current().dealloc(layout.size() as i64);
            return Err(AllocError);
        };

        Ok(allocated_ptr)
    }

    #[inline(always)]
    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() >= META_TRACKER_THRESHOLD {
            if let Some(layout) = unsafe { self.dealloc(ptr, layout) } {
                GlobalStatBuffer::current().dealloc(layout.size() as i64);
                unsafe { self.inner.deallocate(ptr, layout) };
            }

            return;
        }

        GlobalStatBuffer::current().dealloc(layout.size() as i64);
        unsafe { self.inner.deallocate(ptr, layout) };
    }

    #[inline(always)]
    unsafe fn grow(
        &self,
        mut ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() == new_layout.size() {
            return Ok(unsafe {
                NonNull::new_unchecked(slice_from_raw_parts_mut(ptr.as_mut(), new_layout.size()))
            });
        }

        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            unsafe { self.grow_impl(ptr, old_layout, new_layout) }
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = new_layout.size() - old_layout.size();

            GlobalStatBuffer::current().alloc(diff as i64)?;
            let Ok(grow_ptr) = (unsafe { self.inner.grow(ptr, old_layout, new_layout) }) else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(grow_ptr)
        } else {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return unsafe { self.move_grow(ptr, mem_stat, old_layout, new_layout) };
            }

            let new_adjusted_layout = Self::adjusted_layout(new_layout);

            let diff = new_adjusted_layout.size() - old_layout.size();
            GlobalStatBuffer::current().alloc(diff as i64)?;

            let Ok(grow_ptr) = (unsafe { self.inner.grow(ptr, old_layout, new_adjusted_layout) })
            else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(Self::with_meta(grow_ptr, new_layout, 0))
        }
    }

    #[inline(always)]
    unsafe fn grow_zeroed(
        &self,
        mut ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() == new_layout.size() {
            return Ok(unsafe {
                NonNull::new_unchecked(slice_from_raw_parts_mut(ptr.as_mut(), new_layout.size()))
            });
        }

        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            unsafe { self.grow_zeroed_impl(ptr, old_layout, new_layout) }
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = new_layout.size() - old_layout.size();
            GlobalStatBuffer::current().alloc(diff as i64)?;
            let Ok(grow_ptr) = (unsafe { self.inner.grow_zeroed(ptr, old_layout, new_layout) })
            else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(grow_ptr)
        } else {
            if let Some(mem_stat) = ThreadTracker::mem_stat() {
                return unsafe { self.move_grow_zeroed(ptr, mem_stat, old_layout, new_layout) };
            }

            let new_adjusted_layout = Self::adjusted_layout(new_layout);

            let diff = new_adjusted_layout.size() - old_layout.size();
            GlobalStatBuffer::current().alloc(diff as i64)?;
            let Ok(grow_ptr) =
                (unsafe { self.inner.grow_zeroed(ptr, old_layout, new_adjusted_layout) })
            else {
                GlobalStatBuffer::current().dealloc(diff as i64);
                return Err(AllocError);
            };

            Ok(unsafe {
                NonNull::new_unchecked(slice_from_raw_parts_mut(
                    grow_ptr.as_non_null_ptr().as_mut(),
                    new_layout.size(),
                ))
            })
        }
    }

    #[inline(always)]
    unsafe fn shrink(
        &self,
        mut ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        if old_layout.size() == new_layout.size() {
            return Ok(unsafe {
                NonNull::new_unchecked(slice_from_raw_parts_mut(ptr.as_mut(), new_layout.size()))
            });
        }

        if old_layout.size() >= META_TRACKER_THRESHOLD
            && new_layout.size() >= META_TRACKER_THRESHOLD
        {
            unsafe { self.shrink_impl(ptr, old_layout, new_layout) }
        } else if old_layout.size() < META_TRACKER_THRESHOLD
            && new_layout.size() < META_TRACKER_THRESHOLD
        {
            let diff = old_layout.size() - new_layout.size();
            GlobalStatBuffer::current().dealloc(diff as i64);
            let Ok(shrink_ptr) = (unsafe { self.inner.shrink(ptr, old_layout, new_layout) }) else {
                GlobalStatBuffer::current().force_alloc(diff as i64);
                return Err(AllocError);
            };

            Ok(shrink_ptr)
        } else {
            unsafe { self.move_shrink(ptr, old_layout, new_layout) }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::AllocError;
    use std::alloc::Allocator;
    use std::alloc::Layout;
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::base::GlobalUniqName;
    use crate::mem_allocator::DefaultAllocator;
    use crate::mem_allocator::tracker::META_TRACKER_THRESHOLD;
    use crate::mem_allocator::tracker::MetaTrackerAllocator;
    use crate::runtime::GLOBAL_QUERIES_MANAGER;
    use crate::runtime::GlobalStatBuffer;
    use crate::runtime::MemStat;
    use crate::runtime::MemStatBuffer;
    use crate::runtime::ParentMemStat;
    use crate::runtime::Thread;
    use crate::runtime::ThreadTracker;

    fn with_mock_env<
        T: Allocator + Send + Sync + 'static,
        R,
        F: Fn(Arc<MemStat>, Arc<dyn Allocator + Send + Sync + 'static>) -> R,
    >(
        test: F,
        nest: T,
        global: Arc<MemStat>,
    ) -> R {
        {
            let mem_stat = MemStat::create_child(
                Some(GlobalUniqName::unique()),
                0,
                ParentMemStat::Normal(global.clone()),
            );
            let mut tracking_payload = ThreadTracker::new_tracking_payload();
            tracking_payload.mem_stat = Some(mem_stat.clone());

            let allocator = Arc::new(MetaTrackerAllocator::create(nest));
            let _guard = ThreadTracker::tracking(tracking_payload);
            let _mem_stat_guard = MemStatBuffer::mock(global.clone());
            let _global_stat_guard = GlobalStatBuffer::mock(global.clone());
            test(mem_stat, allocator)
        }
    }

    #[test]
    fn test_small_allocation() {
        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(256, 8).unwrap();

                let ptr = allocator.allocate(layout).unwrap();
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 256);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), layout) };
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_large_allocation_with_mem_stat() {
        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(512, 8).unwrap();
                let meta_size = std::mem::size_of::<usize>();

                let ptr = allocator.allocate(layout).unwrap();
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(
                    MemStatBuffer::current().memory_usage,
                    (512 + meta_size) as i64
                );
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), layout) };
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_cross_threshold_grow() {
        let test_function =
            |_mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let old_layout = Layout::from_size_align(256, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                assert_eq!(GlobalStatBuffer::current().memory_usage, 256);

                let new_layout = Layout::from_size_align(768, 8).unwrap();
                let new_ptr =
                    unsafe { allocator.grow(ptr.as_non_null_ptr(), old_layout, new_layout) }
                        .unwrap();

                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(
                    MemStatBuffer::current().memory_usage,
                    768 + std::mem::size_of::<usize>() as i64
                );

                unsafe { allocator.deallocate(new_ptr.as_non_null_ptr(), new_layout) };
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_allocation_rollback() {
        struct FailingAllocator;
        unsafe impl Allocator for FailingAllocator {
            fn allocate(&self, _: Layout) -> Result<NonNull<[u8]>, AllocError> {
                Err(AllocError)
            }
            unsafe fn deallocate(&self, _: NonNull<u8>, _: Layout) {}
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(1024, 8).unwrap();

                let result = allocator.allocate(layout);
                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            FailingAllocator,
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_shrink_memory() {
        let test_function =
            |_mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let old_layout = Layout::from_size_align(1024, 8).unwrap();
                let new_layout = Layout::from_size_align(256, 8).unwrap();

                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = MemStatBuffer::current().memory_usage;

                let new_ptr =
                    unsafe { allocator.shrink(ptr.as_non_null_ptr(), old_layout, new_layout) }
                        .unwrap();

                let expected_dealloc = (old_layout.size() + std::mem::size_of::<usize>()) as i64;
                assert_eq!(
                    MemStatBuffer::current().memory_usage,
                    initial_usage - expected_dealloc
                );
                assert_eq!(GlobalStatBuffer::current().memory_usage, 256);

                unsafe { allocator.deallocate(new_ptr.as_non_null_ptr(), new_layout) };
            };
        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_extreme_alignment() {
        let test_function =
            |_mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(512, 64).unwrap();
                let ptr = allocator.allocate(layout).unwrap();

                let addr = ptr.as_non_null_ptr().as_ptr() as usize;
                assert_eq!(addr % 64, 0);

                unsafe {
                    let meta_ptr = ptr.as_non_null_ptr().as_ptr().add(layout.size());
                    let stat_addr = meta_ptr.cast::<usize>().read_unaligned();
                    assert_ne!(stat_addr, 0);
                }

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), layout) };
            };

        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_allocate_zeroed_failure() {
        struct FailingAllocator;
        unsafe impl Allocator for FailingAllocator {
            fn allocate(&self, _layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                unreachable!()
            }

            fn allocate_zeroed(&self, _: Layout) -> Result<NonNull<[u8]>, AllocError> {
                Err(AllocError)
            }

            unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {
                unreachable!()
            }
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(1024, 8).unwrap();

                let result = allocator.allocate_zeroed(layout);
                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);

                let small_layout = Layout::from_size_align(256, 8).unwrap();
                let result = allocator.allocate_zeroed(small_layout);
                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            FailingAllocator,
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_grow_failure_rollback() {
        struct PartialFailingAllocator<T: Allocator>(T);
        unsafe impl<T: Allocator> Allocator for PartialFailingAllocator<T> {
            fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                self.0.allocate(layout)
            }

            unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                self.0.deallocate(ptr, layout)
            }

            unsafe fn grow(
                &self,
                _ptr: NonNull<u8>,
                _old_layout: Layout,
                _new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                Err(AllocError)
            }
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let old_layout = Layout::from_size_align(256, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_global = GlobalStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(498, 8).unwrap();
                let result =
                    unsafe { allocator.grow(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, initial_global);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(512, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = MemStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(1024, 8).unwrap();
                let result =
                    unsafe { allocator.grow(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(MemStatBuffer::current().memory_usage, initial_usage);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(256, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_global = GlobalStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(768, 8).unwrap();
                let result =
                    unsafe { allocator.grow(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, initial_global);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };
            };

        with_mock_env(
            test_function,
            PartialFailingAllocator(DefaultAllocator::default()),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_grow_zeroed_failure() {
        struct GrowZeroedFailingAllocator<T: Allocator>(T);

        unsafe impl<T: Allocator> Allocator for GrowZeroedFailingAllocator<T> {
            fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                self.0.allocate(layout)
            }

            unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                self.0.deallocate(ptr, layout)
            }

            unsafe fn grow_zeroed(
                &self,
                _ptr: NonNull<u8>,
                _old_layout: Layout,
                _new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                Err(AllocError)
            }
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let old_layout = Layout::from_size_align(256, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_global = GlobalStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(498, 8).unwrap();
                let result =
                    unsafe { allocator.grow_zeroed(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, initial_global);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(512, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = MemStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(1024, 8).unwrap();
                let result =
                    unsafe { allocator.grow_zeroed(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(MemStatBuffer::current().memory_usage, initial_usage);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(256, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_global = GlobalStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(768, 8).unwrap();
                let result =
                    unsafe { allocator.grow_zeroed(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, initial_global);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };
            };

        with_mock_env(
            test_function,
            GrowZeroedFailingAllocator(DefaultAllocator::default()),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_shrink_failure() {
        struct ShrinkFailingAllocator<T: Allocator>(T);
        unsafe impl<T: Allocator> Allocator for ShrinkFailingAllocator<T> {
            fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                self.0.allocate(layout)
            }

            unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                self.0.deallocate(ptr, layout)
            }

            unsafe fn shrink(
                &self,
                _ptr: NonNull<u8>,
                _old_layout: Layout,
                _new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                Err(AllocError)
            }
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let old_layout = Layout::from_size_align(498, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = GlobalStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(256, 8).unwrap();
                let result =
                    unsafe { allocator.shrink(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, initial_usage);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(1024, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = MemStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(512, 8).unwrap();
                let result =
                    unsafe { allocator.shrink(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(MemStatBuffer::current().memory_usage, initial_usage);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };

                let old_layout = Layout::from_size_align(512, 8).unwrap();
                let ptr = allocator.allocate(old_layout).unwrap();
                let initial_usage = MemStatBuffer::current().memory_usage;

                let new_layout = Layout::from_size_align(256, 8).unwrap();
                let result =
                    unsafe { allocator.shrink(ptr.as_non_null_ptr(), old_layout, new_layout) };

                assert!(result.is_err());
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(MemStatBuffer::current().memory_usage, initial_usage);

                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), old_layout) };
            };

        with_mock_env(
            test_function,
            ShrinkFailingAllocator(DefaultAllocator::default()),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_mixed_failure_scenarios() {
        struct ChaosAllocator<T: Allocator> {
            inner: T,
            failure_rate: f64,
        }

        unsafe impl<T: Allocator> Allocator for ChaosAllocator<T> {
            fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                if rand::random::<f64>() < self.failure_rate {
                    Err(AllocError)
                } else {
                    self.inner.allocate(layout)
                }
            }

            fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
                if rand::random::<f64>() < self.failure_rate {
                    Err(AllocError)
                } else {
                    self.inner.allocate_zeroed(layout)
                }
            }

            unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                self.inner.deallocate(ptr, layout)
            }

            unsafe fn grow(
                &self,
                ptr: NonNull<u8>,
                old_layout: Layout,
                new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                if rand::random::<f64>() < self.failure_rate {
                    Err(AllocError)
                } else {
                    self.inner.grow(ptr, old_layout, new_layout)
                }
            }

            unsafe fn grow_zeroed(
                &self,
                ptr: NonNull<u8>,
                old_layout: Layout,
                new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                if rand::random::<f64>() < self.failure_rate {
                    Err(AllocError)
                } else {
                    self.inner.grow_zeroed(ptr, old_layout, new_layout)
                }
            }

            unsafe fn shrink(
                &self,
                ptr: NonNull<u8>,
                old_layout: Layout,
                new_layout: Layout,
            ) -> Result<NonNull<[u8]>, AllocError> {
                if rand::random::<f64>() < self.failure_rate {
                    Err(AllocError)
                } else {
                    self.inner.shrink(ptr, old_layout, new_layout)
                }
            }
        }

        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let mut allocations = vec![];

                for _ in 0..1000000 {
                    match rand::random::<u8>() % 6 {
                        // allocate
                        0 => {
                            let size = rand::random::<usize>() % 2048 + 1;
                            let layout = Layout::from_size_align(size, 8).unwrap();

                            if let Ok(ptr) = allocator.allocate(layout) {
                                allocations.push((ptr, layout));
                            }
                        }
                        // allocate_zero
                        1 => {
                            let size = rand::random::<usize>() % 2048 + 1;
                            let layout = Layout::from_size_align(size, 8).unwrap();

                            if let Ok(ptr) = allocator.allocate_zeroed(layout) {
                                allocations.push((ptr, layout));
                            }
                        }
                        // deallocate
                        2 => {
                            if !allocations.is_empty() {
                                let index = rand::random::<usize>() % allocations.len();
                                let (ptr, layout) = allocations.remove(index);
                                unsafe { allocator.deallocate(ptr.as_non_null_ptr(), layout) };
                            }
                        }
                        // grow
                        3 => {
                            if !allocations.is_empty() {
                                let index = rand::random::<usize>() % allocations.len();
                                let (ptr, old_layout) = allocations[index];
                                let new_size =
                                    old_layout.size() + rand::random::<usize>() % 256 + 1;
                                let new_layout = Layout::from_size_align(new_size, 8).unwrap();

                                if let Ok(new_ptr) = unsafe {
                                    allocator.grow(ptr.as_non_null_ptr(), old_layout, new_layout)
                                } {
                                    allocations[index] = (new_ptr, new_layout);
                                }
                            }
                        }
                        // grow_zero
                        4 => {
                            if !allocations.is_empty() {
                                let index = rand::random::<usize>() % allocations.len();
                                let (ptr, old_layout) = allocations[index];
                                let new_size =
                                    old_layout.size() + rand::random::<usize>() % 256 + 1;
                                let new_layout = Layout::from_size_align(new_size, 8).unwrap();

                                if let Ok(new_ptr) = unsafe {
                                    allocator.grow_zeroed(
                                        ptr.as_non_null_ptr(),
                                        old_layout,
                                        new_layout,
                                    )
                                } {
                                    allocations[index] = (new_ptr, new_layout);
                                }
                            }
                        }
                        // shrink
                        _ => {
                            if !allocations.is_empty() {
                                let index = rand::random::<usize>() % allocations.len();
                                let (ptr, old_layout) = allocations[index];
                                let new_size = old_layout
                                    .size()
                                    .saturating_sub(rand::random::<usize>() % 256);
                                let new_layout =
                                    Layout::from_size_align(std::cmp::max(1, new_size), 8).unwrap();

                                if let Ok(new_ptr) = unsafe {
                                    allocator.shrink(ptr.as_non_null_ptr(), old_layout, new_layout)
                                } {
                                    allocations[index] = (new_ptr, new_layout);
                                }
                            }
                        }
                    }
                }

                let actual_usage =
                    mem_stat.used.load(Ordering::Relaxed) + MemStatBuffer::current().memory_usage;

                let mem_stat_expected_usage = allocations
                    .iter()
                    .filter(|(_, r)| r.size() >= META_TRACKER_THRESHOLD)
                    .map(|(_, r)| r.size() as i64 + std::mem::size_of::<usize>() as i64)
                    .sum::<i64>();

                assert_eq!(actual_usage, mem_stat_expected_usage);

                let small_usage = allocations
                    .iter()
                    .filter(|(_, r)| r.size() < META_TRACKER_THRESHOLD)
                    .map(|(_, r)| r.size() as i64)
                    .sum::<i64>();

                assert_eq!(
                    small_usage + actual_usage,
                    MemStatBuffer::current().memory_usage
                        + GlobalStatBuffer::current().memory_usage
                        + GlobalStatBuffer::current()
                            .global_mem_stat
                            .used
                            .load(Ordering::Relaxed)
                );

                for (ptr, layout) in allocations {
                    unsafe { allocator.deallocate(ptr.as_non_null_ptr(), layout) };
                }

                MemStatBuffer::current().flush::<false>(0).unwrap();
                GlobalStatBuffer::current().flush::<false>(0).unwrap();

                assert_eq!(0, mem_stat.used.load(Ordering::Relaxed));
                assert_eq!(0, MemStatBuffer::current().memory_usage);
                assert_eq!(0, GlobalStatBuffer::current().memory_usage);
                assert_eq!(
                    0,
                    GlobalStatBuffer::current()
                        .global_mem_stat
                        .used
                        .load(Ordering::Relaxed)
                );
            };

        with_mock_env(
            test_function,
            ChaosAllocator {
                inner: DefaultAllocator::default(),
                failure_rate: 0.3,
            },
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_out_of_order_deallocation() {
        let test_function =
            |mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layouts = [
                    Layout::from_size_align(256, 8).unwrap(),
                    Layout::from_size_align(1024, 8).unwrap(),
                    Layout::from_size_align(384, 8).unwrap(),
                ];

                let pointers: Vec<_> = layouts
                    .iter()
                    .map(|&l| allocator.allocate(l).unwrap())
                    .collect();

                unsafe {
                    allocator.deallocate(pointers[1].as_non_null_ptr(), layouts[1]);
                    allocator.deallocate(pointers[0].as_non_null_ptr(), layouts[0]);
                }

                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 384);

                unsafe { allocator.deallocate(pointers[2].as_non_null_ptr(), layouts[2]) };
                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
            };

        with_mock_env(
            test_function,
            DefaultAllocator::default(),
            Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER)),
        );
    }

    #[test]
    fn test_dynamic_memstat_switch() {
        let global = Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER));
        let test_function = {
            let global = global.clone();
            move |mem_stat: Arc<MemStat>, _allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                let layout = Layout::from_size_align(512, 8).unwrap();
                let test_function = {
                    let mem_stat = mem_stat.clone();

                    move |new_mem_stat: Arc<MemStat>, allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                        let layout = Layout::from_size_align(512, 8).unwrap();
                        let ptr = allocator.allocate(layout).unwrap();

                        assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                        assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                        assert_eq!(new_mem_stat.used.load(Ordering::Relaxed), 0);
                        assert_eq!(
                            MemStatBuffer::current().memory_usage,
                            (512 + std::mem::size_of::<usize>()) as i64
                        );

                        (ptr, new_mem_stat, allocator)
                    }
                };

                let (ptr, new_mem_stat, allocator) =
                    with_mock_env(test_function, DefaultAllocator::default(), global.clone());

                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(
                    new_mem_stat.used.load(Ordering::Relaxed),
                    (512 + std::mem::size_of::<usize>()) as i64
                );
                assert_eq!(MemStatBuffer::current().memory_usage, 0);

                unsafe {
                    allocator.deallocate(ptr.as_non_null_ptr(), layout);
                }

                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(
                    new_mem_stat.used.load(Ordering::Relaxed),
                    (512 + std::mem::size_of::<usize>()) as i64
                );
                assert_eq!(
                    MemStatBuffer::current().memory_usage,
                    -((512 + std::mem::size_of::<usize>()) as i64)
                );

                let _ = MemStatBuffer::current().flush::<false>(0);

                assert_eq!(MemStatBuffer::current().memory_usage, 0);
                assert_eq!(GlobalStatBuffer::current().memory_usage, 0);
                assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                assert_eq!(new_mem_stat.used.load(Ordering::Relaxed), 0);
            }
        };

        with_mock_env(test_function, DefaultAllocator::default(), global);
    }

    #[test]
    fn test_thread_local_stat_isolation() {
        let global_mem_stat = Arc::new(MemStat::global(&GLOBAL_QUERIES_MANAGER));

        let test_function = {
            let global_mem_stat = global_mem_stat.clone();

            move |_mem_stat: Arc<MemStat>,
                  _allocator: Arc<dyn Allocator + Send + Sync + 'static>| {
                const THREADS: usize = 8;
                let mut handles = vec![];

                for i in 0..THREADS {
                    let global_stat = global_mem_stat.clone();
                    handles.push(Thread::spawn(move || {
                        let test_function = |mem_stat: Arc<MemStat>,
                                             allocator: Arc<
                            dyn Allocator + Send + Sync + 'static,
                        >| {
                            let layout = Layout::from_size_align(512 * (i + 1), 8).unwrap();
                            let ptr = allocator.allocate(layout).unwrap();

                            assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                            assert_eq!(
                                MemStatBuffer::current().memory_usage,
                                (512 * (i + 1) + std::mem::size_of::<usize>()) as i64
                            );
                            (
                                ptr.as_non_null_ptr().as_ptr() as usize,
                                mem_stat,
                                layout,
                                allocator,
                            )
                        };

                        with_mock_env(
                            test_function,
                            DefaultAllocator::default(),
                            global_stat.clone(),
                        )
                    }));
                }

                let mut memory_usage = 0;
                let mut handle_res = vec![];
                for (idx, handle) in handles.into_iter().enumerate() {
                    let (ptr, mem_stat, layout, allocator) = handle.join().expect("Thread panic");

                    memory_usage += layout.size() as i64 + std::mem::size_of::<usize>() as i64;
                    assert_eq!(
                        mem_stat.used.load(Ordering::Relaxed),
                        (512 * (idx + 1) + std::mem::size_of::<usize>()) as i64
                    );
                    handle_res.push((ptr, mem_stat, layout, allocator));
                }

                assert_eq!(global_mem_stat.used.load(Ordering::Relaxed), memory_usage);
                for (ptr, mem_stat, layout, allocator) in handle_res.into_iter() {
                    unsafe { allocator.deallocate(NonNull::new_unchecked(ptr as *mut u8), layout) };
                    let _ = MemStatBuffer::current().flush::<false>(0);
                    assert_eq!(mem_stat.used.load(Ordering::Relaxed), 0);
                    assert_eq!(MemStatBuffer::current().memory_usage, 0);
                }

                let _ = GlobalStatBuffer::current().flush::<false>(0);
                assert_eq!(global_mem_stat.used.load(Ordering::Relaxed), 0);
            }
        };

        with_mock_env(test_function, DefaultAllocator::default(), global_mem_stat);
    }
}
