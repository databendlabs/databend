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

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::alloc::Layout;
use std::ptr;

use super::Allocator as AllocatorTrait;

/// Implementation of std::alloc::Allocator whose backend is mmap(2)
#[derive(Debug, Clone, Copy)]
pub struct StackfulAllocator<const INIT_BYTES: usize, Allocator: AllocatorTrait> {
    allocator: Allocator,
    stack_bytes: [u8; INIT_BYTES],
}

impl<const INIT_BYTES: usize, Allocator: AllocatorTrait + Default> Default
    for StackfulAllocator<INIT_BYTES, Allocator>
{
    fn default() -> Self {
        Self {
            allocator: Default::default(),
            stack_bytes: [0; INIT_BYTES],
        }
    }
}

/// # Safety
///
/// We should guarantee that the stack is only used as inner memory, such as HashSet
unsafe impl<const INIT_BYTES: usize, Allocator: AllocatorTrait> AllocatorTrait
    for StackfulAllocator<INIT_BYTES, Allocator>
{
    unsafe fn allocx(&mut self, layout: Layout, clear_mem: bool) -> *mut u8 {
        if layout.size() <= INIT_BYTES {
            if clear_mem {
                ptr::write_bytes(self.stack_bytes.as_mut_ptr(), 0, INIT_BYTES);
            }
            return self.stack_bytes.as_mut_ptr();
        }
        self.allocator.allocx(layout, clear_mem)
    }

    unsafe fn deallocx(&mut self, ptr: *mut u8, layout: Layout) {
        if layout.size() > INIT_BYTES {
            self.allocator.deallocx(ptr, layout);
        }
    }

    unsafe fn reallocx(
        &mut self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
        clear_mem: bool,
    ) -> *mut u8 {
        if new_size <= INIT_BYTES {
            return ptr;
        }
        if layout.size() > INIT_BYTES {
            return self.allocator.reallocx(ptr, layout, new_size, clear_mem);
        }

        let new_buf = self.allocator.allocx(
            Layout::from_size_align_unchecked(new_size, layout.align()),
            clear_mem,
        );
        std::ptr::copy_nonoverlapping(ptr, new_buf, layout.size());
        new_buf
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;
    use crate::mem_allocator::Allocator as AllocatorTrait;
    use crate::mem_allocator::MmapAllocator;
    type Aloc = StackfulAllocator<1024, MmapAllocator<true>>;

    #[test]
    fn default() {
        let _alloc = Aloc::default();
    }

    #[test]
    fn allocate() {
        unsafe {
            type T = i64;
            let mut alloc = Aloc::default();

            let layout = Layout::new::<i64>();
            let ptr = alloc.allocx(layout, false) as *mut T;
            assert_ne!(std::ptr::null(), ptr);

            *ptr = 84;
            assert_eq!(84, *ptr);

            *ptr *= -2;
            assert_eq!(-168, *ptr);

            alloc.deallocx(ptr as *mut u8, layout)
        }
    }

    #[test]
    fn alloc_zeroed() {
        unsafe {
            type T = [u8; 1025];
            let mut alloc = Aloc::default();

            let layout = Layout::new::<T>();
            let ptr = alloc.allocx(layout, true) as *mut T;
            let s: &[u8] = &*ptr;

            for u in s {
                assert_eq!(0, *u);
            }

            alloc.deallocx(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn reallocx() {
        unsafe {
            type T = [u8; 1025];
            let mut alloc = Aloc::default();

            let layout = Layout::new::<T>();
            let ptr = alloc.allocx(layout, false) as *mut T;

            let ts = &mut *ptr;
            for t in ts.iter_mut() {
                *t = 1;
            }

            type U = (T, T);

            let new_size = mem::size_of::<U>();
            let ptr = alloc.reallocx(ptr as *mut u8, layout, new_size, false) as *mut T;
            let layout = Layout::from_size_align(new_size, layout.align()).unwrap();

            let ts = &mut *ptr;
            for t in ts.iter_mut() {
                assert_eq!(1, *t);
                *t = 2;
            }

            let new_size = mem::size_of::<u8>();
            let ptr = alloc.reallocx(ptr as *mut u8, layout, new_size, false);
            let layout = Layout::from_size_align(new_size, layout.align()).unwrap();

            assert_eq!(2, *ptr);

            alloc.deallocx(ptr, layout);
        }
    }
}
