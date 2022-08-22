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

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::alloc::Layout;

use super::Allocator;

#[derive(Debug, Clone, Copy, Default)]
pub struct StdAllocator;

unsafe impl Allocator for StdAllocator {
    unsafe fn allocx(&mut self, layout: Layout, clear_mem: bool) -> *mut u8 {
        if clear_mem {
            std::alloc::alloc_zeroed(layout)
        } else {
            std::alloc::alloc(layout)
        }
    }

    unsafe fn deallocx(&mut self, ptr: *mut u8, layout: Layout) {
        std::alloc::dealloc(ptr, layout)
    }

    unsafe fn reallocx(
        &mut self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
        clear_mem: bool,
    ) -> *mut u8 {
        let ptr = std::alloc::realloc(ptr, layout, new_size);
        if clear_mem && new_size > layout.size() {
            std::ptr::write_bytes(
                ptr.offset(layout.size() as isize),
                0,
                new_size - layout.size(),
            );
        }
        ptr
    }
}

#[cfg(test)]
mod test {
    use std::alloc::Layout;

    use crate::mem_allocator::Allocator;
    use crate::mem_allocator::StdAllocator;

    #[test]
    fn test_malloc() {
        type T = i64;
        let mut alloc = StdAllocator::default();

        let align = std::mem::align_of::<T>();
        let size = std::mem::size_of::<T>() * 100;
        let new_size = std::mem::size_of::<T>() * 1000000;

        unsafe {
            let layout = Layout::from_size_align_unchecked(size, align);
            let ptr = alloc.allocx(layout, true) as *mut T;
            *ptr = 84;
            assert_eq!(84, *ptr);
            assert_eq!(0, *(ptr.offset(5)));

            *(ptr.offset(5)) = 1000;

            let new_ptr = alloc.reallocx(ptr as *mut u8, layout, new_size, true) as *mut T;
            assert_eq!(84, *new_ptr);
            assert_eq!(0, *(new_ptr.offset(4)));
            assert_eq!(1000, *(new_ptr.offset(5)));

            alloc.deallocx(new_ptr as *mut u8, layout)
        }
    }
}
