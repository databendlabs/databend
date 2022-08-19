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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::cell::Cell;
use std::os::raw::c_int;
use std::os::raw::c_long;
use std::os::raw::c_void;
use std::ptr;

use libc::memset;
use libc::off_t;
use libc::size_t;

use crate::base::ThreadTracker;

/// Implementation of std::alloc::GlobalAlloc whose backend is mmap(2)
#[derive(Debug, Clone, Copy)]
pub struct StackfulAllocator<const ALIGN: bool, const INIT_BYTES: usize, Allocator: GlobalAlloc> {
    allocator: Allocator,
}

impl<const ALIGN: bool, const INIT_BYTES: usize, Allocator: GlobalAlloc + Default>
    StackfulAllocator<ALIGN, INIT_BYTES, Allocator>
{
    /// Creates a new instance.
    #[inline]
    pub fn new() -> Self {
        Self {
            allocator: Allocator::default(),
        }
    }
}

impl<const ALIGN: bool, const INIT_BYTES: usize, Allocator: GlobalAlloc>
    StackfulAllocator<ALIGN, INIT_BYTES, Allocator>
{
    #[inline]
    unsafe fn alloc_inner(&self, layout: Layout) -> *mut u8 {
        #[cfg(not(target_os = "linux"))]
        let flags: c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        #[cfg(target_os = "linux")]
        let flags: c_int =
         libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | if INIT_BYTES { libc::MAP_POPULATE } else { 0 };

        const ADDR: *mut c_void = ptr::null_mut::<c_void>();
        const PROT: c_int = libc::PROT_READ | libc::PROT_WRITE;
        const FD: c_int = -1; // Should be -1 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
        const OFFSET: off_t = 0; // Should be 0 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
        let length = layout.size() as size_t;

        println!("layout -> {:?} {:?}", layout,  page_size());
        if layout.align() > page_size() {
            if length >= MMAP_THRESHOLD {
                return self.allocator.alloc(layout);
            }
            ThreadTracker::alloc_memory(layout.size() as i64);
            match mmap(ADDR, length, PROT, flags, FD, OFFSET) {
                libc::MAP_FAILED => ptr::null_mut::<u8>(),
                ret => {
                    let ptr = ret as usize;
                    debug_assert_eq!(0, ptr % layout.align());
                    ret as *mut u8
                }
            }
        } else {
            if layout.align() <= MALLOC_MIN_ALIGNMENT {
                if ALIGN {
                    self.allocator.alloc_zeroed(layout)
                } else {
                    self.allocator.alloc(layout)
                }
            } else {
                ThreadTracker::alloc_memory(layout.size() as i64);
                let buf = ptr::null_mut::<u8>();
                posix_memalign(buf as *mut c_void, layout.align(), layout.size());
                if ALIGN {
                    memset(buf as *mut c_void, 0, layout.size());
                }
                buf
            }
        }
    }

    #[inline]
    unsafe fn dealloc_inner(&self, ptr: *mut u8, layout: Layout) {
        let addr = ptr as *mut c_void;
        if layout.align() > page_size() {
            if layout.size() >= MMAP_THRESHOLD {
                return self.allocator.dealloc(ptr, layout);
            }
            munmap(addr, layout.size());
        } else {
            munmap(addr, layout.size());
        }
    }
}


/// # Portability
///
/// alloc() calls mmap() with flag MAP_ANONYMOUS.
/// Many systems support the flag, however, it is not specified in POSIX.
///
/// # Safety
///
/// All functions are thread safe.
///
/// # Error
///
/// Each function don't cause panic but set OS errno on error.
///
/// Note that it is not an error to deallocate pointer which is not allocated.
/// This is the spec of munmap(2). See `man 2 munmap` for details.
unsafe impl<const ALIGN: bool, const INIT_BYTES: usize, Allocator: GlobalAlloc> GlobalAlloc
    for StackfulAllocator<ALIGN, INIT_BYTES, Allocator>
{
    /// # Panics
    ///
    /// This method may panic if the align of `layout` is greater than the kernel page align.
    /// (Basically, kernel page align is always greater than the align of `layout` that rust
    /// generates unless the programer dares to build such a `layout` on purpose.)
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.alloc_inner(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.dealloc_inner(ptr, layout)
    }

    /// # Panics
    ///
    /// This method can panic if the align of `layout` is greater than the kernel page align.
    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // alloc() calls mmap() with the flags which always fills the memory 0.
        self.alloc(layout)
    }

    #[cfg(target_os = "linux")]
    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        use libc::PROT_READ;
        use libc::PROT_WRITE;

        if new_size == layout.size() {
            return ptr;
        }

        if layout.size() < MMAP_THRESHOLD
            && new_size < MMAP_THRESHOLD
            && layout.align() <= MALLOC_MIN_ALIGNMENT
        {
            let new_buf = self.allocator.realloc(ptr, layout, new_size);
            if ALIGN && new_size > layout.size() {
                memset(new_buf as *mut c_void, 0, new_size);
            }
            new_buf
        } else if layout.size() >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD {
            mremap(ptr as *mut c_void, layout.size(), new_size, PROT_READ | PROT_WRITE) as *mut u8
        } else {
            let new_buf = self.alloc_inner(Layout::from_size_align_unchecked(new_size, layout.align()));
            std::ptr::copy_nonoverlapping(ptr, new_buf, new_size.min(layout.size()));
            self.dealloc(ptr, layout);
            new_buf
        }
    }
}

extern "C" {
    fn mmap(
        addr: *mut c_void,
        length: size_t,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: off_t,
    ) -> *mut c_void;

    fn munmap(addr: *mut c_void, length: size_t);

    fn posix_memalign(memptr: *mut c_void, alignment: size_t, size: size_t) -> c_int;

    #[cfg(target_os = "linux")]
    fn mremap(
        old_address: *mut c_void,
        old_size: size_t,
        new_size: size_t,
        flags: c_int,
    ) -> *mut c_void;
}

#[cfg(test)]
mod tests {
    use std::alloc::GlobalAlloc;
    use std::mem;
    use std::ptr;

    use super::*;
    use crate::mem_allocator::Allocator;
    type ALOC = StackfulAllocator<false, true, Allocator>;

    fn clear_errno() {
        unsafe { *libc::__errno_location() = 0 }
    }

    // fn errno() -> i32 {
    //     unsafe { *libc::__errno_location() }
    // }

    #[test]
    fn default() {
        let _alloc = ALOC::new();
    }

    #[test]
    fn allocate() {
        unsafe {
            type T = i64;
            let alloc = ALOC::new();

            let layout = Layout::new::<i64>();
            let ptr = alloc.alloc(layout) as *mut T;
            assert_ne!(std::ptr::null(), ptr);

            *ptr = 84;
            assert_eq!(84, *ptr);

            *ptr = *ptr * -2;
            assert_eq!(-168, *ptr);

            alloc.dealloc(ptr as *mut u8, layout)
        }
    }

    #[test]
    fn allocate_too_large() {
        unsafe {
            clear_errno();

            type T = String;
            let alloc = ALOC::new();

            let align = mem::align_of::<T>();
            let size = std::usize::MAX - mem::size_of::<T>();
            let layout = Layout::from_size_align_unchecked(size, align);

            assert_eq!(ptr::null(), alloc.alloc(layout));
        }
    }

    #[test]
    fn alloc_zeroed() {
        unsafe {
            type T = [u8; 1025];
            let alloc = ALOC::new();

            let layout = Layout::new::<T>();
            let ptr = alloc.alloc_zeroed(layout) as *mut T;
            let s: &[u8] = &*ptr;

            for u in s {
                assert_eq!(0, *u);
            }

            alloc.dealloc(ptr as *mut u8, layout);
        }
    }

    #[test]
    fn realloc() {
        unsafe {
            type T = [u8; 1025];
            let alloc = ALOC::new();

            let layout = Layout::new::<T>();
            let ptr = alloc.alloc(layout) as *mut T;

            let ts = &mut *ptr;
            for t in ts.iter_mut() {
                *t = 1;
            }

            type U = (T, T);

            let new_size = mem::size_of::<U>();
            let ptr = alloc.realloc(ptr as *mut u8, layout, new_size) as *mut T;
            let layout = Layout::from_size_align(new_size, layout.align()).unwrap();

            let ts = &mut *ptr;
            for t in ts.iter_mut() {
                assert_eq!(1, *t);
                *t = 2;
            }

            let new_size = mem::size_of::<u8>();
            let ptr = alloc.realloc(ptr as *mut u8, layout, new_size);
            let layout = Layout::from_size_align(new_size, layout.align()).unwrap();

            assert_eq!(2, *ptr);

            alloc.dealloc(ptr, layout);
        }
    }
}

thread_local! {
    static PAGE_SIZE: Cell<usize> = Cell::new(0);
}

/// Returns OS Page Size.
///
/// See crate document for details.
#[inline]
pub fn page_size() -> usize {
    PAGE_SIZE.with(|s| match s.get() {
        0 => {
            let ret = unsafe { sysconf(libc::_SC_PAGE_SIZE) as usize };
            s.set(ret);
            ret
        }
        ret => ret,
    })
}

extern "C" {
    fn sysconf(name: c_int) -> c_long;
}
