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
use std::cell::Cell;
use std::os::raw::c_int;
use std::os::raw::c_long;
use std::os::raw::c_void;
use std::ptr;

use libc::off_t;
use libc::size_t;

use super::JEAllocator;
use crate::base::ThreadTracker;
use crate::mem_allocator::Allocator as AllocatorTrait;

const MMAP_THRESHOLD: size_t = 64 * (1usize << 20);
const MALLOC_MIN_ALIGNMENT: size_t = 8;

/// Implementation of std::alloc::AllocatorTrait whose backend is mmap(2)
#[derive(Debug, Clone, Copy, Default)]
pub struct MmapAllocator<const MMAP_POPULATE: bool> {
    allocator: JEAllocator,
}

/// # Portability
///
/// allocx() calls mmap() with flag MAP_ANONYMOUS.
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
unsafe impl<const MMAP_POPULATE: bool> AllocatorTrait for MmapAllocator<MMAP_POPULATE> {
    /// # Panics
    ///
    /// This method may panic if the align of `layout` is greater than the kernel page align.
    /// (Basically, kernel page align is always greater than the align of `layout` that rust
    /// generates unless the programer dares to build such a `layout` on purpose.)
    #[inline]
    unsafe fn allocx(&mut self, layout: Layout, clear_mem: bool) -> *mut u8 {
        #[cfg(not(target_os = "linux"))]
        let flags: c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
        #[cfg(target_os = "linux")]
        let flags: c_int = libc::MAP_PRIVATE
            | libc::MAP_ANONYMOUS
            | if MMAP_POPULATE { libc::MAP_POPULATE } else { 0 };

        const ADDR: *mut c_void = ptr::null_mut::<c_void>();
        const PROT: c_int = libc::PROT_READ | libc::PROT_WRITE;
        const FD: c_int = -1; // Should be -1 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
        const OFFSET: off_t = 0; // Should be 0 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
        let length = layout.size() as size_t;

        if layout.size() >= MMAP_THRESHOLD {
            if layout.align() > page_size() {
                // too large alignment, fallback to use allocator
                return self.allocator.allocx(layout, clear_mem);
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
            self.allocator.allocx(layout, clear_mem)
        }
    }

    #[inline]
    unsafe fn deallocx(&mut self, ptr: *mut u8, layout: Layout) {
        let addr = ptr as *mut c_void;
        if layout.size() >= MMAP_THRESHOLD {
            munmap(addr, layout.size());
        } else {
            self.allocator.deallocx(ptr, layout);
        }
    }

    #[inline]
    unsafe fn reallocx(
        &mut self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
        clear_mem: bool,
    ) -> *mut u8 {
        use libc::PROT_READ;
        use libc::PROT_WRITE;

        if new_size == layout.size() {
            return ptr;
        }

        if layout.size() < MMAP_THRESHOLD
            && new_size < MMAP_THRESHOLD
            && layout.align() <= MALLOC_MIN_ALIGNMENT
        {
            self.allocator.reallocx(ptr, layout, new_size, clear_mem)
        } else if layout.size() >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD {
            if layout.align() > page_size() {
                self.allocator.reallocx(ptr, layout, new_size, clear_mem)
            } else {
                mremapx(
                    ptr as *mut c_void,
                    layout.size(),
                    new_size,
                    0,
                    PROT_READ | PROT_WRITE,
                ) as *mut u8
            }
        } else {
            let new_buf = self.allocx(
                Layout::from_size_align_unchecked(new_size, layout.align()),
                clear_mem,
            );
            std::ptr::copy_nonoverlapping(ptr, new_buf, new_size.min(layout.size()));
            self.deallocx(ptr, layout);
            new_buf
        }
    }
}

#[inline]
unsafe fn mremapx(
    old_address: *mut c_void,
    old_size: size_t,
    new_size: size_t,
    flags: c_int,
    mmap_prot: c_int,
) -> *mut c_void {
    #[cfg(not(target_os = "linux"))]
    {
        const ADDR: *mut c_void = ptr::null_mut::<c_void>();
        const FD: c_int = -1; // Should be -1 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
        const OFFSET: off_t = 0; // Should be 0 if flags includes MAP_ANONYMOUS. See `man 2 mmap`

        let new_address = mmap(ADDR, new_size, mmap_prot, flags, FD, OFFSET);
        std::ptr::copy_nonoverlapping(
            old_address as *const u8,
            new_address as *mut u8,
            old_size as usize,
        );
        new_address
    }
    #[cfg(target_os = "linux")]
    mremap(
        old_address,
        old_size,
        new_size,
        flags | libc::MREMAP_MAYMOVE,
        mmap_prot,
    )
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

    #[cfg(target_os = "linux")]
    fn mremap(
        old_address: *mut c_void,
        old_size: size_t,
        new_size: size_t,
        flags: c_int,
        mmap_prot: c_int,
    ) -> *mut c_void;
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::ptr;

    use super::*;
    use crate::mem_allocator::Allocator as AllocatorTrait;
    type Aloc = MmapAllocator<true>;

    fn clear_errno() {
        #[cfg(target_os = "linux")]
        unsafe {
            *libc::__errno_location() = 0
        }
    }

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
    fn allocate_too_large() {
        unsafe {
            clear_errno();

            type T = String;
            let mut alloc = Aloc::default();

            let align = mem::align_of::<T>();
            let size = std::usize::MAX - mem::size_of::<T>();
            let layout = Layout::from_size_align_unchecked(size, align);

            assert_eq!(ptr::null(), alloc.allocx(layout, false));
        }
    }

    #[test]
    fn alloc_zeroed() {
        unsafe {
            type T = [u8; 1025];
            let mut alloc = Aloc::default();

            let layout = Layout::new::<T>();
            let ptr = alloc.allocx(layout, false) as *mut T;
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
