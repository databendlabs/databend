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

//! Graph search reads vectors and links at random offsets of buffers that are
//! gigabytes in total, so with 4 KiB pages nearly every hop misses the TLB.
//! Linux backs a range with transparent huge pages when it is advised with
//! `MADV_HUGEPAGE` before it is first touched, also under the common
//! `transparent_hugepage=madvise` mode.

use std::alloc::Layout;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::ptr::NonNull;

const HUGE_PAGE: usize = 2 * 1024 * 1024;

/// Read-only bytes, backed by transparent huge pages when they are large.
pub struct HugePageBytes {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: it owns its allocation, which is only read after `from_slice`.
unsafe impl Send for HugePageBytes {}
unsafe impl Sync for HugePageBytes {}

impl HugePageBytes {
    /// A copy of `data`.
    pub fn from_slice(data: &[u8]) -> Self {
        let layout = Self::layout(data.len());
        if layout.size() == 0 {
            return Self {
                ptr: NonNull::dangling(),
                len: 0,
            };
        }
        // SAFETY: the layout is not zero-sized; the allocation is checked, and
        // is of at least `data.len()` bytes that `data` cannot overlap.
        unsafe {
            let Some(ptr) = NonNull::new(std::alloc::alloc(layout)) else {
                std::alloc::handle_alloc_error(layout)
            };
            advise_huge_pages(ptr, data.len(), true);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
            Self {
                ptr,
                len: data.len(),
            }
        }
    }

    /// The size of the allocation.
    pub fn capacity(&self) -> usize {
        self.len
    }

    /// Large buffers are aligned to huge pages, so that all their whole huge
    /// pages can be advised: an unaligned range loses up to one at each end.
    fn layout(len: usize) -> Layout {
        let align = if is_large(len) { HUGE_PAGE } else { 1 };
        Layout::from_size_align(len, align).expect("the length of a slice is a valid layout")
    }
}

impl Drop for HugePageBytes {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        // SAFETY: the allocation of `from_slice`, with its layout.
        unsafe {
            // The allocator reuses the range: other allocations must not
            // inherit the advice.
            advise_huge_pages(self.ptr, self.len, false);
            std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len));
        }
    }
}

impl Deref for HugePageBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `len` initialized bytes, or dangling for 0.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Debug for HugePageBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HugePageBytes")
            .field("len", &self.len)
            .finish()
    }
}

/// Smaller buffers gain little, and each advised range costs the process two
/// memory mappings, which Linux limits (`vm.max_map_count`).
fn is_large(len: usize) -> bool {
    len >= 2 * HUGE_PAGE
}

/// Advises the whole huge pages of a large allocation aligned to huge pages
/// for or against transparent huge pages.
///
/// # Safety
///
/// `ptr` is an allocation of `len` bytes whose content is not needed, made
/// with `HugePageBytes::layout(len)`.
#[cfg(target_os = "linux")]
unsafe fn advise_huge_pages(ptr: NonNull<u8>, len: usize, enable: bool) {
    if !is_large(len) {
        return;
    }
    let addr = ptr.as_ptr() as *mut libc::c_void;
    let len = len / HUGE_PAGE * HUGE_PAGE;
    // The advice is best effort: a failure leaves ordinary pages.
    unsafe {
        if enable {
            // Memory that the allocator reuses is already mapped with small
            // pages, which are not replaced when written: unmap them first.
            libc::madvise(addr, len, libc::MADV_DONTNEED);
            libc::madvise(addr, len, libc::MADV_HUGEPAGE);
        } else {
            libc::madvise(addr, len, libc::MADV_NOHUGEPAGE);
        }
    }
}

#[cfg(not(target_os = "linux"))]
unsafe fn advise_huge_pages(_ptr: NonNull<u8>, _len: usize, _enable: bool) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_huge_page_bytes_copies() {
        for len in [0, 1, HUGE_PAGE, 2 * HUGE_PAGE, 3 * HUGE_PAGE + 17] {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let bytes = HugePageBytes::from_slice(&data);
            assert_eq!(&*bytes, data.as_slice(), "len {len}");
            assert_eq!(bytes.capacity(), len);
            let aligned = bytes.as_ptr() as usize % HUGE_PAGE == 0;
            assert!(aligned || !is_large(len), "len {len}");
        }
    }

    /// The bytes are backed by huge pages, also when the allocator hands out
    /// memory that was used before, as it does after the first index is evicted.
    #[cfg(target_os = "linux")]
    #[test]
    fn test_huge_page_bytes_are_huge() {
        let enabled = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled");
        if !enabled.is_ok_and(|modes| modes.contains("[always]") || modes.contains("[madvise]")) {
            return;
        }
        let huge_kb = |bytes: &HugePageBytes| {
            let (start, end) = (
                bytes.as_ptr() as usize,
                bytes.as_ptr() as usize + bytes.len(),
            );
            let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
            let mut inside = false;
            let mut kb = 0;
            for line in smaps.lines() {
                let mut fields = line.split_whitespace();
                let first = fields.next().unwrap_or_default();
                if let Some((from, to)) = first.split_once('-')
                    && let (Ok(from), Ok(to)) = (
                        usize::from_str_radix(from, 16),
                        usize::from_str_radix(to, 16),
                    )
                {
                    inside = from < end && to > start;
                } else if inside && first == "AnonHugePages:" {
                    kb += fields.next().unwrap().parse::<usize>().unwrap();
                }
            }
            kb
        };
        let data = vec![7_u8; 8 * HUGE_PAGE + 4096];
        for round in 0..3 {
            let bytes = HugePageBytes::from_slice(&data);
            // Under fragmentation or pressure, the kernel may back only some or
            // none of the pages with huge pages.
            let kb = huge_kb(&bytes);
            assert!(kb == 0 || kb >= HUGE_PAGE / 1024, "round {round}: {kb} kB");
            if kb == 0 {
                return;
            }
        }
    }
}
