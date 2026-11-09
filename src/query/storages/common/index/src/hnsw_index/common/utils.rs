// Copyright Qdrant
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

pub fn check_process_stopped(stopped: &AtomicBool) -> Result<()> {
    if stopped.load(Ordering::Relaxed) {
        return Err(ErrorCode::Internal("check process stopped error"));
    }
    Ok(())
}

/// Cache line size assumed for prefetching, in bytes.
const CACHE_LINE: usize = 64;

/// Bytes hinted per vector, and the lead of the near (L1) prefetch window: 16
/// hinted lines train the hardware stream prefetcher, which covers the rest of
/// larger vectors (measured by Qdrant, see `common/src/prefetch.rs`).
const NEAR_BYTES: usize = 1024;

/// Lead of the far (L2) prefetch window, in bytes, covering the memory latency
/// at the rate the scoring kernels read vectors.
const FAR_BYTES: usize = 4096;

/// Largest batch scored without prefetch: hints fire too late to help.
pub const MAX_UNPREFETCHED_BATCH: usize = 2;

/// Smallest vector storage that benefits from prefetch: smaller ones stay in
/// the private L2 of a core, and hints only add overhead.
pub const MIN_PREFETCH_STORAGE_BYTES: usize = 1024 * 1024;

/// The near (L1) and far (L2) prefetch windows, in vectors of
/// `vector_size_bytes`, ahead of the vector being scored. The far window is 0
/// for vectors smaller than a cache line.
#[inline]
pub fn prefetch_windows(vector_size_bytes: usize) -> (usize, usize) {
    let near = (NEAR_BYTES / vector_size_bytes.max(1)).clamp(1, 8);
    if vector_size_bytes < CACHE_LINE {
        return (near, 0);
    }
    let far = (FAR_BYTES / vector_size_bytes).clamp(near + 2, 16);
    (near, far)
}

/// Whether the ids of a batch are nearly consecutive, so that the hardware
/// prefetcher already streams their vectors.
pub fn is_dense_batch(ids: &[u32]) -> bool {
    if ids.len() < 2 {
        return false;
    }
    let (min, max) = ids
        .iter()
        .fold((u32::MAX, 0), |(min, max), &id| (min.min(id), max.max(id)));
    ((max - min) as usize) < ids.len() * 2
}

/// Hints the CPU to load up to the first [`NEAR_BYTES`] of `data` into all
/// cache levels, ahead of reading it.
#[inline]
pub fn prefetch_bytes(data: &[u8]) {
    for_each_line(data, prefetch_read);
}

/// Like [`prefetch_bytes`], but no deeper than L2, so that lines needed later do
/// not displace L1 data still being read.
#[inline]
pub fn prefetch_bytes_l2(data: &[u8]) {
    for_each_line(data, prefetch_read_l2);
}

/// Calls `f` on the start of every cache line of up to the first [`NEAR_BYTES`]
/// of `data`. Data that does not start on a cache line also covers the line of
/// its last byte.
#[inline(always)]
fn for_each_line(data: &[u8], f: impl Fn(*const u8)) {
    let len = data.len().min(NEAR_BYTES);
    let Some(last) = len.checked_sub(1) else {
        return;
    };
    let ptr = data.as_ptr();
    for offset in (0..len).step_by(CACHE_LINE).chain([last]) {
        f(unsafe { ptr.add(offset) });
    }
}

/// `prefetcht0`, or `prfm pldl1keep`: a hint that never faults.
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::_MM_HINT_T0;
        use std::arch::x86_64::_mm_prefetch;
        _mm_prefetch::<_MM_HINT_T0>(ptr.cast::<i8>());
    }
    // No stable prefetch intrinsic exists for aarch64.
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// `prefetcht1`, or `prfm pldl2keep`: a hint that never faults.
#[inline(always)]
fn prefetch_read_l2(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::_MM_HINT_T1;
        use std::arch::x86_64::_mm_prefetch;
        _mm_prefetch::<_MM_HINT_T1>(ptr.cast::<i8>());
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl2keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_windows() {
        // 772-byte vectors (768 dimensions): 1 ahead into L1, 5 into L2.
        assert_eq!(prefetch_windows(772), (1, 5));
        assert_eq!(prefetch_windows(36), (8, 0));
        assert_eq!(prefetch_windows(4096), (1, 3));
        assert_eq!(prefetch_windows(0), (8, 0));

        assert!(!is_dense_batch(&[]));
        assert!(!is_dense_batch(&[7]));
        assert!(is_dense_batch(&[3, 1, 2, 5]));
        assert!(!is_dense_batch(&[1, 900, 37, 4000]));

        // Empty, short, unaligned and long slices.
        let data = vec![0u8; 3 * NEAR_BYTES];
        for range in [0..0, 0..1, 3..70, 0..data.len()] {
            prefetch_bytes(&data[range.clone()]);
            prefetch_bytes_l2(&data[range]);
        }
    }
}
