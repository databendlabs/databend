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

//! Software prefetch of the vectors a search is about to score.
//!
//! The vectors of a block do not fit in the cache, and a search reads them in
//! graph order, so each distance starts with a cache miss the hardware
//! prefetcher cannot predict. Prefetching the next vector while the current one
//! is scored overlaps that miss with the arithmetic.

const CACHE_LINE: usize = 64;

/// Hints the CPU to load every cache line of `data` into the cache.
#[inline(always)]
pub fn prefetch_slice<T>(data: &[T]) {
    let start = data.as_ptr() as *const u8;
    let len = std::mem::size_of_val(data);
    let mut offset = 0;
    while offset < len {
        // SAFETY: `start + offset` stays inside `data`.
        prefetch_line(unsafe { start.add(offset) });
        offset += CACHE_LINE;
    }
}

#[inline(always)]
fn prefetch_line(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch is a hint; it does not fault and has no visible effect.
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(ptr as *const i8);
    }
    #[cfg(target_arch = "aarch64")]
    // SAFETY: a prefetch is a hint; it does not fault and has no visible effect.
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}
