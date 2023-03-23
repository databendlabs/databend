// Copyright 2023 Datafuse Labs.
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

// The codes from https://github.com/eto-ai/lance/tree/main/rust/src/utils/distance
// Lance crate relies on arrow-rs. However, Databend uses arrow2, the type is not fully compatible.

#[cfg(any(target_arch = "aarch64"))]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn normalize_neon(vector: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let buf = [0.0_f32; 4];
    let mut sum = vld1q_f32(buf.as_ptr());
    let n = vector.len();
    for i in (0..n).step_by(4) {
        let x = vld1q_f32(vector.as_ptr().add(i));
        sum = vfmaq_f32(sum, x, x);
    }
    vaddvq_f32(sum).sqrt()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "fma")]
#[inline]
unsafe fn normalize_fma(vector: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let mut sums = _mm256_setzero_ps();
    for i in (0..vector.len()).step_by(8) {
        // Cache line-aligned
        let x = _mm256_load_ps(vector.as_ptr().add(i));
        sums = _mm256_fmadd_ps(x, x, sums);
    }
    add_fma(sums).sqrt()
}

/// Normalize a vector.
///
/// The parameters must be cache line aligned. For example, from
/// Arrow Arrays, i.e., Float32Array
#[inline]
pub fn normalize(vector: &[f32]) -> f32 {
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    unsafe {
        normalize_neon(vector)
    }

    #[cfg(target_arch = "x86_64")]
    {
        unsafe { normalize_fma(vector) }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    vector.iter().map(|v| v * v).sum::<f32>().sqrt()
}

#[cfg(any(target_arch = "x86_64"))]
#[target_feature(enable = "fma")]
#[inline]
pub unsafe fn add_fma(x: std::arch::x86_64::__m256) -> f32 {
    use std::arch::x86_64::*;

    let mut sums = x;
    let mut shift = _mm256_permute2f128_ps(sums, sums, 1);
    // [x0+x4, x1+x5, ..]
    sums = _mm256_add_ps(sums, shift);
    shift = _mm256_permute_ps(sums, 14);
    sums = _mm256_add_ps(sums, shift);
    sums = _mm256_hadd_ps(sums, sums);
    let mut results: [f32; 8] = [0f32; 8];
    _mm256_storeu_ps(results.as_mut_ptr(), sums);
    results[0]
}
