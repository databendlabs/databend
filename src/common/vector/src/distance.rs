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

// Borrow from https://github.com/eto-ai/lance/tree/main/rust/src/utils/distance
// Lance crate which relies on arrow-rs. However, Databend uses arrow2, and the type is not fully compatible.

use common_exception::ErrorCode;
use common_exception::Result;

/// Fallback Cosine Distance function.
fn cosine_dist(from: &[f32], to: &[f32], dimension: usize) -> Vec<f32> {
    let n = to.len() / dimension;

    let distances: Vec<f32> = (0..n)
        .map(|idx| {
            let vector = &to[idx * dimension..(idx + 1) * dimension];
            let mut x_sq = 0_f32;
            let mut y_sq = 0_f32;
            let mut xy = 0_f32;
            from.iter().zip(vector.iter()).for_each(|(x, y)| {
                xy += x * y;
                x_sq += x.powi(2);
                y_sq += y.powi(2);
            });
            1.0 - xy / (x_sq.sqrt() * y_sq.sqrt())
        })
        .collect();
    distances
}

#[cfg(any(target_arch = "aarch64"))]
#[target_feature(enable = "neon")]
#[inline]
unsafe fn cosine_dist_neon(x: &[f32], y: &[f32], x_norm: f32) -> f32 {
    use std::arch::aarch64::*;
    let len = x.len();
    let buf = [0.0_f32; 4];
    let mut xy = vld1q_f32(buf.as_ptr());
    let mut y_sq = xy;
    for i in (0..len).step_by(4) {
        let left = vld1q_f32(x.as_ptr().add(i));
        let right = vld1q_f32(y.as_ptr().add(i));
        xy = vfmaq_f32(xy, left, right);
        y_sq = vfmaq_f32(y_sq, right, right);
    }
    1.0 - vaddvq_f32(xy) / (x_norm * vaddvq_f32(y_sq).sqrt())
}

#[cfg(any(target_arch = "x86_64"))]
#[target_feature(enable = "fma")]
#[inline]
unsafe fn cosine_dist_fma(x_vector: &[f32], y_vector: &[f32], x_norm: f32) -> f32 {
    use std::arch::x86_64::*;

    use super::compute::add_fma;

    let len = x_vector.len();
    let mut xy = _mm256_setzero_ps();
    let mut y_sq = _mm256_setzero_ps();
    for i in (0..len).step_by(8) {
        let x = _mm256_load_ps(x_vector.as_ptr().add(i));
        let y = _mm256_load_ps(y_vector.as_ptr().add(i));
        xy = _mm256_fmadd_ps(x, y, xy);
        y_sq = _mm256_fmadd_ps(y, y, y_sq);
    }
    1.0 - add_fma(xy) / (x_norm * add_fma(y_sq).sqrt())
}

#[inline]
fn cosine_dist_simd(from: &[f32], to: &[f32], dimension: usize) -> Vec<f32> {
    let x = from;
    let to_values = to;
    let x_norm = super::compute::normalize(x);
    let n = to.len() / dimension;
    let mut builder: Vec<f32> = Vec::with_capacity(n);
    for y in to_values.chunks_exact(dimension) {
        #[cfg(any(target_arch = "aarch64"))]
        {
            builder.push(unsafe { cosine_dist_neon(x, y, x_norm) });
        }
        #[cfg(any(target_arch = "x86_64"))]
        {
            builder.push(unsafe { cosine_dist_fma(x, y, x_norm) });
        }
    }
    builder
}

/// Cosine Distance
///
/// <https://en.wikipedia.org/wiki/Cosine_similarity>
pub fn cosine_distance(from: &[f32], to: &[f32], dimension: usize) -> Result<Vec<f32>> {
    if from.len() != to.len() || from.len() != dimension {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector len:{:} not equal dimension:{:}",
            from.len(),
            dimension,
        )));
    }

    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::is_aarch64_feature_detected;
        if is_aarch64_feature_detected!("neon") && from.len() % 4 == 0 {
            return Ok(cosine_dist_simd(from, to, dimension));
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("fma") && from.len() % 8 == 0 {
            return Ok(cosine_dist_simd(from, to, dimension));
        }
    }

    // Fallback
    Ok(cosine_dist(from, to, dimension))
}
