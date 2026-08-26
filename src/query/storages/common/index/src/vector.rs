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

use databend_common_exception::Result;
use databend_common_vector::angular_distance;
use databend_common_vector::l1_distance;
use databend_common_vector::l2_distance;
use databend_common_vector::vector_norm;
use databend_storages_common_table_meta::meta::VectorDistanceType;

pub fn vector_stat_distance(
    left: &[f32],
    right: &[f32],
    distance_type: VectorDistanceType,
) -> Result<f32> {
    match distance_type {
        VectorDistanceType::L1 => l1_distance(left, right),
        VectorDistanceType::L2 => l2_distance(left, right),
        VectorDistanceType::Dot => angular_distance(left, right),
    }
}

pub fn normalize_vector(vector: &mut [f32]) {
    let norm = vector_norm(vector);
    if norm <= f32::EPSILON {
        return;
    }
    for value in vector {
        *value /= norm;
    }
}

#[derive(Clone, Copy)]
pub struct VectorDistanceKernel {
    implementation: VectorDistanceKernelImpl,
    dim: usize,
}

#[derive(Clone, Copy)]
enum VectorDistanceKernelImpl {
    Scalar,
    #[cfg(target_arch = "x86_64")]
    Avx,
    #[cfg(target_arch = "x86_64")]
    Sse,
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    Neon,
}

impl VectorDistanceKernel {
    pub fn new(dim: usize) -> Self {
        let implementation = detect_vector_distance_kernel_impl();
        Self {
            implementation,
            dim,
        }
    }

    #[inline]
    pub fn dot(&self, left: &[f32], right: &[f32]) -> f32 {
        debug_assert_eq!(left.len(), self.dim);
        debug_assert_eq!(right.len(), self.dim);
        match self.implementation {
            VectorDistanceKernelImpl::Scalar => scalar_dot(left, right),
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Avx => unsafe { impl_f32_dot_avx(left, right) },
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Sse => unsafe { impl_f32_dot_sse(left, right) },
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            VectorDistanceKernelImpl::Neon => unsafe { impl_f32_dot_neon(left, right) },
        }
    }

    #[inline]
    pub fn l2_squared(&self, left: &[f32], right: &[f32]) -> f32 {
        debug_assert_eq!(left.len(), self.dim);
        debug_assert_eq!(right.len(), self.dim);
        match self.implementation {
            VectorDistanceKernelImpl::Scalar => scalar_l2_squared(left, right),
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Avx => unsafe { impl_f32_l2_sqr_avx(left, right) },
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Sse => unsafe { impl_f32_l2_sqr_sse(left, right) },
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            VectorDistanceKernelImpl::Neon => unsafe { impl_f32_l2_sqr_neon(left, right) },
        }
    }

    #[inline]
    pub fn l1(&self, left: &[f32], right: &[f32]) -> f32 {
        debug_assert_eq!(left.len(), self.dim);
        debug_assert_eq!(right.len(), self.dim);
        match self.implementation {
            VectorDistanceKernelImpl::Scalar => scalar_l1(left, right),
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Avx => unsafe { impl_f32_l1_avx(left, right) },
            #[cfg(target_arch = "x86_64")]
            VectorDistanceKernelImpl::Sse => unsafe { impl_f32_l1_sse(left, right) },
            #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
            VectorDistanceKernelImpl::Neon => unsafe { impl_f32_l1_neon(left, right) },
        }
    }
}

fn detect_vector_distance_kernel_impl() -> VectorDistanceKernelImpl {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return VectorDistanceKernelImpl::Avx;
        }
        if is_x86_feature_detected!("sse4.1") {
            return VectorDistanceKernelImpl::Sse;
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            return VectorDistanceKernelImpl::Neon;
        }
    }

    VectorDistanceKernelImpl::Scalar
}

fn scalar_dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| *left * *right)
        .sum()
}

fn scalar_l2_squared(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| {
            let diff = *left - *right;
            diff * diff
        })
        .sum()
}

fn scalar_l1(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| (*left - *right).abs())
        .sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn impl_f32_dot_avx(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 8);
    let mut sum = _mm256_setzero_ps();
    for idx in (0..m).step_by(8) {
        let left_values = unsafe { _mm256_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm256_loadu_ps(right.as_ptr().add(idx)) };
        sum = _mm256_fmadd_ps(left_values, right_values, sum);
    }

    let mut values = [0.0_f32; 8];
    unsafe { _mm256_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| *left * *right)
            .sum::<f32>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn impl_f32_l2_sqr_avx(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 8);
    let mut sum = _mm256_setzero_ps();
    for idx in (0..m).step_by(8) {
        let left_values = unsafe { _mm256_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm256_loadu_ps(right.as_ptr().add(idx)) };
        let diff = _mm256_sub_ps(left_values, right_values);
        sum = _mm256_fmadd_ps(diff, diff, sum);
    }

    let mut values = [0.0_f32; 8];
    unsafe { _mm256_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| {
                let diff = *left - *right;
                diff * diff
            })
            .sum::<f32>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "fma")]
unsafe fn impl_f32_l1_avx(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 8);
    let mut sum = _mm256_setzero_ps();
    let sign_mask = _mm256_set1_ps(-0.0);
    for idx in (0..m).step_by(8) {
        let left_values = unsafe { _mm256_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm256_loadu_ps(right.as_ptr().add(idx)) };
        let diff = _mm256_sub_ps(left_values, right_values);
        let abs_diff = _mm256_andnot_ps(sign_mask, diff);
        sum = _mm256_add_ps(sum, abs_diff);
    }

    let mut values = [0.0_f32; 8];
    unsafe { _mm256_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| (*left - *right).abs())
            .sum::<f32>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn impl_f32_dot_sse(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = _mm_setzero_ps();
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { _mm_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm_loadu_ps(right.as_ptr().add(idx)) };
        sum = _mm_add_ps(sum, _mm_mul_ps(left_values, right_values));
    }

    let mut values = [0.0_f32; 4];
    unsafe { _mm_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| *left * *right)
            .sum::<f32>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn impl_f32_l2_sqr_sse(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = _mm_setzero_ps();
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { _mm_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm_loadu_ps(right.as_ptr().add(idx)) };
        let diff = _mm_sub_ps(left_values, right_values);
        sum = _mm_add_ps(sum, _mm_mul_ps(diff, diff));
    }

    let mut values = [0.0_f32; 4];
    unsafe { _mm_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| {
                let diff = *left - *right;
                diff * diff
            })
            .sum::<f32>()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn impl_f32_l1_sse(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::x86_64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = _mm_setzero_ps();
    let sign_mask = _mm_set1_ps(-0.0);
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { _mm_loadu_ps(left.as_ptr().add(idx)) };
        let right_values = unsafe { _mm_loadu_ps(right.as_ptr().add(idx)) };
        let diff = _mm_sub_ps(left_values, right_values);
        let abs_diff = _mm_andnot_ps(sign_mask, diff);
        sum = _mm_add_ps(sum, abs_diff);
    }

    let mut values = [0.0_f32; 4];
    unsafe { _mm_storeu_ps(values.as_mut_ptr(), sum) };
    values.iter().sum::<f32>()
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| (*left - *right).abs())
            .sum::<f32>()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon")]
unsafe fn impl_f32_dot_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = vdupq_n_f32(0.0);
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { vld1q_f32(left.as_ptr().add(idx)) };
        let right_values = unsafe { vld1q_f32(right.as_ptr().add(idx)) };
        sum = vmlaq_f32(sum, left_values, right_values);
    }

    vaddvq_f32(sum)
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| *left * *right)
            .sum::<f32>()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon")]
unsafe fn impl_f32_l2_sqr_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = vdupq_n_f32(0.0);
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { vld1q_f32(left.as_ptr().add(idx)) };
        let right_values = unsafe { vld1q_f32(right.as_ptr().add(idx)) };
        let diff = vsubq_f32(left_values, right_values);
        sum = vmlaq_f32(sum, diff, diff);
    }

    vaddvq_f32(sum)
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| {
                let diff = *left - *right;
                diff * diff
            })
            .sum::<f32>()
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon")]
unsafe fn impl_f32_l1_neon(left: &[f32], right: &[f32]) -> f32 {
    use std::arch::aarch64::*;

    let m = left.len() - (left.len() % 4);
    let mut sum = vdupq_n_f32(0.0);
    for idx in (0..m).step_by(4) {
        let left_values = unsafe { vld1q_f32(left.as_ptr().add(idx)) };
        let right_values = unsafe { vld1q_f32(right.as_ptr().add(idx)) };
        let abs_diff = vabsq_f32(vsubq_f32(left_values, right_values));
        sum = vaddq_f32(sum, abs_diff);
    }

    vaddvq_f32(sum)
        + left[m..]
            .iter()
            .zip(right[m..].iter())
            .map(|(left, right)| (*left - *right).abs())
            .sum::<f32>()
}
