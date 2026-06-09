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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_vector::vector_norm;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::meta::VectorDistanceType;

use crate::DistanceType;

pub fn vector_distance(
    left: &[f32],
    right: &[f32],
    distance_type: VectorDistanceType,
) -> Result<f32> {
    match distance_type {
        VectorDistanceType::L1 => vector_l1_distance(left, right),
        VectorDistanceType::L2 => Ok(vector_l2_squared_distance(left, right)?.sqrt()),
        VectorDistanceType::Dot => {
            check_vector_len(left, right)?;
            let kernel = VectorDistanceKernel::new(left.len());
            let left_norm = kernel.dot(left, left).sqrt();
            let right_norm = kernel.dot(right, right).sqrt();
            let distance = if left_norm <= f32::EPSILON || right_norm <= f32::EPSILON {
                1.0
            } else {
                1.0 - kernel.dot(left, right) / (left_norm * right_norm)
            };
            if distance.is_finite() {
                Ok(distance)
            } else {
                Ok(1.0)
            }
        }
    }
}

pub fn vector_spheres_overlap(
    left: &VectorColumnStatistics,
    right: &VectorColumnStatistics,
    distance_type: VectorDistanceType,
) -> Result<bool> {
    let left_centroid = vector_centroid_values(left);
    let right_centroid = vector_centroid_values(right);
    let distance = vector_stat_distance(&left_centroid, &right_centroid, distance_type)?;

    Ok(distance <= left.radius.0 + right.radius.0)
}

pub fn vector_distance_lower_bound(
    query: &[f32],
    stats: &VectorColumnStatistics,
    distance_type: VectorDistanceType,
) -> Result<f32> {
    let centroid = vector_centroid_values(stats);
    let distance = vector_stat_distance(query, &centroid, distance_type)?;
    let lower_bound = (distance - stats.radius.0).max(0.0);
    if matches!(distance_type, VectorDistanceType::Dot) {
        return Ok(1.0 - lower_bound.cos());
    }

    Ok(lower_bound)
}

pub fn vector_stat_distance(
    left: &[f32],
    right: &[f32],
    distance_type: VectorDistanceType,
) -> Result<f32> {
    if matches!(distance_type, VectorDistanceType::Dot) {
        return vector_angular_distance(left, right);
    }

    vector_distance(left, right, distance_type)
}

pub fn vector_angular_distance(left: &[f32], right: &[f32]) -> Result<f32> {
    // Cosine distance (1 - cos(theta)) is not a metric, so it cannot be used
    // directly with the triangle inequality for safe block pruning.  Convert it
    // to angular distance theta, which is a metric on normalized vectors.
    let distance = vector_distance(left, right, VectorDistanceType::Dot)?;
    if !distance.is_finite() {
        return Ok(std::f32::consts::FRAC_PI_2);
    }

    Ok((1.0 - distance).clamp(-1.0, 1.0).acos())
}

pub fn preprocess_vector(values: &[F32], distance_type: DistanceType) -> Vec<f32> {
    let mut vector = values.iter().map(|value| value.0).collect::<Vec<_>>();
    if matches!(distance_type, DistanceType::Dot) {
        normalize_vector(&mut vector);
    }
    vector
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

pub fn vector_statistics_distance_type(distance_type: DistanceType) -> VectorDistanceType {
    match distance_type {
        DistanceType::L1 => VectorDistanceType::L1,
        DistanceType::L2 => VectorDistanceType::L2,
        DistanceType::Dot => VectorDistanceType::Dot,
    }
}

pub fn vector_distance_type_from_index_option(distance_type: &str) -> Option<VectorDistanceType> {
    match distance_type.trim() {
        "cosine" | "dot" => Some(VectorDistanceType::Dot),
        "l1" => Some(VectorDistanceType::L1),
        "l2" => Some(VectorDistanceType::L2),
        _ => None,
    }
}

pub fn vector_distance_type_name(distance_type: VectorDistanceType) -> &'static str {
    match distance_type {
        VectorDistanceType::L1 => "l1",
        VectorDistanceType::L2 => "l2",
        VectorDistanceType::Dot => "dot",
    }
}

fn vector_centroid_values(stats: &VectorColumnStatistics) -> Vec<f32> {
    stats.centroid.iter().map(|value| value.0).collect()
}

pub fn vector_l2_squared_distance(left: &[f32], right: &[f32]) -> Result<f32> {
    check_vector_len(left, right)?;
    Ok(VectorDistanceKernel::new(left.len()).l2_squared(left, right))
}

pub fn vector_l1_distance(left: &[f32], right: &[f32]) -> Result<f32> {
    check_vector_len(left, right)?;
    Ok(VectorDistanceKernel::new(left.len()).l1(left, right))
}

fn check_vector_len(left: &[f32], right: &[f32]) -> Result<()> {
    if left.len() != right.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            left.len(),
            right.len(),
        )));
    }
    Ok(())
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
