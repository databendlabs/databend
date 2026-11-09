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

// The scalar similarities and the SIMD dispatch (`Metric::similarity`) of qdrant
// `lib/segment/src/spaces/simple.rs` (v1.20.0).

#[cfg(target_arch = "x86_64")]
use super::simple_avx::*;
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
use super::simple_neon::*;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use super::simple_sse::*;
use crate::DistanceType;
use crate::hnsw_index::common::types::ScoreType;

#[cfg(target_arch = "x86_64")]
const MIN_DIM_SIZE_AVX: usize = 32;

#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_feature = "neon")
))]
const MIN_DIM_SIZE_SIMD: usize = 16;

/// The similarity of two vectors (higher is closer), computed with the SIMD
/// kernel the CPU supports.
pub fn similarity(distance_type: DistanceType, v1: &[f32], v2: &[f32]) -> ScoreType {
    // The SIMD kernels read `v1.len()` values of both vectors.
    assert_eq!(v1.len(), v2.len());
    match distance_type {
        DistanceType::Dot => dot_similarity_simd(v1, v2),
        DistanceType::L1 => manhattan_similarity_simd(v1, v2),
        DistanceType::L2 => euclid_similarity_simd(v1, v2),
    }
}

fn euclid_similarity_simd(v1: &[f32], v2: &[f32]) -> ScoreType {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_SIZE_AVX
        {
            return unsafe { euclid_similarity_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { euclid_similarity_sse(v1, v2) };
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { euclid_similarity_neon(v1, v2) };
        }
    }

    euclid_similarity(v1, v2)
}

fn manhattan_similarity_simd(v1: &[f32], v2: &[f32]) -> ScoreType {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_SIZE_AVX
        {
            return unsafe { manhattan_similarity_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { manhattan_similarity_sse(v1, v2) };
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { manhattan_similarity_neon(v1, v2) };
        }
    }

    manhattan_similarity(v1, v2)
}

fn dot_similarity_simd(v1: &[f32], v2: &[f32]) -> ScoreType {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx")
            && is_x86_feature_detected!("fma")
            && v1.len() >= MIN_DIM_SIZE_AVX
        {
            return unsafe { dot_similarity_avx(v1, v2) };
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { dot_similarity_sse(v1, v2) };
        }
    }

    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && v1.len() >= MIN_DIM_SIZE_SIMD {
            return unsafe { dot_similarity_neon(v1, v2) };
        }
    }

    dot_similarity(v1, v2)
}

pub(crate) fn euclid_similarity(v1: &[f32], v2: &[f32]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).powi(2))
        .sum::<ScoreType>()
}

pub(crate) fn manhattan_similarity(v1: &[f32], v2: &[f32]) -> ScoreType {
    -v1.iter()
        .zip(v2)
        .map(|(a, b)| (a - b).abs())
        .sum::<ScoreType>()
}

pub(crate) fn dot_similarity(v1: &[f32], v2: &[f32]) -> ScoreType {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_similarity_matches_scalar() {
        // Around the SIMD widths and the remainder loops, and the 768 of Cohere.
        for dim in [0, 1, 15, 16, 17, 31, 32, 33, 63, 768, 1000] {
            let v1: Vec<f32> = (0..dim)
                .map(|i| ((i * 7 % 13) as f32 - 6.0) / 7.3)
                .collect();
            let v2: Vec<f32> = (0..dim)
                .map(|i| ((i * 5 % 11) as f32 - 5.0) / 3.1)
                .collect();
            for distance_type in [DistanceType::Dot, DistanceType::L1, DistanceType::L2] {
                let expected = match distance_type {
                    DistanceType::Dot => dot_similarity(&v1, &v2),
                    DistanceType::L1 => manhattan_similarity(&v1, &v2),
                    DistanceType::L2 => euclid_similarity(&v1, &v2),
                };
                let actual = similarity(distance_type, &v1, &v2);
                // Only the order of the additions differs.
                let tol = 1e-5 * expected.abs().max(1.0);
                assert!(
                    (actual - expected).abs() <= tol,
                    "dim {dim}: {actual} vs {expected}"
                );
            }
        }
    }
}
