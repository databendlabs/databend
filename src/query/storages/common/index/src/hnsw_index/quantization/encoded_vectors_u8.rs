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
use serde::Deserialize;
use serde::Serialize;

use crate::hnsw_index::common::hugepage::HugePageBytes;
use crate::hnsw_index::common::prefetch::MIN_PREFETCH_STORAGE_BYTES;
use crate::hnsw_index::common::prefetch::prefetch_slice;
use crate::hnsw_index::common::prefetch::prefetch_slice_l2;
use crate::hnsw_index::common::prefetch::prefetch_windows;
use crate::hnsw_index::quantization::encoded_storage::EncodedStorage;
use crate::hnsw_index::quantization::encoded_storage::EncodedStorageBuilder;
use crate::hnsw_index::quantization::encoded_vectors::DistanceType;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;
use crate::hnsw_index::quantization::encoded_vectors::VectorParameters;
use crate::hnsw_index::quantization::encoded_vectors::validate_vector_parameters;
use crate::hnsw_index::quantization::quantile::find_min_max_from_iter;
use crate::hnsw_index::quantization::quantile::find_quantile_interval;

pub const ALIGNMENT: usize = 16;

pub struct EncodedVectorsU8<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
    /// The SIMD kernel of `score_point_simple`, see `select_score_kernel`.
    score_kernel: Option<ScoreKernel>,
}

/// A SIMD kernel scoring an encoded query against an encoded vector.
type ScoreKernel = unsafe extern "C" fn(*const u8, *const u8, u32) -> f32;

pub struct EncodedQueryU8 {
    offset: f32,
    encoded_query: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    actual_dim: usize,
    alpha: f32,
    offset: f32,
    multiplier: f32,
    vector_parameters: VectorParameters,
}

impl EncodedVectorsU8<HugePageBytes> {
    pub(crate) fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.encoded_vectors.capacity()
    }
}

impl<TStorage: EncodedStorage> EncodedVectorsU8<TStorage> {
    pub fn encode<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
        mut storage_builder: impl EncodedStorageBuilder<TStorage>,
        vector_parameters: &VectorParameters,
        quantile: Option<f32>,
        stopped: &AtomicBool,
    ) -> Result<Self> {
        let actual_dim = Self::get_actual_dim(vector_parameters);

        if vector_parameters.count == 0 {
            return Ok(EncodedVectorsU8 {
                encoded_vectors: storage_builder.build(),
                score_kernel: select_score_kernel(vector_parameters.distance_type, actual_dim),
                metadata: Metadata {
                    actual_dim,
                    alpha: 0.0,
                    offset: 0.0,
                    multiplier: 0.0,
                    vector_parameters: vector_parameters.clone(),
                },
            });
        }

        debug_assert!(validate_vector_parameters(orig_data.clone(), vector_parameters).is_ok());
        let (alpha, offset) = Self::find_alpha_offset_size_dim(orig_data.clone());
        let (alpha, offset) = if let Some(quantile) = quantile {
            if let Some((min, max)) = find_quantile_interval(
                orig_data.clone(),
                vector_parameters.dim,
                vector_parameters.count,
                quantile,
            ) {
                Self::alpha_offset_from_min_max(min, max)
            } else {
                (alpha, offset)
            }
        } else {
            (alpha, offset)
        };

        for vector in orig_data {
            if stopped.load(Ordering::Relaxed) {
                return Err(ErrorCode::Internal("check process stopped error"));
            }

            let mut encoded_vector = Vec::with_capacity(actual_dim + std::mem::size_of::<f32>());
            encoded_vector.extend_from_slice(&f32::default().to_ne_bytes());
            for &value in vector.as_ref() {
                let encoded = Self::f32_to_u8(value, alpha, offset);
                encoded_vector.push(encoded);
            }
            if vector_parameters.dim % ALIGNMENT != 0 {
                for _ in 0..(ALIGNMENT - vector_parameters.dim % ALIGNMENT) {
                    let placeholder = match vector_parameters.distance_type {
                        DistanceType::Dot => 0.0,
                        DistanceType::L1 | DistanceType::L2 => offset,
                    };
                    let encoded = Self::f32_to_u8(placeholder, alpha, offset);
                    encoded_vector.push(encoded);
                }
            }
            let vector_offset = match vector_parameters.distance_type {
                DistanceType::Dot => {
                    actual_dim as f32 * offset * offset
                        + encoded_vector.iter().map(|&x| f32::from(x)).sum::<f32>() * alpha * offset
                }
                DistanceType::L1 => 0.0,
                DistanceType::L2 => {
                    actual_dim as f32 * offset * offset
                        + encoded_vector
                            .iter()
                            .map(|&x| f32::from(x) * f32::from(x))
                            .sum::<f32>()
                            * alpha
                            * alpha
                }
            };
            let vector_offset = if vector_parameters.invert {
                -vector_offset
            } else {
                vector_offset
            };
            encoded_vector[0..std::mem::size_of::<f32>()]
                .copy_from_slice(&vector_offset.to_ne_bytes());
            storage_builder.push_vector_data(&encoded_vector);
        }
        let multiplier = match vector_parameters.distance_type {
            DistanceType::Dot => alpha * alpha,
            DistanceType::L1 => alpha,
            DistanceType::L2 => -2.0 * alpha * alpha,
        };
        let multiplier = if vector_parameters.invert {
            -multiplier
        } else {
            multiplier
        };

        Ok(EncodedVectorsU8 {
            encoded_vectors: storage_builder.build(),
            score_kernel: select_score_kernel(vector_parameters.distance_type, actual_dim),
            metadata: Metadata {
                actual_dim,
                alpha,
                offset,
                multiplier,
                vector_parameters: vector_parameters.clone(),
            },
        })
    }

    pub fn score_point_simple(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        let (vector_offset, v_ptr) = self.get_vec_ptr(i);
        let q_ptr = query.encoded_query.as_ptr();
        let score = match self.score_kernel {
            Some(kernel) => unsafe { kernel(q_ptr, v_ptr, self.metadata.actual_dim as u32) },
            None => {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot(q_ptr, v_ptr, self.metadata.actual_dim)
                    }
                    DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, self.metadata.actual_dim),
                };
                score as f32
            }
        };
        self.metadata.multiplier * score + query.offset + vector_offset
    }

    /// Hints the CPU to load vector `i` into the cache, ahead of scoring it. HNSW
    /// search scores the neighbors of a node in an order unrelated to memory, so
    /// each vector is otherwise read from memory when it is scored.
    #[inline]
    pub fn prefetch(&self, i: u32) {
        prefetch_slice(self.vector_data(i));
    }

    /// Like [`Self::prefetch`], but no deeper than L2.
    #[inline]
    pub fn prefetch_l2(&self, i: u32) {
        prefetch_slice_l2(self.vector_data(i));
    }

    /// The near and far prefetch windows for scoring a batch of vectors (see
    /// `prefetch_windows`), or `None` when the vectors fit in the cache.
    pub fn prefetch_windows(&self) -> Option<(usize, usize)> {
        let vector_size = self.metadata.actual_dim + std::mem::size_of::<f32>();
        let storage_bytes = vector_size.saturating_mul(self.metadata.vector_parameters.count);
        (storage_bytes >= MIN_PREFETCH_STORAGE_BYTES).then(|| prefetch_windows(vector_size))
    }

    #[inline]
    fn vector_data(&self, i: u32) -> &[u8] {
        let size = self.metadata.actual_dim + std::mem::size_of::<f32>();
        self.encoded_vectors.get_vector_data(i as usize, size)
    }

    fn find_alpha_offset_size_dim<'a>(
        orig_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    ) -> (f32, f32) {
        let (min, max) = find_min_max_from_iter(orig_data);
        Self::alpha_offset_from_min_max(min, max)
    }

    fn alpha_offset_from_min_max(min: f32, max: f32) -> (f32, f32) {
        let alpha = (max - min) / 127.0;
        let offset = min;
        (alpha, offset)
    }

    fn f32_to_u8(i: f32, alpha: f32, offset: f32) -> u8 {
        let i = (i - offset) / alpha;
        i.clamp(0.0, 127.0) as u8
    }

    #[inline]
    fn get_vec_ptr(&self, i: u32) -> (f32, *const u8) {
        unsafe {
            let vector_data_size = self.metadata.actual_dim + std::mem::size_of::<f32>();
            let v_ptr = self
                .encoded_vectors
                .get_vector_data(i as usize, vector_data_size)
                .as_ptr();
            let vector_offset = *v_ptr.cast::<f32>();
            (vector_offset, v_ptr.add(std::mem::size_of::<f32>()))
        }
    }

    #[allow(dead_code)]
    pub fn get_quantized_vector(&self, i: u32) -> (f32, &[u8]) {
        let (offset, v_ptr) = self.get_vec_ptr(i);
        let vector_data_size = self.metadata.actual_dim;
        (offset, unsafe {
            std::slice::from_raw_parts(v_ptr, vector_data_size)
        })
    }

    pub fn get_quantized_vector_size(vector_parameters: &VectorParameters) -> usize {
        let actual_dim = Self::get_actual_dim(vector_parameters);
        actual_dim + std::mem::size_of::<f32>()
    }

    #[allow(dead_code)]
    pub fn get_multiplier(&self) -> f32 {
        self.metadata.multiplier
    }

    #[allow(dead_code)]
    pub fn get_diff(&self) -> f32 {
        let diff = self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset;
        if self.metadata.vector_parameters.invert {
            -diff
        } else {
            diff
        }
    }

    pub fn get_actual_dim(vector_parameters: &VectorParameters) -> usize {
        vector_parameters.dim + (ALIGNMENT - vector_parameters.dim % ALIGNMENT) % ALIGNMENT
    }

    #[allow(dead_code)]
    pub fn vectors_count(&self) -> usize {
        self.metadata.vector_parameters.count
    }
}

impl<TStorage: EncodedStorage> EncodedVectors<EncodedQueryU8> for EncodedVectorsU8<TStorage> {
    fn build_data(&self) -> Result<Vec<u8>> {
        self.encoded_vectors.to_vec()
    }

    fn build_meta(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        serde_json::to_writer(&mut buf, &self.metadata)?;
        Ok(buf)
    }

    fn load(data: &[u8], meta: &[u8], vector_parameters: &VectorParameters) -> Result<Self> {
        let contents = std::str::from_utf8(meta)?;
        let metadata: Metadata = serde_json::from_str(contents)?;
        let quantized_vector_size = Self::get_quantized_vector_size(vector_parameters);
        let encoded_vectors =
            TStorage::from_slice(data, quantized_vector_size, vector_parameters.count)?;
        let score_kernel = select_score_kernel(
            metadata.vector_parameters.distance_type,
            metadata.actual_dim,
        );
        let result = Self {
            encoded_vectors,
            metadata,
            score_kernel,
        };
        Ok(result)
    }

    fn encode_query(&self, query: &[f32]) -> EncodedQueryU8 {
        let dim = query.len();
        let mut query: Vec<_> = query
            .iter()
            .map(|&v| Self::f32_to_u8(v, self.metadata.alpha, self.metadata.offset))
            .collect();
        if dim % ALIGNMENT != 0 {
            for _ in 0..(ALIGNMENT - dim % ALIGNMENT) {
                let placeholder = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot => 0.0,
                    DistanceType::L1 | DistanceType::L2 => self.metadata.offset,
                };
                let encoded =
                    Self::f32_to_u8(placeholder, self.metadata.alpha, self.metadata.offset);
                query.push(encoded);
            }
        }
        let offset = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot => {
                query.iter().map(|&x| f32::from(x)).sum::<f32>()
                    * self.metadata.alpha
                    * self.metadata.offset
            }
            DistanceType::L1 => 0.0,
            DistanceType::L2 => {
                query
                    .iter()
                    .map(|&x| f32::from(x) * f32::from(x))
                    .sum::<f32>()
                    * self.metadata.alpha
                    * self.metadata.alpha
            }
        };
        let offset = if self.metadata.vector_parameters.invert {
            -offset
        } else {
            offset
        };
        EncodedQueryU8 {
            offset,
            encoded_query: query,
        }
    }

    fn score_point(&self, query: &EncodedQueryU8, i: u32) -> f32 {
        self.score_point_simple(query, i)
    }

    fn score_internal(&self, i: u32, j: u32) -> f32 {
        let (query_offset, q_ptr) = self.get_vec_ptr(i);
        let (vector_offset, v_ptr) = self.get_vec_ptr(j);
        let diff = self.metadata.actual_dim as f32 * self.metadata.offset * self.metadata.offset;
        let diff = if self.metadata.vector_parameters.invert {
            -diff
        } else {
            diff
        };
        let offset = query_offset + vector_offset - diff;

        let score = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => {
                impl_score_dot(q_ptr, v_ptr, self.metadata.actual_dim)
            }
            DistanceType::L1 => impl_score_l1(q_ptr, v_ptr, self.metadata.actual_dim),
        };

        self.metadata.multiplier * score as f32 + offset
    }
}

/// Largest `actual_dim` for the VNNI kernels: up to it, a dot product of codes
/// up to 127 is at most 2^24, so the f32 lane sums of `impl_score_dot_avx` are
/// exact and its scores match the integer sums of the VNNI kernels. Above it,
/// AVX2 rounds partial sums and can differ by a few ULPs.
#[cfg(target_arch = "x86_64")]
const MAX_VNNI_DIM: usize = (1 << 24) / (127 * 127);

/// The SIMD kernel for `distance_type` and `actual_dim` that the CPU supports,
/// chosen once per index instead of on every score; `None` uses the scalar
/// implementation. Every kernel scores exactly like the one it replaces.
#[allow(unused_variables)]
fn select_score_kernel(distance_type: DistanceType, actual_dim: usize) -> Option<ScoreKernel> {
    #[cfg(target_arch = "x86_64")]
    {
        // One `vpdpbusd` multiplies and sums 64 or 32 bytes; L1 has no VNNI form.
        if distance_type != DistanceType::L1 && actual_dim <= MAX_VNNI_DIM {
            if is_x86_feature_detected!("avx512vnni") && is_x86_feature_detected!("avx512bw") {
                return Some(impl_score_dot_avx512_vnni);
            }
            if is_x86_feature_detected!("avxvnni") && is_x86_feature_detected!("avx2") {
                return Some(impl_score_dot_avx_vnni);
            }
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return Some(match distance_type {
                DistanceType::Dot | DistanceType::L2 => impl_score_dot_avx,
                DistanceType::L1 => impl_score_l1_avx,
            });
        }
        if is_x86_feature_detected!("sse4.1") {
            return Some(match distance_type {
                DistanceType::Dot | DistanceType::L2 => impl_score_dot_sse,
                DistanceType::L1 => impl_score_l1_sse,
            });
        }
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    if std::arch::is_aarch64_feature_detected!("dotprod") {
        // One `udot` multiplies or sums 16 bytes into 4 lanes (FEAT_DotProd, on
        // Graviton 2 and later).
        return Some(match distance_type {
            DistanceType::Dot | DistanceType::L2 => impl_score_dot_neon_dotprod,
            DistanceType::L1 => impl_score_l1_neon_dotprod,
        });
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    if std::arch::is_aarch64_feature_detected!("neon") {
        return Some(match distance_type {
            DistanceType::Dot | DistanceType::L2 => impl_score_dot_neon,
            DistanceType::L1 => impl_score_l1_neon,
        });
    }
    None
}

fn impl_score_dot(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)) * i32::from(*v_ptr.add(i));
        }
        score
    }
}

fn impl_score_l1(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)).abs_diff(i32::from(*v_ptr.add(i))) as i32;
        }
        score
    }
}

#[cfg(target_arch = "x86_64")]
unsafe extern "C" {
    fn impl_score_dot_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;

    fn impl_score_dot_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}

/// The dot product with AVX-512 VNNI: 64 bytes per `vpdpbusd`, two
/// accumulators, and a masked load for the last 16 to 48 bytes. Codes are in
/// `0..=127`, so the unsigned-by-signed byte products are exact, and the sum is
/// the integer of `impl_score_dot`. `dim` is a multiple of `ALIGNMENT`.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
unsafe extern "C" fn impl_score_dot_avx512_vnni(
    q_ptr: *const u8,
    v_ptr: *const u8,
    dim: u32,
) -> f32 {
    use std::arch::x86_64::*;
    let dim = dim as usize;
    unsafe {
        let mut acc0 = _mm512_setzero_si512();
        let mut acc1 = _mm512_setzero_si512();
        let mut i = 0;
        while i + 128 <= dim {
            acc0 = _mm512_dpbusd_epi32(
                acc0,
                _mm512_loadu_si512(v_ptr.add(i).cast()),
                _mm512_loadu_si512(q_ptr.add(i).cast()),
            );
            acc1 = _mm512_dpbusd_epi32(
                acc1,
                _mm512_loadu_si512(v_ptr.add(i + 64).cast()),
                _mm512_loadu_si512(q_ptr.add(i + 64).cast()),
            );
            i += 128;
        }
        if i + 64 <= dim {
            acc0 = _mm512_dpbusd_epi32(
                acc0,
                _mm512_loadu_si512(v_ptr.add(i).cast()),
                _mm512_loadu_si512(q_ptr.add(i).cast()),
            );
            i += 64;
        }
        if i < dim {
            let mask: __mmask64 = (1u64 << (dim - i)) - 1;
            acc1 = _mm512_dpbusd_epi32(
                acc1,
                _mm512_maskz_loadu_epi8(mask, v_ptr.add(i).cast()),
                _mm512_maskz_loadu_epi8(mask, q_ptr.add(i).cast()),
            );
        }
        _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc1)) as f32
    }
}

/// The dot product with AVX-VNNI, for CPUs without AVX-512: 32 bytes per
/// `vpdpbusd`, two accumulators, and one 128-bit step for the last 16 bytes.
/// Exact as [`impl_score_dot_avx512_vnni`].
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avxvnni")]
unsafe extern "C" fn impl_score_dot_avx_vnni(q_ptr: *const u8, v_ptr: *const u8, dim: u32) -> f32 {
    use std::arch::x86_64::*;
    let dim = dim as usize;
    unsafe {
        let mut acc0 = _mm256_setzero_si256();
        let mut acc1 = _mm256_setzero_si256();
        let mut i = 0;
        while i + 64 <= dim {
            acc0 = _mm256_dpbusd_avx_epi32(
                acc0,
                _mm256_loadu_si256(v_ptr.add(i).cast()),
                _mm256_loadu_si256(q_ptr.add(i).cast()),
            );
            acc1 = _mm256_dpbusd_avx_epi32(
                acc1,
                _mm256_loadu_si256(v_ptr.add(i + 32).cast()),
                _mm256_loadu_si256(q_ptr.add(i + 32).cast()),
            );
            i += 64;
        }
        if i + 32 <= dim {
            acc0 = _mm256_dpbusd_avx_epi32(
                acc0,
                _mm256_loadu_si256(v_ptr.add(i).cast()),
                _mm256_loadu_si256(q_ptr.add(i).cast()),
            );
            i += 32;
        }
        let acc = _mm256_add_epi32(acc0, acc1);
        let mut sum = _mm_add_epi32(
            _mm256_castsi256_si128(acc),
            _mm256_extracti128_si256(acc, 1),
        );
        if i < dim {
            sum = _mm_dpbusd_avx_epi32(
                sum,
                _mm_loadu_si128(v_ptr.add(i).cast()),
                _mm_loadu_si128(q_ptr.add(i).cast()),
            );
        }
        sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b01_00_11_10));
        sum = _mm_add_epi32(sum, _mm_shuffle_epi32(sum, 0b10_11_00_01));
        _mm_cvtsi128_si32(sum) as f32
    }
}

/// The dot product with NEON `udot`: 16 bytes per instruction, four
/// accumulators. Codes are in `0..=127`, so the unsigned products and their u32
/// sum are exact, the integer of `impl_score_dot`. `dim` is a multiple of
/// `ALIGNMENT`.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon,dotprod")]
unsafe extern "C" fn impl_score_dot_neon_dotprod(
    q_ptr: *const u8,
    v_ptr: *const u8,
    dim: u32,
) -> f32 {
    use std::arch::aarch64::*;
    let dim = dim as usize;
    unsafe {
        let mut acc = [vdupq_n_u32(0); 4];
        let mut i = 0;
        while i + 64 <= dim {
            for (j, acc) in acc.iter_mut().enumerate() {
                let offset = i + 16 * j;
                *acc = vdotq_u32(
                    *acc,
                    vld1q_u8(q_ptr.add(offset)),
                    vld1q_u8(v_ptr.add(offset)),
                );
            }
            i += 64;
        }
        while i < dim {
            acc[0] = vdotq_u32(acc[0], vld1q_u8(q_ptr.add(i)), vld1q_u8(v_ptr.add(i)));
            i += 16;
        }
        let sum = vaddq_u32(vaddq_u32(acc[0], acc[1]), vaddq_u32(acc[2], acc[3]));
        vaddvq_u32(sum) as f32
    }
}

/// The L1 distance with NEON: `vabdq_u8` gives the absolute differences, and a
/// `udot` with ones sums them into u32 lanes, exactly as `impl_score_l1`. `dim`
/// is a multiple of `ALIGNMENT`.
#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
#[target_feature(enable = "neon,dotprod")]
unsafe extern "C" fn impl_score_l1_neon_dotprod(
    q_ptr: *const u8,
    v_ptr: *const u8,
    dim: u32,
) -> f32 {
    use std::arch::aarch64::*;
    let dim = dim as usize;
    unsafe {
        let ones = vdupq_n_u8(1);
        let mut acc = [vdupq_n_u32(0); 4];
        let mut i = 0;
        while i + 64 <= dim {
            for (j, acc) in acc.iter_mut().enumerate() {
                let offset = i + 16 * j;
                let diff = vabdq_u8(vld1q_u8(q_ptr.add(offset)), vld1q_u8(v_ptr.add(offset)));
                *acc = vdotq_u32(*acc, diff, ones);
            }
            i += 64;
        }
        while i < dim {
            let diff = vabdq_u8(vld1q_u8(q_ptr.add(i)), vld1q_u8(v_ptr.add(i)));
            acc[0] = vdotq_u32(acc[0], diff, ones);
            i += 16;
        }
        let sum = vaddq_u32(vaddq_u32(acc[0], acc[1]), vaddq_u32(acc[2], acc[3]));
        vaddvq_u32(sum) as f32
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe extern "C" {
    fn impl_score_dot_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kernel chosen once scores like the scalar implementation, and every
    /// vector, including the last one, can be prefetched.
    #[test]
    fn test_score_kernel_and_prefetch() -> Result<()> {
        // 20 dimensions are padded to 32, and vectors of 36 bytes do not start on
        // cache lines.
        let (dim, count) = (20, 50);
        let data: Vec<f32> = (0..dim * count)
            .map(|i| ((i * 37 % 101) as f32 - 50.0) / 25.0)
            .collect();
        let query: Vec<f32> = (0..dim).map(|i| (i as f32 - 10.0) / 7.0).collect();
        for (distance_type, invert) in [
            (DistanceType::Dot, false),
            (DistanceType::L1, true),
            (DistanceType::L2, true),
        ] {
            let params = VectorParameters {
                dim,
                count,
                distance_type,
                invert,
            };
            let vectors = EncodedVectorsU8::encode(
                data.chunks(dim),
                Vec::new(),
                &params,
                None,
                &AtomicBool::new(false),
            )?;
            let mut scalar = EncodedVectorsU8::<Vec<u8>>::load(
                &vectors.build_data()?,
                &vectors.build_meta()?,
                &params,
            )?;
            scalar.score_kernel = None;
            let encoded = vectors.encode_query(&query);
            for i in 0..count as u32 {
                vectors.prefetch(i);
                assert_eq!(
                    vectors.score_point(&encoded, i),
                    scalar.score_point(&encoded, i)
                );
            }
            // 50 vectors of 36 bytes fit in the cache.
            assert_eq!(vectors.prefetch_windows(), None);
        }
        Ok(())
    }

    /// The dotprod kernels give the exact integers of the scalar dot product and
    /// L1 distance, for every length that is a multiple of `ALIGNMENT`, with
    /// codes up to 127.
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    #[test]
    fn test_score_neon_dotprod() {
        if !std::arch::is_aarch64_feature_detected!("dotprod") {
            return;
        }
        for dim in (ALIGNMENT..=320).step_by(ALIGNMENT) {
            let q: Vec<u8> = (0..dim).map(|i| (i * 37 % 128) as u8).collect();
            let v: Vec<u8> = (0..dim).map(|i| 127 - (i * 11 % 128) as u8).collect();
            let (q, v) = (q.as_ptr(), v.as_ptr());
            let dot = unsafe { impl_score_dot_neon_dotprod(q, v, dim as u32) };
            assert_eq!(dot, impl_score_dot(q, v, dim) as f32, "dot, dim {dim}");
            let l1 = unsafe { impl_score_l1_neon_dotprod(q, v, dim as u32) };
            assert_eq!(l1, impl_score_l1(q, v, dim) as f32, "l1, dim {dim}");
        }
    }

    /// Codes in `64..=127` from an LCG, high enough that 4,096-dimensional dot
    /// products exceed 2^24.
    #[cfg(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "neon")
    ))]
    fn high_codes(dim: usize, seed: u64) -> Vec<u8> {
        let mut x = seed;
        (0..dim)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                64 + (x >> 58) as u8
            })
            .collect()
    }

    /// The kernel chosen for every distance and dimension scores exactly like
    /// the kernel main uses on this CPU (AVX2 on x86, NEON on arm), including
    /// dot products above 2^24, where AVX2 rounds its f32 partial sums.
    #[cfg(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "neon")
    ))]
    #[test]
    fn test_score_kernel_matches_main() {
        #[cfg(target_arch = "x86_64")]
        {
            if !(is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma")) {
                return;
            }
        }
        #[cfg(target_arch = "x86_64")]
        let (main_dot, main_l1): (ScoreKernel, ScoreKernel) =
            (impl_score_dot_avx, impl_score_l1_avx);
        #[cfg(target_arch = "aarch64")]
        let (main_dot, main_l1): (ScoreKernel, ScoreKernel) =
            (impl_score_dot_neon, impl_score_l1_neon);
        let main_kernel = |distance_type| match distance_type {
            DistanceType::Dot | DistanceType::L2 => main_dot,
            DistanceType::L1 => main_l1,
        };
        let mut rounded = 0;
        for dim in [16, 1024, 1040, 1056, 1536, 4080, 4096] {
            let mut cases = vec![
                (vec![127u8; dim], vec![127u8; dim]),
                (vec![0; dim], vec![127; dim]),
            ];
            cases.extend((1..40).map(|seed| (high_codes(dim, seed), high_codes(dim, seed + 1000))));
            for (q, v) in &cases {
                let (q, v) = (q.as_ptr(), v.as_ptr());
                for distance_type in [DistanceType::Dot, DistanceType::L1, DistanceType::L2] {
                    let kernel = select_score_kernel(distance_type, dim).unwrap();
                    let expected = unsafe { main_kernel(distance_type)(q, v, dim as u32) };
                    let score = unsafe { kernel(q, v, dim as u32) };
                    assert_eq!(score, expected, "{distance_type:?}, dim {dim}");
                }
                let dot = unsafe { main_dot(q, v, dim as u32) };
                rounded += usize::from(dot != impl_score_dot(q, v, dim) as f32);
            }
        }
        // The cases include sums that AVX2 rounds (arm sums exactly).
        if cfg!(target_arch = "x86_64") {
            assert!(rounded > 0);
        } else {
            assert_eq!(rounded, 0);
        }
    }

    /// The VNNI kernels give the exact integer of the scalar dot product, for
    /// every length that is a multiple of `ALIGNMENT`, with codes up to 127.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_score_dot_vnni() {
        let avx512 = is_x86_feature_detected!("avx512vnni") && is_x86_feature_detected!("avx512bw");
        let avx = is_x86_feature_detected!("avxvnni") && is_x86_feature_detected!("avx2");
        for dim in (ALIGNMENT..=320).step_by(ALIGNMENT) {
            let q: Vec<u8> = (0..dim).map(|i| (i * 37 % 128) as u8).collect();
            let v: Vec<u8> = (0..dim).map(|i| 127 - (i * 11 % 128) as u8).collect();
            let expected = impl_score_dot(q.as_ptr(), v.as_ptr(), dim) as f32;
            if avx512 {
                let score =
                    unsafe { impl_score_dot_avx512_vnni(q.as_ptr(), v.as_ptr(), dim as u32) };
                assert_eq!(score, expected, "avx512 vnni, dim {dim}");
            }
            if avx {
                let score = unsafe { impl_score_dot_avx_vnni(q.as_ptr(), v.as_ptr(), dim as u32) };
                assert_eq!(score, expected, "avx vnni, dim {dim}");
            }
        }
    }
}
