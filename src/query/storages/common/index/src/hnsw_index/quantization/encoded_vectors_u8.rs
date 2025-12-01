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

use crate::hnsw_index::quantization::encoded_storage::EncodedStorage;
use crate::hnsw_index::quantization::encoded_storage::EncodedStorageBuilder;
use crate::hnsw_index::quantization::encoded_vectors::validate_vector_parameters;
use crate::hnsw_index::quantization::encoded_vectors::DistanceType;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;
use crate::hnsw_index::quantization::encoded_vectors::VectorParameters;
use crate::hnsw_index::quantization::quantile::find_min_max_from_iter;
use crate::hnsw_index::quantization::quantile::find_quantile_interval;

pub const ALIGNMENT: usize = 16;

pub struct EncodedVectorsU8<TStorage: EncodedStorage> {
    encoded_vectors: TStorage,
    metadata: Metadata,
}

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
        #[cfg(target_arch = "x86_64")]
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_avx(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score as f32 + query.offset + vector_offset;
            }
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if is_x86_feature_detected!("sse4.1") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_sse(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score as f32 + query.offset + vector_offset;
            }
        }

        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        if std::arch::is_aarch64_feature_detected!("neon") {
            unsafe {
                let score = match self.metadata.vector_parameters.distance_type {
                    DistanceType::Dot | DistanceType::L2 => {
                        impl_score_dot_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                    DistanceType::L1 => {
                        impl_score_l1_neon(q_ptr, v_ptr, self.metadata.actual_dim as u32)
                    }
                };

                return self.metadata.multiplier * score as f32 + query.offset + vector_offset;
            }
        }

        let score = match self.metadata.vector_parameters.distance_type {
            DistanceType::Dot | DistanceType::L2 => impl_score_dot(
                query.encoded_query.as_ptr(),
                v_ptr,
                self.metadata.actual_dim,
            ),
            DistanceType::L1 => impl_score_l1(
                query.encoded_query.as_ptr(),
                v_ptr,
                self.metadata.actual_dim,
            ),
        };

        self.metadata.multiplier * score as f32 + query.offset + vector_offset
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
        let result = Self {
            encoded_vectors,
            metadata,
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

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe extern "C" {
    fn impl_score_dot_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    fn impl_score_l1_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}
