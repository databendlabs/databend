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

use databend_common_expression::Column;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorScalarRef;

use crate::DistanceType;
use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoreType;
use crate::hnsw_index::common::types::ScoredPointOffset;
use crate::hnsw_index::quantization::EncodedQueryU8;
use crate::hnsw_index::quantization::EncodedVectorsU8;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;

pub enum RawScorer<'a> {
    Original(OriginalRawScorer<'a>),
    Quantized(QuantizedRawScorer<'a>),
}

pub struct OriginalRawScorer<'a> {
    pub distance_type: DistanceType,
    pub index: usize,
    pub column: &'a Column,
}

pub struct QuantizedRawScorer<'a> {
    pub query: EncodedQueryU8,
    pub vector: &'a EncodedVectorsU8<Vec<u8>>,
}

impl RawScorer<'_> {
    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        match self {
            RawScorer::Original(original) => {
                let self_val = unsafe { original.column.index_unchecked(original.index) };
                let point_val = unsafe { original.column.index_unchecked(point_id as usize) };
                calculate_score(original.distance_type, self_val, point_val)
            }
            RawScorer::Quantized(quantized) => {
                quantized.vector.score_point(&quantized.query, point_id)
            }
        }
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        match self {
            RawScorer::Original(original) => {
                let point_a_val = unsafe { original.column.index_unchecked(point_a as usize) };
                let point_b_val = unsafe { original.column.index_unchecked(point_b as usize) };
                calculate_score(original.distance_type, point_a_val, point_b_val)
            }
            RawScorer::Quantized(quantized) => quantized.vector.score_internal(point_a, point_b),
        }
    }
}

pub struct FilteredScorer<'a> {
    raw_scorer: &'a RawScorer<'a>,
    points_buffer: Vec<ScoredPointOffset>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(raw_scorer: &'a RawScorer) -> Self {
        FilteredScorer {
            raw_scorer,
            points_buffer: Vec::new(),
        }
    }

    pub fn check_vector(&self, _point_id: PointOffsetType) -> bool {
        true
    }

    /// Method filters and calculates scores for the given slice of points IDs
    ///
    /// For performance reasons this function mutates input values.
    /// For result slice allocation this function mutates self.
    ///
    /// # Arguments
    ///
    /// * `point_ids` - list of points to score. *Warn*: This input will be wrecked during the execution.
    /// * `limit` - limits the number of points to process after filtering.
    pub fn score_points(
        &mut self,
        point_ids: &mut [PointOffsetType],
        limit: usize,
    ) -> &[ScoredPointOffset] {
        if limit == 0 {
            self.points_buffer
                .resize_with(point_ids.len(), ScoredPointOffset::default);
        } else {
            self.points_buffer
                .resize_with(limit, ScoredPointOffset::default);
        }
        let mut size: usize = 0;
        for point_id in point_ids.iter().copied() {
            let score = self.score_point(point_id);
            self.points_buffer[size] = ScoredPointOffset {
                idx: point_id,
                score,
            };

            size += 1;
            if size == self.points_buffer.len() {
                break;
            }
        }
        &self.points_buffer[0..size]
    }

    pub fn score_point(&self, point_id: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_point(point_id)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        self.raw_scorer.score_internal(point_a, point_b)
    }
}

fn calculate_score(distance_type: DistanceType, lhs: ScalarRef, rhs: ScalarRef) -> f32 {
    match (lhs, rhs) {
        (
            ScalarRef::Vector(VectorScalarRef::Int8(lhs)),
            ScalarRef::Vector(VectorScalarRef::Int8(rhs)),
        ) => {
            let l: Vec<_> = lhs.iter().map(|v| *v as f32).collect();
            let r: Vec<_> = rhs.iter().map(|v| *v as f32).collect();
            match distance_type {
                DistanceType::Dot => dot_similarity(&l, &r),
                DistanceType::L1 => manhattan_similarity(&l, &r),
                DistanceType::L2 => euclid_similarity(&l, &r),
            }
        }
        (
            ScalarRef::Vector(VectorScalarRef::Float32(lhs)),
            ScalarRef::Vector(VectorScalarRef::Float32(rhs)),
        ) => {
            let l = unsafe { std::mem::transmute::<&[F32], &[f32]>(lhs) };
            let r = unsafe { std::mem::transmute::<&[F32], &[f32]>(rhs) };
            match distance_type {
                DistanceType::Dot => dot_similarity(l, r),
                DistanceType::L1 => manhattan_similarity(l, r),
                DistanceType::L2 => euclid_similarity(l, r),
            }
        }
        (_, _) => 0.0,
    }
}

fn dot_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    v1.iter().zip(v2).map(|(a, b)| a * b).sum()
}

fn euclid_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    -v1.iter().zip(v2).map(|(a, b)| (a - b).powi(2)).sum::<f32>()
}

fn manhattan_similarity(v1: &[f32], v2: &[f32]) -> f32 {
    -v1.iter().zip(v2).map(|(a, b)| (a - b).abs()).sum::<f32>()
}
