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

use databend_common_expression::types::VectorScalarRef;
use databend_common_expression::types::F32;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_vector::cosine_distance;

use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoreType;
use crate::hnsw_index::common::types::ScoredPointOffset;

pub enum RawScorer {
    Index(usize),
    Scalar(Scalar),
}

pub struct FilteredScorer<'a> {
    raw_scorer: &'a RawScorer,
    column: &'a Column,
    points_buffer: Vec<ScoredPointOffset>,
}

impl<'a> FilteredScorer<'a> {
    pub fn new(raw_scorer: &'a RawScorer, column: &'a Column) -> Self {
        FilteredScorer {
            raw_scorer,
            column,
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
        let self_val = match self.raw_scorer {
            RawScorer::Index(index) => unsafe { self.column.index_unchecked(*index) },
            RawScorer::Scalar(scalar) => scalar.as_ref(),
        };

        for point_id in point_ids.iter().copied() {
            let point_val = unsafe { self.column.index_unchecked(point_id as usize) };
            let score = calculate_score(self_val.clone(), point_val);
            // println!("---point_id={:?} score={:?}", point_id, score);

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
        let self_val = match self.raw_scorer {
            RawScorer::Index(index) => unsafe { self.column.index_unchecked(*index) },
            RawScorer::Scalar(scalar) => scalar.as_ref(),
        };
        let point_val = unsafe { self.column.index_unchecked(point_id as usize) };
        calculate_score(self_val, point_val)
    }

    pub fn score_internal(&self, point_a: PointOffsetType, point_b: PointOffsetType) -> ScoreType {
        let point_a_val = unsafe { self.column.index_unchecked(point_a as usize) };
        let point_b_val = unsafe { self.column.index_unchecked(point_b as usize) };
        calculate_score(point_a_val, point_b_val)
    }
}

fn calculate_score(lhs: ScalarRef, rhs: ScalarRef) -> f32 {
    match (lhs, rhs) {
        (
            ScalarRef::Vector(VectorScalarRef::Int8(lhs)),
            ScalarRef::Vector(VectorScalarRef::Int8(rhs)),
        ) => {
            let l: Vec<_> = lhs.iter().map(|v| *v as f32).collect();
            let r: Vec<_> = rhs.iter().map(|v| *v as f32).collect();
            cosine_distance(l.as_slice(), r.as_slice()).unwrap()
        }
        (
            ScalarRef::Vector(VectorScalarRef::Float32(lhs)),
            ScalarRef::Vector(VectorScalarRef::Float32(rhs)),
        ) => {
            let l = unsafe { std::mem::transmute::<&[F32], &[f32]>(lhs) };
            let r = unsafe { std::mem::transmute::<&[F32], &[f32]>(rhs) };
            cosine_distance(l, r).unwrap()
        }
        (_, _) => 0.0,
    }
}
