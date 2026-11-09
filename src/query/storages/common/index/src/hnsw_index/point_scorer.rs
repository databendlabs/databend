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
use crate::hnsw_index::common::utils::MAX_UNPREFETCHED_BATCH;
use crate::hnsw_index::common::utils::is_dense_batch;
use crate::hnsw_index::quantization::EncodedQueryU8;
use crate::hnsw_index::quantization::EncodedVectorsU8;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;
use crate::hnsw_index::spaces::prefetch::prefetch_slice;
use crate::hnsw_index::spaces::simple::similarity;

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

    /// Hints the CPU to load the vector of `point_id` ahead of scoring it.
    #[inline]
    pub fn prefetch(&self, point_id: PointOffsetType) {
        match self {
            RawScorer::Original(original) => {
                match unsafe { original.column.index_unchecked(point_id as usize) } {
                    ScalarRef::Vector(VectorScalarRef::Float32(values)) => prefetch_slice(values),
                    ScalarRef::Vector(VectorScalarRef::Int8(values)) => prefetch_slice(values),
                    _ => {}
                }
            }
            RawScorer::Quantized(quantized) => quantized.vector.prefetch(point_id),
        }
    }

    /// Like [`Self::prefetch`], but no deeper than L2.
    #[inline]
    pub fn prefetch_l2(&self, point_id: PointOffsetType) {
        if let RawScorer::Quantized(quantized) = self {
            quantized.vector.prefetch_l2(point_id);
        }
    }

    /// The near and far prefetch windows for scoring a batch, in vectors, or
    /// `None` when prefetch does not help.
    pub fn prefetch_windows(&self) -> Option<(usize, usize)> {
        match self {
            // The build scores the original vectors, which do not fit in the
            // cache: load the next one while the current one is scored.
            RawScorer::Original(_) => Some((1, 0)),
            RawScorer::Quantized(quantized) => quantized.vector.prefetch_windows(),
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
        let count = point_ids.len().min(self.points_buffer.len());
        let point_ids = &point_ids[..count];
        // Prefetch `near` vectors ahead into L1 and `far` vectors ahead into L2,
        // so that a bounded number of loads overlap with scoring. Tiny batches and
        // nearly consecutive ids gain nothing from hints.
        let windows = self
            .raw_scorer
            .prefetch_windows()
            .filter(|_| count > MAX_UNPREFETCHED_BATCH && !is_dense_batch(point_ids));
        if let Some((near, far)) = windows {
            for &point_id in point_ids.iter().take(far).skip(near) {
                self.raw_scorer.prefetch_l2(point_id);
            }
            for &point_id in point_ids.iter().take(near) {
                self.raw_scorer.prefetch(point_id);
            }
        }
        for (i, &point_id) in point_ids.iter().enumerate() {
            if let Some((near, far)) = windows {
                if let Some(&upcoming) = point_ids.get(i + far).filter(|_| far > 0) {
                    self.raw_scorer.prefetch_l2(upcoming);
                }
                if let Some(&upcoming) = point_ids.get(i + near) {
                    self.raw_scorer.prefetch(upcoming);
                }
            }
            self.points_buffer[i] = ScoredPointOffset {
                idx: point_id,
                score: self.raw_scorer.score_point(point_id),
            };
        }
        &self.points_buffer[..count]
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
            similarity(distance_type, &l, &r)
        }
        (
            ScalarRef::Vector(VectorScalarRef::Float32(lhs)),
            ScalarRef::Vector(VectorScalarRef::Float32(rhs)),
        ) => {
            let l = unsafe { std::mem::transmute::<&[F32], &[f32]>(lhs) };
            let r = unsafe { std::mem::transmute::<&[F32], &[f32]>(rhs) };
            similarity(distance_type, l, r)
        }
        (_, _) => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::hnsw_index::quantization::VectorParameters;

    /// Scoring a batch with prefetch scores every point as alone, and keeps at
    /// most `limit` points.
    #[test]
    fn test_score_points_with_prefetch() -> databend_common_exception::Result<()> {
        // 4,200 vectors of 260 bytes: over 1 MiB, so prefetch is used.
        let (dim, count) = (256, 4200);
        let data: Vec<f32> = (0..dim * count)
            .map(|i| ((i * 37 % 101) as f32 - 50.0) / 25.0)
            .collect();
        let params = VectorParameters {
            dim,
            count,
            distance_type: DistanceType::L2,
            invert: true,
        };
        let vectors = EncodedVectorsU8::encode(
            data.chunks(dim),
            Vec::new(),
            &params,
            None,
            &AtomicBool::new(false),
        )?;
        assert!(vectors.prefetch_windows().is_some());
        let query: Vec<f32> = (0..dim).map(|i| (i as f32 - 128.0) / 64.0).collect();
        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: vectors.encode_query(&query),
            vector: &vectors,
        });
        let mut scorer = FilteredScorer::new(&raw_scorer);
        for (len, limit) in [(0, 0), (2, 0), (40, 0), (40, 16), (10, 32)] {
            let mut ids: Vec<u32> = (0..len).map(|i| (i * 977 % count) as u32).collect();
            let expected: Vec<ScoredPointOffset> = ids
                .iter()
                .take(if limit == 0 { len } else { limit })
                .map(|&idx| ScoredPointOffset {
                    idx,
                    score: raw_scorer.score_point(idx),
                })
                .collect();
            assert_eq!(scorer.score_points(&mut ids, limit), expected.as_slice());
        }
        Ok(())
    }
}
