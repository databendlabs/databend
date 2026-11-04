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

use databend_common_column::bitmap::TrueIdxIter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::VectorColumnBuilder;
use databend_common_expression::types::VectorScalar;
use log::error;
use rand::thread_rng;
use rayon::ThreadPoolBuilder;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;

use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoredPointOffset;
use crate::hnsw_index::common::utils::check_process_stopped;
use crate::hnsw_index::graph_layers::GraphLayers;
use crate::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::hnsw_index::graph_links::GraphLinksFormat;
use crate::hnsw_index::point_scorer::FilteredScorer;
use crate::hnsw_index::point_scorer::OriginalRawScorer;
use crate::hnsw_index::point_scorer::QuantizedRawScorer;
use crate::hnsw_index::point_scorer::RawScorer;
use crate::hnsw_index::quantization::DistanceType;
use crate::hnsw_index::quantization::EncodedVectorsU8;
use crate::hnsw_index::quantization::VectorParameters;
use crate::hnsw_index::quantization::encoded_vectors::EncodedVectors;

const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

/// Name suffixes of the four HNSW components, in the order `HNSWIndex::build`
/// produces and `HNSWIndex::open` expects them.
const HNSW_GRAPH_LINKS_SUFFIX: &str = "_graph_links";
const HNSW_GRAPH_DATA_SUFFIX: &str = "_graph_data";
const HNSW_ENCODED_U8_META_SUFFIX: &str = "_encoded_u8_meta";
const HNSW_ENCODED_U8_DATA_SUFFIX: &str = "_encoded_u8_data";
const HNSW_COMPONENT_SUFFIXES: [&str; 4] = [
    HNSW_GRAPH_LINKS_SUFFIX,
    HNSW_GRAPH_DATA_SUFFIX,
    HNSW_ENCODED_U8_META_SUFFIX,
    HNSW_ENCODED_U8_DATA_SUFFIX,
];

/// Component name: `{column_id}-{distance_type}{suffix}`.
fn hnsw_component_name(column_id: ColumnId, distance_type: DistanceType, suffix: &str) -> String {
    format!("{column_id}-{distance_type}{suffix}")
}

/// Names of the four HNSW components of one column and distance, in `open` order.
pub fn hnsw_component_names(column_id: ColumnId, distance_type: DistanceType) -> Vec<String> {
    HNSW_COMPONENT_SUFFIXES
        .iter()
        .map(|suffix| hnsw_component_name(column_id, distance_type, suffix))
        .collect()
}

pub struct HNSWIndex {
    graph_layers: GraphLayers,
    distance_type: DistanceType,
    encoded_vectors: EncodedVectorsU8<Vec<u8>>,
    // Column validity, present only when the column has NULLs (none in old files).
    // Only `generate_scores` needs it: NULL rows are not linked into the graph.
    validity: Option<Bitmap>,
}

impl HNSWIndex {
    /// Opens the four serialized components in `HNSW_COMPONENT_SUFFIXES` order.
    pub fn open(
        distance_type: DistanceType,
        dim: usize,
        count: usize,
        components: &[impl AsRef<[u8]>],
        validity: Option<&[u8]>,
    ) -> Result<HNSWIndex> {
        let [graph_links, graph_data, encoded_meta, encoded_data] = components else {
            return Err(ErrorCode::StorageOther("Expected four HNSW components"));
        };
        let graph_layers = GraphLayers::open(graph_links.as_ref(), graph_data.as_ref())?;

        let invert = match distance_type {
            DistanceType::Dot => false,
            DistanceType::L1 | DistanceType::L2 => true,
        };

        let params = VectorParameters {
            dim,
            count,
            distance_type,
            invert,
        };

        let encoded_vectors: EncodedVectorsU8<Vec<u8>> =
            EncodedVectorsU8::load(encoded_data.as_ref(), encoded_meta.as_ref(), &params)?;

        let validity = validity
            .map(|bytes| {
                Bitmap::try_new(bytes.to_vec(), count).map_err(|error| {
                    ErrorCode::StorageOther(format!("Invalid vector validity: {error}"))
                })
            })
            .transpose()?;

        Ok(Self {
            graph_layers,
            distance_type,
            encoded_vectors,
            validity,
        })
    }

    /// Column validity; `None` when every row is valid.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Memory retained by this index, including the maximum retained visited-list pool.
    /// Reserving the pool up front keeps cache accounting stable as searches reuse it.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.graph_layers.memory_size()
            - std::mem::size_of::<GraphLayers>()
            + self.encoded_vectors.memory_size()
            - std::mem::size_of::<EncodedVectorsU8<Vec<u8>>>()
            + self
                .validity
                .as_ref()
                .map_or(0, |bitmap| bitmap.as_slice().0.len())
    }

    pub fn search(&self, limit: usize, query_values: &[f32]) -> Result<Vec<ScoredPointOffset>> {
        self.search_with_ef(limit, None, query_values, false)
    }

    /// The `limit` nearest rows found, plus, with `keep_ties`, the other rows
    /// found at the distance of the farthest of them: another sort key, such as
    /// `id` in `ORDER BY d, id`, may choose any tied row.
    pub fn search_with_ef(
        &self,
        limit: usize,
        search_ef: Option<usize>,
        query_values: &[f32],
        keep_ties: bool,
    ) -> Result<Vec<ScoredPointOffset>> {
        let query_encode = self.encoded_vectors.encode_query(query_values);

        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: query_encode,
            vector: &self.encoded_vectors,
        });

        // ef is used to maintain the size of the set of candidate points in the search process,
        // the larger the search precision is higher, the smaller the speed is faster,
        // take 4 times the limit is a more balanced parameter.
        let ef = search_ef
            .unwrap_or_else(|| limit.saturating_mul(4))
            .max(limit);
        let query_filter_scorer = FilteredScorer::new(&raw_scorer);

        let is_stopped = AtomicBool::new(false);
        // To find the ties, return all `ef` rows: the search is the same as for
        // `limit` rows, which are the first of them.
        let top = if keep_ties { ef } else { limit };
        let values = self
            .graph_layers
            .search(top, ef, query_filter_scorer, None, &is_stopped)?;

        let values = Self::postprocess_score(self.distance_type, values);
        Ok(truncate_with_ties(values, limit))
    }

    pub fn generate_scores(
        &self,
        row_nums: u32,
        query_values: &[f32],
    ) -> Result<Vec<ScoredPointOffset>> {
        let query_encode = self.encoded_vectors.encode_query(query_values);

        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: query_encode,
            vector: &self.encoded_vectors,
        });

        let score = |idx: u32| ScoredPointOffset {
            idx,
            score: raw_scorer.score_point(idx),
        };
        // NULL rows keep their slot, so `idx` is the block row; skip them here as
        // this path does not go through the graph. The bitmap covers the whole
        // block, which is exactly `row_nums` rows.
        let values = match &self.validity {
            Some(bitmap) => TrueIdxIter::new(row_nums as usize, Some(bitmap))
                .map(|idx| score(idx as u32))
                .collect(),
            None => (0..row_nums).map(score).collect(),
        };

        let values = Self::postprocess_score(self.distance_type, values);
        Ok(values)
    }

    pub fn build(
        m: usize,
        ef_construct: usize,
        column_id: ColumnId,
        column: Column,
        distance_type: DistanceType,
    ) -> Result<(Vec<TableField>, Vec<BlockEntry>)> {
        let m0 = m * 2;
        let entry_points_num = 2;
        let use_heuristic = true;
        let num_vectors = column.len();
        let validity = column
            .as_nullable()
            .map(|nullable| nullable.validity().clone())
            .filter(|bitmap| bitmap.null_count() != 0);

        let column = column.remove_nullable();
        let vector_column = column.as_vector().unwrap();
        let vector_column = preprocess(distance_type, vector_column.clone());

        let mut rng = thread_rng();
        let mut graph_layers_builder = GraphLayersBuilder::new(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            use_heuristic,
        );

        for i in 0..column.len() {
            let vector_id = i as PointOffsetType;
            // NULL rows keep an empty level-0 slot so point ids stay block rows; they
            // are never inserted below, so they are not reachable in the graph.
            let level = if validity.as_ref().is_none_or(|bitmap| bitmap.get_bit(i)) {
                graph_layers_builder.get_random_layer(&mut rng)
            } else {
                0
            };
            graph_layers_builder.set_levels(vector_id, level);
        }

        let parallelism = match std::thread::available_parallelism() {
            Ok(degree) => degree.get(),
            Err(e) => {
                error!(
                    "failed to detect the number of parallelism: {}, fallback to 8",
                    e
                );
                8
            }
        };

        let pool = ThreadPoolBuilder::new()
            .thread_name(|index| format!("hnsw-build-{}", index))
            .num_threads(parallelism)
            .build()
            .expect("failed to build hnsw build thread pool");

        let mut ids: Vec<usize> = TrueIdxIter::new(column.len(), validity.as_ref()).collect();
        let first_few_ids: Vec<usize> = ids
            .drain(..SINGLE_THREADED_HNSW_BUILD_THRESHOLD.min(ids.len()))
            .collect();

        let (data, dim) = vector_column.as_float32().unwrap();
        let data = unsafe { std::mem::transmute::<Buffer<F32>, Buffer<f32>>(data.clone()) };

        let stopped = AtomicBool::new(false);

        let mut index_fields = Vec::with_capacity(4);
        let mut index_columns = Vec::with_capacity(4);

        let insert_point = |vector_id| {
            check_process_stopped(&stopped)?;

            let raw_scorer = RawScorer::Original(OriginalRawScorer {
                distance_type,
                index: vector_id,
                column: &column,
            });
            let points_scorer = FilteredScorer::new(&raw_scorer);
            graph_layers_builder.link_new_point(vector_id as PointOffsetType, points_scorer);

            Ok::<_, ErrorCode>(())
        };

        for vector_id in first_few_ids {
            insert_point(vector_id)?;
        }
        if !ids.is_empty() {
            pool.install(|| ids.into_par_iter().try_for_each(insert_point))?;
        }

        let (graph_links, graph_data) =
            graph_layers_builder.into_graph_data(GraphLinksFormat::Compressed)?;

        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(graph_links),
            1,
        ));
        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(graph_data),
            1,
        ));

        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_GRAPH_LINKS_SUFFIX),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_GRAPH_DATA_SUFFIX),
            TableDataType::Binary,
        ));

        // Set invert parameter to query the closest vector (the most similar vector).
        // For Dot distances: invert = false (because a larger dot product means more similar)
        // For L1 distances: invert = true (because a smaller Manhattan distance means more similar)
        // For L2 distances: invert = true (because a smaller Euclidean distance means more similar)
        let invert = match distance_type {
            DistanceType::Dot => false,
            DistanceType::L1 | DistanceType::L2 => true,
        };

        let params = VectorParameters {
            dim: *dim,
            count: column.len(),
            distance_type,
            invert,
        };

        let builder = Vec::new();
        let encoded_vectors = EncodedVectorsU8::encode(
            (0..params.count).map(|i| &data.as_ref()[i * params.dim..(i + 1) * params.dim]),
            builder,
            &params,
            None,
            &stopped,
        )?;

        let encoded_meta = encoded_vectors.build_meta()?;
        let encoded_data = encoded_vectors.build_data()?;

        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(encoded_meta),
            1,
        ));
        index_columns.push(BlockEntry::new_const_column(
            DataType::Binary,
            Scalar::Binary(encoded_data),
            1,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_ENCODED_U8_META_SUFFIX),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &hnsw_component_name(column_id, distance_type, HNSW_ENCODED_U8_DATA_SUFFIX),
            TableDataType::Binary,
        ));

        Ok((index_fields, index_columns))
    }

    pub fn preprocess_query(distance_type: DistanceType, query_values: Vec<f32>) -> Vec<f32> {
        match distance_type {
            DistanceType::Dot => cosine_preprocess(query_values),
            DistanceType::L1 | DistanceType::L2 => query_values,
        }
    }

    fn postprocess_score(
        distance_type: DistanceType,
        mut values: Vec<ScoredPointOffset>,
    ) -> Vec<ScoredPointOffset> {
        match distance_type {
            DistanceType::L1 => {
                for value in &mut values {
                    value.score = value.score.abs();
                }
            }
            DistanceType::L2 => {
                for value in &mut values {
                    value.score = value.score.abs().sqrt();
                }
            }
            DistanceType::Dot => {
                for value in &mut values {
                    value.score = (1.0_f32 - value.score).abs();
                }
            }
        }
        values
    }
}

/// The first `limit` of `values`, plus the later ones whose score equals the
/// largest of those `limit` scores. `values` are in search order, which the
/// postprocessed scores may not follow exactly (`(1 - s).abs()` for Dot).
fn truncate_with_ties(mut values: Vec<ScoredPointOffset>, limit: usize) -> Vec<ScoredPointOffset> {
    let rest = values.split_off(limit.min(values.len()));
    let last = values
        .iter()
        .map(|value| value.score)
        .fold(f32::NEG_INFINITY, f32::max);
    values.extend(rest.into_iter().filter(|value| value.score == last));
    values
}

fn preprocess(distance_type: DistanceType, column: VectorColumn) -> VectorColumn {
    match distance_type {
        DistanceType::Dot => {
            let ty = column.data_type();
            let len = column.len();
            let mut builder = VectorColumnBuilder::with_capacity(&ty, len);
            for scalar in column.iter() {
                let val = scalar.as_float32().unwrap();
                let val = unsafe { std::mem::transmute::<Vec<F32>, Vec<f32>>(val.to_vec()) };

                let new_val = cosine_preprocess(val);
                let new_val = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(new_val) };
                let new_scalar = VectorScalar::Float32(new_val);
                builder.push(&new_scalar.as_ref());
            }
            builder.build()
        }
        DistanceType::L1 | DistanceType::L2 => column,
    }
}

fn is_length_zero_or_normalized(length: f32) -> bool {
    length < f32::EPSILON || (length - 1.0).abs() <= 1.0e-6
}

fn cosine_preprocess(vector: Vec<f32>) -> Vec<f32> {
    let mut length: f32 = vector.iter().map(|x| x * x).sum();
    if is_length_zero_or_normalized(length) {
        return vector;
    }
    length = length.sqrt();
    vector.iter().map(|x| x / length).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_with_ties() {
        let scores = [0.1, 0.3, 0.2, 0.3, 0.4, 0.3];
        let ids = |limit| {
            let values = scores
                .iter()
                .enumerate()
                .map(|(idx, &score)| ScoredPointOffset {
                    idx: idx as u32,
                    score,
                })
                .collect();
            truncate_with_ties(values, limit)
                .iter()
                .map(|value| value.idx)
                .collect::<Vec<_>>()
        };
        // The largest of the first scores is 0.3, also the score of rows 3 and 5.
        assert_eq!(ids(2), [0, 1, 3, 5]);
        assert_eq!(ids(3), [0, 1, 2, 3, 5]);
        assert_eq!(ids(1), [0]);
        assert_eq!(ids(6), [0, 1, 2, 3, 4, 5]);
        assert!(ids(0).is_empty());
    }

    /// Two rows of one index at the same nearest distance are both returned for
    /// limit 1.
    #[test]
    fn test_search_keeps_ties() -> Result<()> {
        // Rows 3 and 9 are the same vector, the nearest to the query; the others
        // are at least 2 away.
        let vectors: Vec<[f32; 4]> = (0..16)
            .map(|i| match i {
                3 | 9 => [1.0; 4],
                _ => [i as f32, -(i as f32), 2.0, 0.5 * i as f32],
            })
            .collect();
        let values: Vec<F32> = vectors.iter().flatten().map(|&v| F32::from(v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), 4)));
        let (_, entries) = HNSWIndex::build(4, 8, 0, column, DistanceType::L2)?;
        // Each component is a single-row binary column.
        let columns: Vec<_> = entries.iter().map(|entry| entry.to_column()).collect();
        let components: Vec<_> = columns
            .iter()
            .map(|column| column.as_binary().unwrap().index(0).unwrap())
            .collect();
        let index = HNSWIndex::open(DistanceType::L2, 4, vectors.len(), &components, None)?;

        // An ef of every row visits the whole graph, so the result is exact.
        let search = |limit, keep_ties| -> Result<Vec<u32>> {
            let rows = index.search_with_ef(limit, Some(vectors.len()), &[1.0; 4], keep_ties)?;
            assert!(rows.iter().all(|row| row.score == rows[0].score));
            let mut ids: Vec<_> = rows.iter().map(|row| row.idx).collect();
            ids.sort_unstable();
            Ok(ids)
        };
        assert_eq!(search(1, true)?, [3, 9]);
        assert_eq!(search(2, true)?, [3, 9]);
        // Without `keep_ties`, one of them.
        let one = search(1, false)?;
        assert!(one == [3] || one == [9], "{one:?}");
        assert_eq!(search(2, false)?, [3, 9]);
        Ok(())
    }
}
