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
use rand::Rng;
use rand::seq::SliceRandom;
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
        self.search_with_ef(limit, None, query_values)
    }

    pub fn search_with_ef(
        &self,
        limit: usize,
        search_ef: Option<usize>,
        query_values: &[f32],
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
        let values = self
            .graph_layers
            .search(limit, ef, query_filter_scorer, None, &is_stopped)?;

        let values = Self::postprocess_score(self.distance_type, values);
        Ok(values)
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
            .map(|nullable| nullable.validity.clone())
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

        let mut ids = insertion_order(column.len(), validity.as_ref(), &mut rng);
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

/// The valid rows in the order they are inserted into the graph. Rows may be laid
/// out by locality (vector clustering sorts them by cluster and FP16 page); inserted
/// in that order, the first points span a small region and later regions keep few
/// links to earlier ones, so searches miss neighbors in other regions. A random order
/// keeps the graph independent of the row order; point ids stay block rows.
fn insertion_order(rows: usize, validity: Option<&Bitmap>, rng: &mut impl Rng) -> Vec<usize> {
    let mut ids: Vec<usize> = TrueIdxIter::new(rows, validity).collect();
    ids.shuffle(rng);
    ids
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
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    #[test]
    fn test_insertion_order_shuffles_valid_rows() {
        let mut rng = StdRng::seed_from_u64(7);
        let validity = Bitmap::from_iter((0..1000).map(|row| row % 10 != 0));
        let order = insertion_order(1000, Some(&validity), &mut rng);
        // Every valid row once, no NULL row.
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(
            sorted,
            (0..1000).filter(|row| row % 10 != 0).collect::<Vec<_>>()
        );
        // Not the row order: the first points do not all come from the first rows.
        assert_ne!(order, sorted);
        assert!(order[..256].iter().any(|&row| row >= 500));
    }
}
