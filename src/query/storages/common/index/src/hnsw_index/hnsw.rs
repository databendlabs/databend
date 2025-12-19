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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
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

pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

pub struct HNSWIndex {
    graph_layers: GraphLayers,
    distance_type: DistanceType,
    encoded_vectors: EncodedVectorsU8<Vec<u8>>,
}

impl HNSWIndex {
    pub fn open(
        distance_type: DistanceType,
        dim: usize,
        count: usize,
        binary_columns: Vec<Column>,
    ) -> Result<HNSWIndex> {
        let graph_links = unsafe { binary_columns[0].as_binary().unwrap().index_unchecked(0) };
        let graph_data = unsafe { binary_columns[1].as_binary().unwrap().index_unchecked(0) };
        let encoded_meta = unsafe { binary_columns[2].as_binary().unwrap().index_unchecked(0) };
        let encoded_data = unsafe { binary_columns[3].as_binary().unwrap().index_unchecked(0) };

        let graph_layers = GraphLayers::open(graph_links, graph_data)?;

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
            EncodedVectorsU8::load(encoded_data, encoded_meta, &params)?;

        Ok(Self {
            graph_layers,
            distance_type,
            encoded_vectors,
        })
    }

    pub fn search(&self, limit: usize, query_values: &[f32]) -> Result<Vec<ScoredPointOffset>> {
        let query_encode = self.encoded_vectors.encode_query(query_values);

        let raw_scorer = RawScorer::Quantized(QuantizedRawScorer {
            query: query_encode,
            vector: &self.encoded_vectors,
        });

        // ef is used to maintain the size of the set of candidate points in the search process,
        // the larger the search precision is higher, the smaller the speed is faster,
        // take 4 times the limit is a more balanced parameter.
        let ef = limit * 4;
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

        let mut values = Vec::with_capacity(row_nums as usize);
        for idx in 0..row_nums {
            let score = raw_scorer.score_point(idx);
            values.push(ScoredPointOffset { idx, score });
        }

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
            let level = graph_layers_builder.get_random_layer(&mut rng);
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

        let first_few_num = std::cmp::min(SINGLE_THREADED_HNSW_BUILD_THRESHOLD, column.len());
        let left_num = if column.len() > first_few_num {
            column.len() - first_few_num
        } else {
            0
        };

        let mut first_few_ids = Vec::with_capacity(first_few_num);
        let mut ids = Vec::with_capacity(left_num);
        for i in 0..first_few_num {
            first_few_ids.push(i);
        }
        for i in first_few_num..column.len() {
            ids.push(i);
        }

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

        // let graph_layers = graph_layers_builder.into_graph_layers_ram(GraphLinksFormat::Compressed);
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
            &format!("{}-{}_graph_links", column_id, distance_type),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &format!("{}-{}_graph_data", column_id, distance_type),
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
            &format!("{}-{}_encoded_u8_meta", column_id, distance_type),
            TableDataType::Binary,
        ));
        index_fields.push(TableField::new(
            &format!("{}-{}_encoded_u8_data", column_id, distance_type),
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
