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
use databend_common_expression::types::VectorScalar;
use databend_common_expression::types::F32;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use log::error;
use rand::thread_rng;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::utils::check_process_stopped;
use crate::hnsw_index::graph_layers::GraphLayers;
use crate::hnsw_index::graph_layers_builder::GraphLayersBuilder;
use crate::hnsw_index::graph_links::GraphLinksFormat;
use crate::hnsw_index::point_scorer::FilteredScorer;
use crate::hnsw_index::point_scorer::RawScorer;

pub const SINGLE_THREADED_HNSW_BUILD_THRESHOLD: usize = 256;

#[derive(Debug)]
pub struct HNSWIndex {
    graph_layer: GraphLayers,
}

impl HNSWIndex {
    pub fn open(graph_links: &[u8], graph_data: &[u8]) -> Result<Self> {
        let graph_layer = GraphLayers::open(graph_links, graph_data)?;

        Ok(Self { graph_layer })
    }

    pub fn search(&self, vals: Vec<f32>, column: Column) -> Result<()> {
        let top = 3;
        let ef_construct = 10;
        let values = unsafe { std::mem::transmute::<Vec<f32>, Vec<F32>>(vals) };
        let query_scorer = RawScorer::Scalar(Scalar::Vector(VectorScalar::Float32(values)));
        let query_filter_scorer = FilteredScorer::new(&query_scorer, &column);

        let is_stopped = AtomicBool::new(false);
        let res =
            self.graph_layer
                .search(top, ef_construct, query_filter_scorer, None, &is_stopped)?;
        println!("res={:?}", res);

        Ok(())
    }

    pub fn build(m: usize, ef_construct: usize, column: Column) -> Result<(Scalar, Scalar)> {
        let m0 = m * 2;
        let entry_points_num = 2;
        let use_heuristic = true;
        let num_vectors = column.len();

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

        let stopped = AtomicBool::new(false);
        let insert_point = |vector_id| {
            check_process_stopped(&stopped)?;

            let raw_scorer = RawScorer::Index(vector_id);
            let points_scorer = FilteredScorer::new(&raw_scorer, &column);
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

        Ok((Scalar::Binary(graph_links), Scalar::Binary(graph_data)))
    }
}
