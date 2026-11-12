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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VectorColumn;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_storages_common_index::KMeans;
use databend_storages_common_index::normalize_vector;
use databend_storages_common_table_meta::meta::VectorDistanceType;

// Kmeans is computed per bounded batch so the transform can stream instead of
// buffering the whole INSERT/RECLUSTER input. Cluster ids are batch-local.
const KMEANS_BATCH_CLUSTER_COUNT: usize = 64;
const KMEANS_MAX_BATCH_ROWS: usize = 262_144;
// Sort by cluster first, then by distance to the assigned centroid.  The low
// distance bits are quantized because they only need to provide a stable
// within-cluster order before fixed-size block splitting.
const VECTOR_CLUSTER_ID_BITS: u32 = 16;
const VECTOR_CLUSTER_DISTANCE_BITS: u32 = 64 - VECTOR_CLUSTER_ID_BITS;
const VECTOR_CLUSTER_DISTANCE_MASK: u64 = (1_u64 << VECTOR_CLUSTER_DISTANCE_BITS) - 1;
const VECTOR_CLUSTER_DISTANCE_SCALE: f64 = 1_000_000.0;

pub struct TransformVectorCluster {
    vector_column_input_offset: usize,
    dimension: usize,
    distance_type: VectorDistanceType,
    kmeans: KMeans,
    batch_rows: usize,
    pending_blocks: Vec<DataBlock>,
    pending_rows: usize,
}

impl TransformVectorCluster {
    pub fn new(
        vector_column_input_offset: usize,
        dimension: usize,
        distance_type: VectorDistanceType,
        rows_per_block: usize,
    ) -> Self {
        let rows_per_block = rows_per_block.max(1);
        Self {
            vector_column_input_offset,
            dimension,
            distance_type,
            kmeans: KMeans::new(dimension, rows_per_block, distance_type),
            batch_rows: kmeans_batch_rows(rows_per_block),
            pending_blocks: vec![],
            pending_rows: 0,
        }
    }

    fn cluster_blocks(&mut self) -> Result<Vec<DataBlock>> {
        if self.pending_blocks.is_empty() {
            return Ok(vec![]);
        }

        let mut block = if self.pending_blocks.len() == 1 {
            self.pending_rows = 0;
            self.pending_blocks.pop().unwrap()
        } else {
            let blocks = std::mem::take(&mut self.pending_blocks);
            self.pending_rows = 0;
            DataBlock::concat(&blocks)?
        };
        let num_rows = block.num_rows();
        if num_rows <= 1 {
            append_vector_sort_key_column(&mut block, vec![0; num_rows], vec![0.0; num_rows]);
            return Ok(vec![block]);
        }

        let samples = vector_samples(
            block
                .get_by_offset(self.vector_column_input_offset)
                .to_column(),
            self.dimension,
            self.distance_type,
        )?;

        let result = self.kmeans.compute(&samples)?;
        append_vector_sort_key_column(&mut block, result.assignments, result.distances_to_centroid);
        Ok(vec![block])
    }

    fn push_block(&mut self, data: DataBlock, output: &mut Vec<DataBlock>) -> Result<()> {
        let rows = data.num_rows();
        if rows == 0 {
            let mut data = data;
            append_vector_sort_key_column(&mut data, vec![], vec![]);
            output.push(data);
            return Ok(());
        }

        if rows > self.batch_rows {
            output.extend(self.cluster_blocks()?);
            let (blocks, remain) = data.split_by_rows(self.batch_rows);
            for block in blocks {
                self.pending_rows = block.num_rows();
                self.pending_blocks.push(block);
                output.extend(self.cluster_blocks()?);
            }
            if let Some(remain) = remain {
                self.pending_rows = remain.num_rows();
                self.pending_blocks.push(remain);
            }
            return Ok(());
        }

        if self.pending_rows > 0 && self.pending_rows.saturating_add(rows) > self.batch_rows {
            output.extend(self.cluster_blocks()?);
        }

        self.pending_rows += rows;
        self.pending_blocks.push(data);
        if self.pending_rows >= self.batch_rows {
            output.extend(self.cluster_blocks()?);
        }

        Ok(())
    }
}

impl AccumulatingTransform for TransformVectorCluster {
    const NAME: &'static str = "TransformVectorCluster";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let mut output = Vec::new();
        self.push_block(data, &mut output)?;
        Ok(output)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            self.cluster_blocks()
        } else {
            self.pending_blocks.clear();
            self.pending_rows = 0;
            Ok(vec![])
        }
    }
}

fn kmeans_batch_rows(rows_per_block: usize) -> usize {
    rows_per_block
        .saturating_mul(KMEANS_BATCH_CLUSTER_COUNT)
        .min(KMEANS_MAX_BATCH_ROWS)
        .max(rows_per_block)
        .max(1)
}

fn vector_samples(
    column: Column,
    expected_dimension: usize,
    distance_type: VectorDistanceType,
) -> Result<Vec<f32>> {
    let column = column.remove_nullable();
    let Column::Vector(VectorColumn::Float32((values, dimension))) = column else {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key only supports float32 vector column",
        ));
    };
    if expected_dimension == 0
        || dimension != expected_dimension
        || values.len() % expected_dimension != 0
    {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key has invalid vector dimension",
        ));
    }

    let mut samples = values.iter().map(|value| value.0).collect::<Vec<_>>();
    if matches!(&distance_type, VectorDistanceType::Dot) {
        for vector in samples.chunks_exact_mut(expected_dimension) {
            normalize_vector(vector);
        }
    }

    Ok(samples)
}

fn append_vector_sort_key_column(
    block: &mut DataBlock,
    assignments: Vec<usize>,
    distances_to_centroid: Vec<f32>,
) {
    debug_assert_eq!(block.num_rows(), assignments.len());
    debug_assert_eq!(block.num_rows(), distances_to_centroid.len());
    let sort_keys = assignments
        .into_iter()
        .zip(distances_to_centroid)
        .map(|(cluster_id, distance)| pack_vector_cluster_sort_key(cluster_id, distance))
        .collect::<Vec<_>>();
    block.add_column(UInt64Type::from_data(sort_keys));
}

fn pack_vector_cluster_sort_key(cluster_id: usize, distance_to_centroid: f32) -> u64 {
    debug_assert!(cluster_id < (1_usize << VECTOR_CLUSTER_ID_BITS));
    let cluster_part = (cluster_id as u64) << VECTOR_CLUSTER_DISTANCE_BITS;
    let distance = if distance_to_centroid.is_finite() {
        distance_to_centroid.max(0.0) as f64
    } else {
        f64::INFINITY
    };
    let distance_part = (distance * VECTOR_CLUSTER_DISTANCE_SCALE)
        .min(VECTOR_CLUSTER_DISTANCE_MASK as f64)
        .max(0.0) as u64;
    cluster_part | distance_part
}
