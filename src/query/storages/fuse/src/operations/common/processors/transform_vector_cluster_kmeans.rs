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
use databend_storages_common_index::normalize_vector;
use databend_storages_common_index::vector_distance;
use databend_storages_common_table_meta::meta::VectorDistanceType;

const KMEANS_MAX_ITER: usize = 100;
const KMEANS_SEED: u64 = 0xD47A_BA5E_C1A5_7E12;
const KMEANS_TOLERANCE: f32 = 1e-4;
// Kmeans is computed per bounded batch so the transform can stream instead of
// buffering the whole INSERT/RECLUSTER input. Cluster ids are batch-local.
const KMEANS_BATCH_CLUSTER_COUNT: usize = 64;
const KMEANS_MAX_BATCH_ROWS: usize = 262_144;

struct KMeansResult {
    assignments: Vec<usize>,
}

struct SmallRng {
    state: u64,
}

impl SmallRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }

    fn next_f32(&mut self) -> f32 {
        let value = self.next_u64() >> 40;
        (value as f32) / ((1u64 << 24) as f32)
    }

    fn gen_range(&mut self, upper: usize) -> usize {
        (self.next_u64() as usize) % upper
    }
}

pub struct TransformVectorClusterKmeans {
    vector_column_input_offset: usize,
    distance_type: VectorDistanceType,
    rows_per_block: usize,
    batch_rows: usize,
    pending_blocks: Vec<DataBlock>,
    pending_rows: usize,
}

impl TransformVectorClusterKmeans {
    pub fn new(
        vector_column_input_offset: usize,
        distance_type: VectorDistanceType,
        rows_per_block: usize,
    ) -> Self {
        let rows_per_block = rows_per_block.max(1);
        Self {
            vector_column_input_offset,
            distance_type,
            rows_per_block,
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
            append_cluster_id_column(&mut block, vec![0; num_rows]);
            return Ok(vec![block]);
        }

        let (samples, dimension) = vector_samples(
            block
                .get_by_offset(self.vector_column_input_offset)
                .to_column(),
            self.distance_type,
        )?;
        let k = num_rows.div_ceil(self.rows_per_block).clamp(1, num_rows);
        if k <= 1 {
            append_cluster_id_column(&mut block, vec![0; num_rows]);
            return Ok(vec![block]);
        }

        let result = kmeans(
            &samples,
            num_rows,
            dimension,
            k,
            KMEANS_MAX_ITER,
            KMEANS_TOLERANCE,
            KMEANS_SEED,
            self.distance_type,
        )?;

        append_cluster_id_column(&mut block, result.assignments);
        Ok(vec![block])
    }

    fn push_block(&mut self, data: DataBlock, output: &mut Vec<DataBlock>) -> Result<()> {
        let rows = data.num_rows();
        if rows == 0 {
            let mut data = data;
            append_cluster_id_column(&mut data, vec![]);
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

impl AccumulatingTransform for TransformVectorClusterKmeans {
    const NAME: &'static str = "TransformVectorClusterKmeans";

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

fn vector_samples(column: Column, distance_type: VectorDistanceType) -> Result<(Vec<f32>, usize)> {
    let column = column.remove_nullable();
    let Column::Vector(VectorColumn::Float32((values, dimension))) = column else {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key only supports float32 vector column",
        ));
    };
    if dimension == 0 || values.len() % dimension != 0 {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key has invalid vector dimension",
        ));
    }

    let mut samples = values.iter().map(|value| value.0).collect::<Vec<_>>();
    if matches!(&distance_type, VectorDistanceType::Dot) {
        for vector in samples.chunks_exact_mut(dimension) {
            normalize_vector(vector);
        }
    }

    Ok((samples, dimension))
}

fn append_cluster_id_column(block: &mut DataBlock, assignments: Vec<usize>) {
    debug_assert_eq!(block.num_rows(), assignments.len());
    let cluster_ids = assignments
        .into_iter()
        .map(|cluster_id| cluster_id as u64)
        .collect::<Vec<_>>();
    block.add_column(UInt64Type::from_data(cluster_ids));
}

fn kmeans(
    data: &[f32],
    rows: usize,
    dim: usize,
    k: usize,
    max_iter: usize,
    tolerance: f32,
    seed: u64,
    distance_type: VectorDistanceType,
) -> Result<KMeansResult> {
    if rows == 0 || dim == 0 || k == 0 || k > rows || data.len() != rows * dim {
        return Err(ErrorCode::InvalidClusterKeys(
            "Invalid kmeans input for vector cluster key",
        ));
    }

    let mut rng = SmallRng::new(seed ^ 0x9e3779b97f4a7c15);
    let mut centroids = init_kmeans_plus_plus(data, rows, dim, k, seed, distance_type)?;
    let mut assignments = vec![usize::MAX; rows];
    let mut distances = vec![0.0; rows];

    for _ in 0..max_iter {
        let mut changed = false;
        for row_idx in 0..rows {
            let (cluster, distance) =
                nearest_centroid(row(data, row_idx, dim), &centroids, k, dim, distance_type)?;
            if assignments[row_idx] != cluster {
                changed = true;
                assignments[row_idx] = cluster;
            }
            distances[row_idx] = distance;
        }

        let mut next_centroids = vec![0.0; k * dim];
        let mut counts = vec![0usize; k];
        for (row_idx, cluster) in assignments.iter().enumerate().take(rows) {
            counts[*cluster] += 1;
            let point = row(data, row_idx, dim);
            let centroid = row_mut(&mut next_centroids, *cluster, dim);
            for (dst, src) in centroid.iter_mut().zip(point.iter()) {
                *dst += *src;
            }
        }

        for (cluster, count) in counts.iter().enumerate().take(k) {
            if *count == 0 {
                let farthest = distances
                    .iter()
                    .enumerate()
                    .max_by(|(_, left), (_, right)| {
                        left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(idx, _)| idx)
                    .unwrap_or_else(|| rng.gen_range(rows));
                row_mut(&mut next_centroids, cluster, dim)
                    .copy_from_slice(row(data, farthest, dim));
                continue;
            }

            let inv_count = 1.0 / counts[cluster] as f32;
            for value in row_mut(&mut next_centroids, cluster, dim) {
                *value *= inv_count;
            }
            if matches!(&distance_type, VectorDistanceType::Dot) {
                normalize_vector(row_mut(&mut next_centroids, cluster, dim));
            }
        }

        let mut shift = 0.0;
        for cluster in 0..k {
            shift += vector_distance(
                row(&centroids, cluster, dim),
                row(&next_centroids, cluster, dim),
                VectorDistanceType::L2,
            )?;
        }
        centroids = next_centroids;

        if !changed || shift <= tolerance {
            break;
        }
    }

    Ok(KMeansResult { assignments })
}

fn init_kmeans_plus_plus(
    data: &[f32],
    rows: usize,
    dim: usize,
    k: usize,
    seed: u64,
    distance_type: VectorDistanceType,
) -> Result<Vec<f32>> {
    let mut rng = SmallRng::new(seed);
    let mut centroids = vec![0.0; k * dim];
    let first = rng.gen_range(rows);
    row_mut(&mut centroids, 0, dim).copy_from_slice(row(data, first, dim));

    let mut min_distances = vec![f32::INFINITY; rows];
    for cluster in 1..k {
        let last_centroid = row(&centroids, cluster - 1, dim);
        let mut total = 0.0;
        for (row_idx, min_distance) in min_distances.iter_mut().enumerate().take(rows) {
            let distance = vector_distance(row(data, row_idx, dim), last_centroid, distance_type)?;
            if distance < *min_distance {
                *min_distance = distance;
            }
            total += *min_distance;
        }

        let chosen = if total <= f32::EPSILON || !total.is_finite() {
            rng.gen_range(rows)
        } else {
            let mut threshold = rng.next_f32() * total;
            let mut picked = rows - 1;
            for (idx, distance) in min_distances.iter().enumerate() {
                threshold -= *distance;
                if threshold <= 0.0 {
                    picked = idx;
                    break;
                }
            }
            picked
        };
        row_mut(&mut centroids, cluster, dim).copy_from_slice(row(data, chosen, dim));
    }

    Ok(centroids)
}

fn nearest_centroid(
    point: &[f32],
    centroids: &[f32],
    k: usize,
    dim: usize,
    distance_type: VectorDistanceType,
) -> Result<(usize, f32)> {
    let mut best_idx = 0;
    let mut best_dist = f32::INFINITY;
    for cluster in 0..k {
        let distance = vector_distance(point, row(centroids, cluster, dim), distance_type)?;
        if distance < best_dist {
            best_idx = cluster;
            best_dist = distance;
        }
    }
    Ok((best_idx, best_dist))
}

fn row(data: &[f32], idx: usize, dim: usize) -> &[f32] {
    &data[idx * dim..(idx + 1) * dim]
}

fn row_mut(data: &mut [f32], idx: usize, dim: usize) -> &mut [f32] {
    &mut data[idx * dim..(idx + 1) * dim]
}
