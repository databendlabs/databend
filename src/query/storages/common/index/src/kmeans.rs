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
use databend_storages_common_table_meta::meta::VectorDistanceType;
use log::debug;

use crate::vector::VectorDistanceKernel;
use crate::vector::normalize_vector;

pub struct KMeansResult {
    pub assignments: Vec<usize>,
    pub distances_to_centroid: Vec<f32>,
}

const DEFAULT_KMEANS_MAX_ITER: usize = 100;
const DEFAULT_KMEANS_SEED: u64 = 0xD47A_BA5E_C1A5_7E12;
const DEFAULT_KMEANS_TOLERANCE: f32 = 1e-4;

const LCG_MULTIPLIER: u64 = 6364136223846793005;
const LCG_INCREMENT: u64 = 1442695040888963407;

// A small deterministic RNG used only for kmeans initialization. The constants
// are the PCG-style 64-bit LCG state transition parameters; this intentionally
// avoids adding a dependency for non-cryptographic, reproducible sampling.
struct SmallRng {
    state: u64,
}

pub struct KMeans {
    dim: usize,
    rows_per_cluster: usize,
    max_iter: usize,
    tolerance: f32,
    seed: u64,
    distance_kernel: KMeansDistanceKernel,
}

#[derive(Clone, Copy)]
struct KMeansDistanceKernel {
    distance_type: VectorDistanceType,
    vector_kernel: VectorDistanceKernel,
}

impl SmallRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(LCG_MULTIPLIER)
            .wrapping_add(LCG_INCREMENT);
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

impl KMeans {
    pub fn new(dim: usize, rows_per_cluster: usize, distance_type: VectorDistanceType) -> Self {
        debug_assert!(dim > 0);
        debug_assert!(rows_per_cluster > 0);
        Self {
            dim,
            rows_per_cluster,
            max_iter: DEFAULT_KMEANS_MAX_ITER,
            tolerance: DEFAULT_KMEANS_TOLERANCE,
            seed: DEFAULT_KMEANS_SEED,
            distance_kernel: KMeansDistanceKernel::new(dim, distance_type),
        }
    }

    pub fn compute(&self, data: &[f32]) -> Result<KMeansResult> {
        let rows = self.rows(data);
        if rows == 0 || data.len() != rows * self.dim {
            return Err(ErrorCode::InvalidClusterKeys(
                "Invalid kmeans input for vector cluster key",
            ));
        }

        let k = rows.div_ceil(self.rows_per_cluster).clamp(1, rows);
        if k <= 1 {
            debug!(
                "vector kmeans clustered rows={}, dim={}, k=1, iterations=0, objective=0, sizes={:?}, max_radius_per_cluster={:?}",
                rows,
                self.dim,
                vec![rows],
                vec![0.0_f32],
            );
            return Ok(KMeansResult {
                assignments: vec![0; rows],
                distances_to_centroid: vec![0.0; rows],
            });
        }

        self.compute_kmeans(data, rows, k)
    }

    fn compute_kmeans(&self, data: &[f32], rows: usize, k: usize) -> Result<KMeansResult> {
        let mut centroids = self.choose_initial_centroids(data, rows, k);
        let mut assignments = vec![usize::MAX; rows];
        let mut rng = SmallRng::new(self.seed ^ 0x9e3779b97f4a7c15);
        let mut distances = vec![0.0; rows];
        let mut iterations = 0;

        for _ in 0..self.max_iter {
            iterations += 1;
            let mut changed = false;
            for row_idx in 0..rows {
                let (cluster, distance) =
                    self.nearest_centroid(self.row(data, row_idx), &centroids);
                if assignments[row_idx] != cluster {
                    changed = true;
                    assignments[row_idx] = cluster;
                }
                distances[row_idx] = distance;
            }

            let mut next_centroids = vec![0.0; k * self.dim];
            let mut counts = vec![0usize; k];
            for (row_idx, cluster) in assignments.iter().enumerate().take(rows) {
                counts[*cluster] += 1;
                let point = self.row(data, row_idx);
                let centroid = self.row_mut(&mut next_centroids, *cluster);
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
                    self.row_mut(&mut next_centroids, cluster)
                        .copy_from_slice(self.row(data, farthest));
                    continue;
                }

                let inv_count = 1.0 / counts[cluster] as f32;
                for value in self.row_mut(&mut next_centroids, cluster) {
                    *value *= inv_count;
                }
                self.distance_kernel
                    .normalize_centroid(self.row_mut(&mut next_centroids, cluster));
            }

            let mut shift = 0.0;
            for cluster in 0..k {
                shift += self.distance_kernel.centroid_shift(
                    self.row(&centroids, cluster),
                    self.row(&next_centroids, cluster),
                );
            }
            centroids = next_centroids;

            if !changed || shift <= self.tolerance {
                break;
            }
        }

        self.build_result(data, rows, k, centroids, assignments, iterations)
    }

    fn build_result(
        &self,
        data: &[f32],
        rows: usize,
        k: usize,
        centroids: Vec<f32>,
        assignments: Vec<usize>,
        iterations: usize,
    ) -> Result<KMeansResult> {
        let mut sizes = vec![0usize; k];
        let mut objective = 0.0_f64;
        let mut max_radius_per_cluster = vec![0.0_f32; k];
        let mut distances_to_centroid = Vec::with_capacity(rows);

        // Measure the clustering that will actually be emitted as cluster ids.
        for (row_idx, cluster) in assignments.iter().copied().enumerate().take(rows) {
            if cluster >= k {
                return Err(ErrorCode::InvalidClusterKeys(
                    "Invalid kmeans cluster assignment for vector cluster key",
                ));
            }
            let point = self.row(data, row_idx);
            let centroid = self.row(&centroids, cluster);
            let distance = self.distance_kernel.distance(point, centroid);
            distances_to_centroid.push(distance);
            sizes[cluster] += 1;
            objective += distance as f64;

            let radius = self.distance_kernel.stat_distance(point, centroid);
            max_radius_per_cluster[cluster] = max_radius_per_cluster[cluster].max(radius);
        }

        debug!(
            "vector kmeans clustered rows={}, dim={}, k={}, iterations={}, objective={}, sizes={:?}, max_radius_per_cluster={:?}",
            rows, self.dim, k, iterations, objective, sizes, max_radius_per_cluster,
        );

        Ok(KMeansResult {
            assignments,
            distances_to_centroid,
        })
    }

    // Choose initial centroids with kmeans++ weighted sampling.
    fn choose_initial_centroids(&self, data: &[f32], rows: usize, k: usize) -> Vec<f32> {
        let mut rng = SmallRng::new(self.seed);
        let mut centroids = vec![0.0; k * self.dim];
        let first = rng.gen_range(rows);
        self.row_mut(&mut centroids, 0)
            .copy_from_slice(self.row(data, first));

        let mut min_distances = vec![f32::INFINITY; rows];
        for cluster in 1..k {
            let last_centroid = self.row(&centroids, cluster - 1);
            let mut total = 0.0;
            for (row_idx, min_distance) in min_distances.iter_mut().enumerate().take(rows) {
                let distance = self
                    .distance_kernel
                    .compare(self.row(data, row_idx), last_centroid);
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
            self.row_mut(&mut centroids, cluster)
                .copy_from_slice(self.row(data, chosen));
        }

        centroids
    }

    fn nearest_centroid(&self, point: &[f32], centroids: &[f32]) -> (usize, f32) {
        let mut best_idx = 0;
        let mut best_dist = f32::INFINITY;
        for cluster in 0..centroids.len() / self.dim {
            let distance = self
                .distance_kernel
                .compare(point, self.row(centroids, cluster));
            if distance < best_dist {
                best_idx = cluster;
                best_dist = distance;
            }
        }
        (best_idx, best_dist)
    }

    fn rows(&self, data: &[f32]) -> usize {
        if self.dim == 0 {
            return 0;
        }

        data.len() / self.dim
    }

    fn row<'a>(&self, data: &'a [f32], idx: usize) -> &'a [f32] {
        &data[idx * self.dim..(idx + 1) * self.dim]
    }

    fn row_mut<'a>(&self, data: &'a mut [f32], idx: usize) -> &'a mut [f32] {
        &mut data[idx * self.dim..(idx + 1) * self.dim]
    }
}

impl KMeansDistanceKernel {
    fn new(dim: usize, distance_type: VectorDistanceType) -> Self {
        Self {
            distance_type,
            vector_kernel: VectorDistanceKernel::new(dim),
        }
    }

    #[inline]
    fn compare(&self, point: &[f32], centroid: &[f32]) -> f32 {
        match self.distance_type {
            VectorDistanceType::L1 => self.vector_kernel.l1(point, centroid),
            // Kmeans only needs ordering for nearest-centroid selection.
            // Comparing squared L2 distances avoids a sqrt in the hottest loop.
            VectorDistanceType::L2 => self.vector_kernel.l2_squared(point, centroid),
            // Dot vectors are normalized before clustering, so cosine distance
            // is equivalent to 1 - dot(point, centroid).
            VectorDistanceType::Dot => {
                normalize_dot_distance(self.vector_kernel.dot(point, centroid))
            }
        }
    }

    #[inline]
    fn distance(&self, point: &[f32], centroid: &[f32]) -> f32 {
        match self.distance_type {
            VectorDistanceType::L1 => self.vector_kernel.l1(point, centroid),
            VectorDistanceType::L2 => self.vector_kernel.l2_squared(point, centroid).sqrt(),
            VectorDistanceType::Dot => {
                normalize_dot_distance(self.vector_kernel.dot(point, centroid))
            }
        }
    }

    #[inline]
    fn stat_distance(&self, point: &[f32], centroid: &[f32]) -> f32 {
        if matches!(self.distance_type, VectorDistanceType::Dot) {
            return (1.0 - self.distance(point, centroid))
                .clamp(-1.0, 1.0)
                .acos();
        }

        self.distance(point, centroid)
    }

    #[inline]
    fn centroid_shift(&self, left: &[f32], right: &[f32]) -> f32 {
        self.vector_kernel.l2_squared(left, right).sqrt()
    }

    #[inline]
    fn normalize_centroid(&self, centroid: &mut [f32]) {
        if matches!(self.distance_type, VectorDistanceType::Dot) {
            normalize_vector(centroid);
        }
    }
}

fn normalize_dot_distance(dot: f32) -> f32 {
    let distance = 1.0 - dot;
    if distance.is_finite() {
        distance.max(0.0)
    } else {
        1.0
    }
}
