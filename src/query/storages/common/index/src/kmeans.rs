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

//! k-means for vector cluster keys, following the defaults of faiss `Clustering`:
//! random initialization, 25 Lloyd iterations, subsampling to 256 training points
//! per centroid, and splitting large clusters to fill empty ones.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use log::debug;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::index;

use crate::vector::VectorDistanceKernel;
use crate::vector::normalize_vector;

/// Lloyd iterations (faiss `niter`).
const NITER: usize = 25;
/// Training points per centroid beyond which the input is subsampled (faiss
/// `max_points_per_centroid`).
const MAX_POINTS_PER_CENTROID: usize = 256;
/// Fewer training points per centroid make training unreliable; faiss only warns
/// (faiss `min_points_per_centroid`).
const MIN_POINTS_PER_CENTROID: usize = 39;
/// Random seed (faiss `seed`), so that the same input gives the same layout.
const SEED: u64 = 1234;
/// Relative perturbation of the two centroids of a split cluster (faiss `EPS`).
const SPLIT_EPS: f32 = 1.0 / 1024.0;

/// How the initial centroids are chosen.
#[derive(Clone, Copy, Debug)]
pub enum KMeansInit<'a> {
    /// `k` random training points, as in faiss.
    Random,
    /// Weighted summaries of the data, such as block centroids weighted by row
    /// count, which may also cover data outside the training points. Up to `k`
    /// seeds are drawn by weight and the rest are random training points. A seeded
    /// centroid that attracts no training point keeps its position instead of being
    /// replaced by a split, since its data may not have been sampled.
    Seeds {
        centroids: &'a [f32],
        weights: &'a [u64],
    },
}

/// Trains k-means models over prepared vectors of one dimension and distance.
#[derive(Clone, Copy)]
pub struct KMeans {
    dim: usize,
    distance: VectorDistanceType,
    kernel: VectorDistanceKernel,
}

/// Trained centroids, which assign each prepared vector to its nearest centroid.
pub struct KMeansModel {
    kmeans: KMeans,
    centroids: Vec<f32>,
}

impl KMeans {
    pub fn new(dim: usize, distance: VectorDistanceType) -> Self {
        debug_assert!(dim > 0);
        Self {
            dim,
            distance,
            kernel: VectorDistanceKernel::new(dim),
        }
    }

    /// Prepares `point` in place for training or assignment; cosine vectors are
    /// normalized. Returns `false` for points that cannot be clustered: non-finite
    /// values, or a zero vector for cosine.
    pub fn prepare(&self, point: &mut [f32]) -> bool {
        if !point.iter().all(|value| value.is_finite()) {
            return false;
        }
        if matches!(self.distance, VectorDistanceType::Dot) {
            // Finite f32 values cannot overflow the f64 norm.
            let norm = point
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                .sqrt();
            if norm == 0.0 {
                return false;
            }
            for value in point.iter_mut() {
                *value = (f64::from(*value) / norm) as f32;
            }
        }
        true
    }

    /// Trains up to `k` centroids on `n` points, where `point(i, out)` writes the
    /// `i`-th prepared point. `k` is reduced to the number of points and seeds.
    ///
    /// # Errors
    ///
    /// Returns an error when there are neither points nor seeds, or when the seeds
    /// and their weights differ in length.
    pub fn train(
        &self,
        n: usize,
        k: usize,
        init: KMeansInit<'_>,
        mut point: impl FnMut(usize, &mut [f32]),
    ) -> Result<KMeansModel> {
        let dim = self.dim;
        let mut rng = StdRng::seed_from_u64(SEED);
        let mut centroids = match init {
            KMeansInit::Random => Vec::new(),
            KMeansInit::Seeds { centroids, weights } => {
                self.choose_seeds(centroids, weights, k, &mut rng)?
            }
        };
        let seeded = centroids.len() / dim;
        if seeded + n == 0 {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector kmeans requires training points",
            ));
        }
        let k = k.clamp(1, seeded + n);

        let sample_size = k.saturating_mul(MAX_POINTS_PER_CENTROID);
        let mut sample: Vec<usize> = if n > sample_size {
            index::sample(&mut rng, n, sample_size).into_vec()
        } else {
            (0..n).collect()
        };
        sample.sort_unstable();
        let mut points = vec![0.0; sample.len() * dim];
        for (out, &i) in points.chunks_exact_mut(dim).zip(&sample) {
            point(i, out);
        }
        let n = sample.len();
        if n < k.saturating_mul(MIN_POINTS_PER_CENTROID) {
            debug!(
                "vector kmeans has {n} training points for {k} centroids, fewer than {MIN_POINTS_PER_CENTROID} per centroid"
            );
        }

        // As in faiss, the initial centroids (besides seeds) are random training points.
        for i in index::sample(&mut rng, n, k - seeded) {
            centroids.extend_from_slice(&points[i * dim..(i + 1) * dim]);
        }

        let mut assignment = vec![usize::MAX; n];
        let mut sums = vec![0.0_f64; k * dim];
        let mut counts = vec![0.0_f64; k];
        let mut iterations = 0;
        for _ in 0..NITER {
            iterations += 1;
            sums.fill(0.0);
            counts.fill(0.0);
            let mut changed = false;
            for (p, assigned) in points.chunks_exact(dim).zip(&mut assignment) {
                let (cluster, _) = self.nearest(p, &centroids);
                changed |= *assigned != cluster;
                *assigned = cluster;
                counts[cluster] += 1.0;
                for (sum, value) in sums[cluster * dim..(cluster + 1) * dim].iter_mut().zip(p) {
                    *sum += f64::from(*value);
                }
            }
            // Empty clusters keep their centroid here: seeds stay, others are split.
            for ((centroid, sum), count) in centroids
                .chunks_exact_mut(dim)
                .zip(sums.chunks_exact(dim))
                .zip(&counts)
            {
                if *count > 0.0 {
                    for (value, sum) in centroid.iter_mut().zip(sum) {
                        *value = (sum / count) as f32;
                    }
                }
            }
            let splits = split_clusters(&mut centroids, &mut counts, dim, n, seeded, &mut rng);
            if matches!(self.distance, VectorDistanceType::Dot) {
                // faiss `spherical`: keep centroids on the unit sphere.
                centroids.chunks_exact_mut(dim).for_each(normalize_vector);
            }
            // The same assignment gives the same centroids, so later iterations would
            // not change them.
            if !changed && splits == 0 {
                break;
            }
        }
        debug!(
            "vector kmeans trained points={n} k={k} seeded={seeded} iterations={iterations} sizes={counts:?}"
        );
        Ok(KMeansModel {
            kmeans: *self,
            centroids,
        })
    }

    /// Draws up to `k` prepared seeds by weight without replacement, using
    /// Efraimidis-Spirakis keys `u^(1/w)`. Seeds that cannot be clustered are skipped.
    fn choose_seeds(
        &self,
        centroids: &[f32],
        weights: &[u64],
        k: usize,
        rng: &mut StdRng,
    ) -> Result<Vec<f32>> {
        if centroids.len() != weights.len() * self.dim {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector kmeans seeds do not match their weights",
            ));
        }
        let mut keyed = Vec::with_capacity(weights.len());
        for (centroid, &weight) in centroids.chunks_exact(self.dim).zip(weights) {
            let mut seed = centroid.to_vec();
            if weight > 0 && self.prepare(&mut seed) {
                let key = rng.r#gen::<f64>().powf(1.0 / weight as f64);
                keyed.push((key, seed));
            }
        }
        keyed.sort_by(|left, right| right.0.total_cmp(&left.0));
        Ok(keyed
            .into_iter()
            .take(k)
            .flat_map(|(_, seed)| seed)
            .collect())
    }

    /// Orders `n` prepared points into leaves of `leaf_rows` points (the last may be
    /// smaller) so that nearby points share a leaf, by bisecting 2-means: a node of
    /// `m` points has `l = ceil(m / leaf_rows)` leaves, and its first
    /// `floor(l / 2) * leaf_rows` points by the difference of their distances to the
    /// two centroids form the left node. Returns the point at each position; the
    /// points of leaf `j` take positions `j * leaf_rows..`, so leaves align with pages
    /// of `leaf_rows` rows starting at position 0.
    pub fn page_order(
        &self,
        n: usize,
        leaf_rows: usize,
        mut point: impl FnMut(usize, &mut [f32]),
    ) -> Result<Vec<usize>> {
        let dim = self.dim;
        let leaf_rows = leaf_rows.max(1);
        let mut points = vec![0.0; n * dim];
        for (i, out) in points.chunks_exact_mut(dim).enumerate() {
            point(i, out);
        }
        let points = &points;
        let mut order: Vec<usize> = (0..n).collect();
        // A stack of position ranges, starting with all points.
        let mut nodes: Vec<std::ops::Range<usize>> = std::iter::once(0..n).collect();
        let mut scored = Vec::new();
        while let Some(range) = nodes.pop() {
            let rows = range.len();
            if rows <= leaf_rows {
                continue;
            }
            let node = &mut order[range.clone()];
            let model = self.train(rows, 2, KMeansInit::Random, |i, out| {
                let p = node[i];
                out.copy_from_slice(&points[p * dim..(p + 1) * dim]);
            })?;
            if model.k() == 2 {
                let (left, right) = model.centroids.split_at(dim);
                scored.clear();
                scored.extend(node.iter().map(|&p| {
                    let x = &points[p * dim..(p + 1) * dim];
                    (self.compare(x, left) - self.compare(x, right), p)
                }));
                scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                for (slot, (_, p)) in node.iter_mut().zip(&scored) {
                    *slot = *p;
                }
            }
            let mid = range.start + rows.div_ceil(leaf_rows) / 2 * leaf_rows;
            nodes.push(range.start..mid);
            nodes.push(mid..range.end);
        }
        Ok(order)
    }

    fn nearest(&self, point: &[f32], centroids: &[f32]) -> (usize, f32) {
        let mut best = (0, f32::INFINITY);
        for (cluster, centroid) in centroids.chunks_exact(self.dim).enumerate() {
            let distance = self.compare(point, centroid);
            if distance < best.1 {
                best = (cluster, distance);
            }
        }
        best
    }

    /// Orders centroids by distance: squared for L2, and `-dot` for normalized
    /// cosine vectors.
    fn compare(&self, point: &[f32], centroid: &[f32]) -> f32 {
        match self.distance {
            VectorDistanceType::L1 => self.kernel.l1(point, centroid),
            VectorDistanceType::L2 => self.kernel.l2_squared(point, centroid),
            VectorDistanceType::Dot => -self.kernel.dot(point, centroid),
        }
    }
}

impl KMeansModel {
    pub fn k(&self) -> usize {
        self.centroids.len() / self.kmeans.dim
    }

    /// Returns the nearest centroid of a prepared point, and the distance to it.
    pub fn assign(&self, point: &[f32]) -> (usize, f32) {
        let (cluster, compare) = self.kmeans.nearest(point, &self.centroids);
        (cluster, self.distance(compare))
    }

    /// Like `assign`, but only to clusters holding fewer than `capacity` points, as the
    /// balanced assignment of knowhere does: a point goes to the nearest cluster that is
    /// not full. `counts` holds the points assigned so far and is updated. When every
    /// cluster is full the nearest one is used.
    pub fn assign_balanced(
        &self,
        point: &[f32],
        counts: &mut [u64],
        capacity: u64,
    ) -> (usize, f32) {
        debug_assert_eq!(counts.len(), self.k());
        let mut nearest = (0, f32::INFINITY);
        let mut open = (usize::MAX, f32::INFINITY);
        for (cluster, centroid) in self.centroids.chunks_exact(self.kmeans.dim).enumerate() {
            let compare = self.kmeans.compare(point, centroid);
            if compare < nearest.1 {
                nearest = (cluster, compare);
            }
            if compare < open.1 && counts[cluster] < capacity {
                open = (cluster, compare);
            }
        }
        let (cluster, compare) = if open.0 == usize::MAX { nearest } else { open };
        counts[cluster] += 1;
        (cluster, self.distance(compare))
    }

    /// A rank per cluster such that clusters with consecutive ranks are close: a
    /// greedy nearest-neighbor path over the centroids, starting from cluster 0.
    /// Sorting rows by rank keeps similar clusters next to each other, so that small
    /// clusters packed into one block are also similar.
    pub fn neighbor_ranks(&self) -> Vec<usize> {
        let k = self.k();
        let dim = self.kmeans.dim;
        let centroid = |cluster: usize| &self.centroids[cluster * dim..(cluster + 1) * dim];
        let mut ranks = vec![usize::MAX; k];
        let mut current = 0;
        for rank in 0..k {
            ranks[current] = rank;
            let mut next = (usize::MAX, f32::INFINITY);
            for cluster in (0..k).filter(|cluster| ranks[*cluster] == usize::MAX) {
                let compare = self.kmeans.compare(centroid(current), centroid(cluster));
                if compare < next.1 || next.0 == usize::MAX {
                    next = (cluster, compare);
                }
            }
            if next.0 == usize::MAX {
                break;
            }
            current = next.0;
        }
        ranks
    }

    fn distance(&self, compare: f32) -> f32 {
        match self.kmeans.distance {
            VectorDistanceType::L1 => compare,
            VectorDistanceType::L2 => compare.sqrt(),
            // 1 - dot, from the compared -dot.
            VectorDistanceType::Dot => (1.0 + compare).max(0.0),
        }
    }
}

/// Fills the empty clusters from `first` on by splitting large ones, as faiss
/// `split_clusters` does: a cluster is picked with probability proportional to its
/// size, its centroid is copied, and the two copies are moved apart by `SPLIT_EPS`.
/// Returns the number of splits.
fn split_clusters(
    centroids: &mut [f32],
    counts: &mut [f64],
    dim: usize,
    n: usize,
    first: usize,
    rng: &mut StdRng,
) -> usize {
    let k = counts.len();
    // faiss divides by `n - k`; keep it positive when seeds make `k` exceed `n`.
    let scale = n.saturating_sub(k).max(1) as f64;
    let mut splits = 0;
    for ci in first..k {
        if counts[ci] != 0.0 {
            continue;
        }
        // Only a cluster with more than one point can be split.
        if !counts.iter().any(|count| *count > 1.0) {
            break;
        }
        let mut cj = 0;
        loop {
            let p = (counts[cj] - 1.0) / scale;
            if rng.r#gen::<f64>() < p {
                break;
            }
            cj = (cj + 1) % k;
        }
        for d in 0..dim {
            let value = centroids[cj * dim + d];
            let (split, source) = if d % 2 == 0 {
                (1.0 + SPLIT_EPS, 1.0 - SPLIT_EPS)
            } else {
                (1.0 - SPLIT_EPS, 1.0 + SPLIT_EPS)
            };
            centroids[ci * dim + d] = value * split;
            centroids[cj * dim + d] = value * source;
        }
        counts[ci] = counts[cj] / 2.0;
        counts[cj] -= counts[ci];
        splits += 1;
    }
    splits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn train(
        kmeans: &KMeans,
        points: &[f32],
        k: usize,
        init: KMeansInit<'_>,
    ) -> Result<KMeansModel> {
        let dim = kmeans.dim;
        kmeans.train(points.len() / dim, k, init, |i, out| {
            out.copy_from_slice(&points[i * dim..(i + 1) * dim])
        })
    }

    #[test]
    fn test_kmeans_separates_clusters() -> Result<()> {
        // Two groups around (0, 0) and (10, 10).
        let points: Vec<f32> = (0..40)
            .flat_map(|i| {
                let base = if i < 20 { 0.0 } else { 10.0 };
                let jitter = (i % 5) as f32 * 0.1;
                [base + jitter, base - jitter]
            })
            .collect();
        let kmeans = KMeans::new(2, VectorDistanceType::L2);
        let model = train(&kmeans, &points, 2, KMeansInit::Random)?;
        let clusters: Vec<usize> = points
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| model.assign(p).0)
            .collect();
        assert!(clusters[..20].iter().all(|c| *c == clusters[0]));
        assert!(clusters[20..].iter().all(|c| *c == clusters[20]));
        assert_ne!(clusters[0], clusters[20]);
        // Training is deterministic.
        let again = train(&kmeans, &points, 2, KMeansInit::Random)?;
        assert_eq!(again.centroids, model.centroids);
        Ok(())
    }

    #[test]
    fn test_kmeans_seeds_and_sampling() -> Result<()> {
        let kmeans = KMeans::new(2, VectorDistanceType::L2);
        // A seed summarizing data away from the training points keeps its position.
        let points = [0.0, 0.0, 0.1, 0.0, 0.0, 0.1];
        let seeds = [100.0, 100.0];
        let init = KMeansInit::Seeds {
            centroids: &seeds,
            weights: &[10],
        };
        let model = train(&kmeans, &points, 2, init)?;
        assert_eq!(model.k(), 2);
        assert_eq!(&model.centroids[..2], &seeds);
        assert_eq!(model.assign(&[99.0, 99.0]).0, 0);

        // At most `MAX_POINTS_PER_CENTROID` points per centroid are read.
        let mut read = 0;
        kmeans.train(1000, 2, KMeansInit::Random, |i, out| {
            read += 1;
            out.copy_from_slice(&[i as f32, 0.0]);
        })?;
        assert_eq!(read, 2 * MAX_POINTS_PER_CENTROID);
        Ok(())
    }

    #[test]
    fn test_kmeans_balanced_assignment_and_ranks() -> Result<()> {
        let kmeans = KMeans::new(1, VectorDistanceType::L2);
        // Centroids at 0, 10 and 1: cluster 2 is next to cluster 0.
        let model = KMeansModel {
            kmeans,
            centroids: vec![0.0, 10.0, 1.0],
        };
        // Once cluster 0 holds `capacity` points, the next nearest open one is used.
        let mut counts = vec![0; 3];
        let clusters: Vec<usize> = (0..3)
            .map(|_| model.assign_balanced(&[0.0], &mut counts, 2).0)
            .collect();
        assert_eq!(clusters, [0, 0, 2]);
        assert_eq!(counts, [2, 0, 1]);
        // When every cluster is full, the nearest is used.
        assert_eq!(model.assign_balanced(&[0.0], &mut [2, 2, 2], 2).0, 0);
        // The path 0 -> 2 -> 1 visits close centroids consecutively.
        assert_eq!(model.neighbor_ranks(), [0, 2, 1]);
        Ok(())
    }

    #[test]
    fn test_split_clusters() {
        let mut centroids = vec![1.0, 2.0, 0.0, 0.0];
        let mut counts = vec![4.0, 0.0];
        let mut rng = StdRng::seed_from_u64(SEED);
        assert_eq!(
            split_clusters(&mut centroids, &mut counts, 2, 4, 0, &mut rng),
            1
        );
        assert_eq!(counts, [2.0, 2.0]);
        let (low, high) = (1.0 - SPLIT_EPS, 1.0 + SPLIT_EPS);
        assert_eq!(centroids, [low, 2.0 * high, high, 2.0 * low]);
        // Empty clusters before `first` (seeds) are not filled.
        let mut counts = vec![0.0, 4.0];
        assert_eq!(
            split_clusters(&mut centroids, &mut counts, 2, 4, 1, &mut rng),
            0
        );
    }

    #[test]
    fn test_kmeans_prepare() {
        let cosine = KMeans::new(2, VectorDistanceType::Dot);
        let mut point = [3.0, 4.0];
        assert!(cosine.prepare(&mut point));
        assert_eq!(point, [0.6, 0.8]);
        assert!(!cosine.prepare(&mut [0.0, 0.0]));
        assert!(!KMeans::new(2, VectorDistanceType::L2).prepare(&mut [f32::NAN, 0.0]));
    }

    #[test]
    fn test_page_order() -> Result<()> {
        // Four groups of 8 nearby points, interleaved; 20 points with leaves of 8.
        let kmeans = KMeans::new(2, VectorDistanceType::L2);
        let groups = [[0.0, 0.0], [100.0, 0.0], [0.0, 100.0], [100.0, 100.0]];
        let points: Vec<f32> = (0..32)
            .flat_map(|i| {
                let [x, y] = groups[i % 4];
                [x + (i / 4) as f32 * 0.1, y]
            })
            .collect();
        let order = kmeans.page_order(32, 8, |i, out| {
            out.copy_from_slice(&points[i * 2..i * 2 + 2])
        })?;
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..32).collect::<Vec<_>>());
        // Each leaf, a page of 8 positions, holds one group.
        for leaf in order.chunks(8) {
            assert!(leaf.iter().all(|i| i % 4 == leaf[0] % 4));
        }
        // A smaller last leaf: leaves of 8, 8 and 4 positions.
        let order = kmeans.page_order(20, 8, |i, out| {
            out.copy_from_slice(&points[i * 2..i * 2 + 2])
        })?;
        assert_eq!(order.len(), 20);
        Ok(())
    }
}
