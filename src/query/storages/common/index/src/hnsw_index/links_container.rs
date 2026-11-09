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

// Ported from qdrant `lib/segment/src/index/hnsw_index/links_container.rs`
// (v1.20.0, "Heuristic optimization", qdrant#6501). The redundancy test keeps the
// strict comparison of the original heuristic, so the selected links are the same.
// Unlike qdrant, the first link is also marked as processed.

use std::cell::Cell;
use std::iter::Copied;
use std::num::NonZeroU32;

use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoreType;
use crate::hnsw_index::common::types::ScoredPointOffset;

/// Neighbour-selection ("not closer than base") test: a candidate is dropped when
/// an already selected link is closer to it than the target is.
#[inline]
fn is_redundant(candidate_to_existing: ScoreType, candidate_score: ScoreType) -> bool {
    candidate_to_existing > candidate_score
}

/// The links of a point on one level, sorted from the closest, and how many of
/// them the heuristic has already selected together.
#[derive(Debug, Default)]
pub struct LinksContainer {
    links: Vec<PointOffsetType>,
    /// Number of links that have been processed by the heuristic.
    processed_by_heuristic: u32,
}

impl LinksContainer {
    pub fn with_capacity(m: usize) -> Self {
        Self {
            links: Vec::with_capacity(m),
            processed_by_heuristic: 0,
        }
    }

    pub fn push(&mut self, link: PointOffsetType) {
        self.links.push(link);
    }

    pub fn links(&self) -> &[PointOffsetType] {
        &self.links
    }

    pub fn iter(&self) -> Copied<std::slice::Iter<'_, PointOffsetType>> {
        self.links.iter().copied()
    }

    pub fn into_vec(self) -> Vec<PointOffsetType> {
        self.links
    }

    /// Put points into the container.
    pub fn fill_from(&mut self, points: impl Iterator<Item = PointOffsetType>) {
        self.links.clear();
        self.links.extend(points);
        self.processed_by_heuristic = 0;
    }

    /// Put `m` candidates selected by the heuristic into the container.
    pub fn fill_from_sorted_with_heuristic(
        &mut self,
        candidates: impl Iterator<Item = ScoredPointOffset>,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        self.links.clear();
        if level_m == 0 {
            // Unlikely.
            self.processed_by_heuristic = 0;
            return;
        }
        'outer: for candidate in candidates {
            for &existing in &self.links {
                if is_redundant(score(candidate.idx, existing), candidate.score) {
                    continue 'outer;
                }
            }
            self.links.push(candidate.idx);
            if self.links.len() >= level_m {
                break;
            }
        }
        self.processed_by_heuristic = self.links.len() as u32;
    }

    /// Connect new point to links, so that links contains only closest points.
    pub fn connect(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    ) {
        // Invalidate assumptions about the heuristic eagerly.
        self.processed_by_heuristic = 0;

        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score(target_point_id, new_point_id);

        let mut id_to_insert = self.links.len();
        for (i, &item) in self.links.iter().enumerate() {
            let target_to_link = score(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if self.links.len() < level_m {
            self.links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != self.links.len() {
            self.links.pop();
            self.links.insert(id_to_insert, new_point_id);
        }
    }

    /// Append one point to the container. If the container is full, run the heuristic.
    ///
    /// The result is exactly the same as `connect_with_heuristic_simple`, the
    /// reference implementation in the tests module,
    /// but this implementation cuts some corners given that some of the links
    /// are already processed by the heuristic.
    pub fn connect_with_heuristic(
        &mut self,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
        items: &mut ItemsBuffer,
    ) {
        if level_m == 0 {
            // Unlikely.
            return;
        }

        if self.links.len() < level_m {
            self.links.push(new_point_id);
            return;
        }

        items.0.clear();
        items.0.reserve(level_m + 1);
        for (order, &link) in self.links.iter().enumerate() {
            items.0.push(Item {
                idx: link,
                score: Cell::new(None),
                // `+ 1`: `NonZeroU32::new(0)` would leave the first link unmarked.
                order: if order < self.processed_by_heuristic as usize {
                    NonZeroU32::new(order as u32 + 1)
                } else {
                    None
                },
            });
        }
        items.0.push(Item {
            idx: new_point_id,
            score: Cell::new(None),
            order: None,
        });
        items.0.sort_unstable_by(|a, b| {
            if let (Some(a_order), Some(b_order)) = (a.order, b.order) {
                // Both items processed by the heuristic, compare their order
                // to avoid recomputing the score.
                return a_order.cmp(&b_order);
            }
            b.cached_score(target_point_id, &mut score)
                .total_cmp(&a.cached_score(target_point_id, &mut score))
        });

        self.links.clear();

        // The code below is similar to `fill_from_sorted_with_heuristic` with
        // two notable differences:
        //
        // (A) If both items have already been processed by the heuristic, the
        // score check is skipped as it is known to pass.
        //
        // (B) Instead of having separate input iterator (`candidates`) and
        // intermediate vector for the processed items (`self.links`), this
        // implementation reads and updates the same vector in-place:
        // - `items[read]` is the next candidate to be processed.
        // - `items[0..write]` are already processed items.
        // Since `read` ≤ `write`, there are no collisions, so this approach is
        // sound.
        let mut write = 0;
        'outer: for read in 0..items.0.len() {
            let candidate = items.0[read].clone();
            for existing in &items.0[0..write] {
                if candidate.order.is_some() && existing.order.is_some() {
                    continue; // See (A).
                }
                if is_redundant(
                    score(candidate.idx, existing.idx),
                    candidate.cached_score(target_point_id, &mut score),
                ) {
                    continue 'outer;
                }
            }

            self.links.push(candidate.idx);
            items.0[write] = candidate;
            write += 1;
            if write >= level_m {
                break;
            }
        }
        self.processed_by_heuristic = self.links.len() as u32;
    }
}

/// Internal buffer to avoid allocations.
#[derive(Default)]
pub struct ItemsBuffer(Vec<Item>);

#[derive(Debug, Clone)]
struct Item {
    idx: PointOffsetType,
    score: Cell<Option<ScoreType>>,
    /// Order is used to avoid recomputing the score for items known be sorted.
    order: Option<NonZeroU32>,
}

impl Item {
    /// Get the score. This value is lazy/cached: it's computed no more than once.
    fn cached_score<F>(&self, query: PointOffsetType, score: F) -> ScoreType
    where F: FnOnce(PointOffsetType, PointOffsetType) -> ScoreType {
        if let Some(score) = self.score.get() {
            score
        } else {
            let score = score(query, self.idx);
            self.score.set(Some(score));
            score
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use itertools::Itertools as _;
    use rand::Rng as _;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom as _;

    use super::*;
    use crate::hnsw_index::common::fixed_length_priority_queue::FixedLengthPriorityQueue;

    impl LinksContainer {
        /// Append one point to the container. If the container is full, run the heuristic.
        ///
        /// This is a reference implementation for testing.
        fn connect_with_heuristic_simple(
            &mut self,
            new_point_id: PointOffsetType,
            target_point_id: PointOffsetType,
            level_m: usize,
            mut score: impl FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
        ) {
            if self.links.len() < level_m {
                self.links.push(new_point_id);
            } else {
                let mut candidates = Vec::with_capacity(level_m + 1);
                for &idx in &self.links {
                    candidates.push(ScoredPointOffset {
                        idx,
                        score: score(target_point_id, idx),
                    });
                }
                candidates.push(ScoredPointOffset {
                    idx: new_point_id,
                    score: score(target_point_id, new_point_id),
                });
                candidates.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
                self.fill_from_sorted_with_heuristic(candidates.into_iter(), level_m, score);
            }
        }
    }

    #[test]
    fn test_connect_new_point() {
        let m = 6;

        // ○ 10 K
        //
        //
        //
        //
        //
        //
        //
        //
        //
        //
        //                 ○ 9 H
        //
        //
        //
        //
        //
        //           ○ 7 I
        //                        ● 6 G
        //
        //
        //           ○ 8 J                                         ○ 4 E
        //
        //
        //                                                          ● 3 D
        //
        //
        //                                            ◉ Target             ○ 5 F
        //   Y
        //   ▲
        //   │                                  ● 1 B
        //   └──▶ X
        //                                         ○ 2 C
        let points: Vec<Vec<f32>> = vec![
            vec![21.79, 07.18], //   Target
            vec![20.58, 05.46], // + 1 B
            vec![21.19, 04.51], //   2 C   closer to B than to the target
            vec![24.73, 08.24], // + 3 D
            vec![24.55, 09.98], //   4 E   closer to D than to the target
            vec![26.11, 06.85], //   5 F   closer to D than to the target
            vec![17.64, 11.14], // + 6 G
            vec![14.97, 11.52], //   7 I   closer to G than to the target
            vec![14.97, 09.60], //   8 J   closer to B and G than to the target
            vec![16.23, 14.32], //   9 H   closer to G than to the target
            vec![12.69, 19.13], //  10 K   closer to G than to the target
        ];

        let scorer = |a: PointOffsetType, b: PointOffsetType| {
            -((points[a as usize][0] - points[b as usize][0]).powi(2)
                + (points[a as usize][1] - points[b as usize][1]).powi(2))
            .sqrt()
        };

        let mut insert_ids = (1..points.len() as PointOffsetType).collect_vec();

        let mut candidates = FixedLengthPriorityQueue::new(insert_ids.len());
        for &id in &insert_ids {
            candidates.push(ScoredPointOffset {
                idx: id,
                score: scorer(0, id),
            });
        }

        let mut res = LinksContainer::with_capacity(m);
        res.fill_from_sorted_with_heuristic(candidates.into_iter_sorted(), m, scorer);

        assert_eq!(res.links(), &[1, 3, 6]);

        let mut rng = StdRng::seed_from_u64(42);

        let mut links_container = LinksContainer::with_capacity(m);
        insert_ids.shuffle(&mut rng);
        for &id in &insert_ids {
            links_container.connect(id, 0, m, scorer);
        }
        assert_eq!(links_container.links(), &[1, 2, 3, 4, 5, 6]);
    }

    /// `connect_with_heuristic` selects the same links as the reference, which
    /// runs the heuristic over all links and their scores again on every insert,
    /// as the builder did before.
    #[test]
    fn test_connect_new_point_with_heuristic() {
        let mut rng = StdRng::seed_from_u64(42);

        const NUM_VECTORS: usize = 20;
        const DIM: usize = 128;
        const M: usize = 5;

        for _ in 0..1000 {
            let vectors: Vec<Vec<f32>> = (0..NUM_VECTORS)
                .map(|_| (0..DIM).map(|_| rng.gen_range(-1.0..1.0)).collect())
                .collect();
            let score = |a: u32, b: u32| {
                -vectors[a as usize]
                    .iter()
                    .zip(&vectors[b as usize])
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
            };

            let mut candidate_indices: Vec<_> = (0..NUM_VECTORS as u32).collect();
            candidate_indices.shuffle(&mut rng);

            let query_idx = candidate_indices.pop().unwrap();
            let scored_offset = |idx: u32| ScoredPointOffset {
                idx,
                score: score(query_idx, idx),
            };

            let mut container = LinksContainer::with_capacity(M);
            container.fill_from_sorted_with_heuristic(
                candidate_indices
                    .iter()
                    .copied()
                    .map(scored_offset)
                    .take(5)
                    .sorted_by(|a, b| b.score.total_cmp(&a.score)),
                M,
                score,
            );

            let mut reference_container = LinksContainer::with_capacity(M);
            reference_container.fill_from_sorted_with_heuristic(
                candidate_indices
                    .iter()
                    .copied()
                    .map(scored_offset)
                    .take(5)
                    .sorted_by(|a, b| b.score.total_cmp(&a.score)),
                M,
                score,
            );

            let mut items = ItemsBuffer::default();
            for &candidate_idx in candidate_indices.iter().skip(5) {
                container.connect_with_heuristic(candidate_idx, query_idx, M, score, &mut items);
                reference_container.connect_with_heuristic_simple(
                    candidate_idx,
                    query_idx,
                    M,
                    score,
                );
                assert_eq!(container.links, reference_container.links);
            }
        }
    }

    /// Two links the heuristic selected together, the first one included, are not
    /// scored against each other again, and the links are those of the reference.
    #[test]
    fn test_connect_with_heuristic_skips_processed_pairs() {
        let mut rng = StdRng::seed_from_u64(7);

        const NUM_VECTORS: usize = 20;
        const DIM: usize = 128;
        const M: usize = 5;

        let mut checked = 0;
        for _ in 0..100 {
            let vectors: Vec<Vec<f32>> = (0..NUM_VECTORS)
                .map(|_| (0..DIM).map(|_| rng.gen_range(-1.0..1.0)).collect())
                .collect();
            let calls = RefCell::new(Vec::new());
            let score = |a: u32, b: u32| {
                calls.borrow_mut().push((a, b));
                -vectors[a as usize]
                    .iter()
                    .zip(&vectors[b as usize])
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
            };

            let (target, new_point) = (0, NUM_VECTORS as u32 - 1);
            let mut container = LinksContainer::with_capacity(M);
            container.fill_from_sorted_with_heuristic(
                (1..new_point)
                    .map(|idx| ScoredPointOffset {
                        idx,
                        score: score(target, idx),
                    })
                    .sorted_by(|a, b| b.score.total_cmp(&a.score)),
                M,
                score,
            );
            if container.links().len() < M {
                continue;
            }
            let processed = container.links().to_vec();
            let mut reference = LinksContainer::with_capacity(M);
            reference.fill_from(processed.iter().copied());

            calls.borrow_mut().clear();
            let mut items = ItemsBuffer::default();
            container.connect_with_heuristic(new_point, target, M, score, &mut items);
            for &(a, b) in calls.borrow().iter() {
                assert!(
                    !(processed.contains(&a) && processed.contains(&b)),
                    "processed links {a} and {b} scored again"
                );
            }

            reference.connect_with_heuristic_simple(new_point, target, M, score);
            assert_eq!(container.links, reference.links);
            checked += 1;
        }
        assert!(checked > 0);
    }
}
