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

use std::borrow::Cow;
use std::cmp::max;
use std::ops::ControlFlow;
use std::sync::atomic::AtomicBool;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use super::entry_points::EntryPoint;
use super::graph_links::GraphLinks;
use super::graph_links::GraphLinksFormat;
use crate::hnsw_index::common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoredPointOffset;
use crate::hnsw_index::common::utils::check_process_stopped;
use crate::hnsw_index::common::utils::rev_range;
use crate::hnsw_index::entry_points::EntryPoints;
use crate::hnsw_index::graph_links::GraphLinksSerializer;
use crate::hnsw_index::point_scorer::FilteredScorer;
use crate::hnsw_index::search_context::SearchContext;
use crate::hnsw_index::visited_pool::VisitedListHandle;
use crate::hnsw_index::visited_pool::VisitedPool;

pub type LinkContainer = Vec<PointOffsetType>;

/// Should return up to `n` points that match the filter.
/// Used when the graph has no entry point matching the filter.
pub type FilteredPoints<'a> = dyn Fn(usize) -> Vec<PointOffsetType> + 'a;

/// How many [`FilteredPoints`] to start from, per `ef`.
const SEEDS_PER_EF: usize = 2;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SearchAlgorithm {
    Hnsw,
    Acorn,
}

/// Where a search enters the graph.
#[derive(Debug, Clone)]
pub enum SearchEntry {
    /// Descend from this point down to level 0. Classic HNSW.
    Point(EntryPoint),
    /// Start level 0 from all of these points at once.
    Seeds(Vec<ScoredPointOffset>),
}
#[allow(dead_code)]
pub type LayersContainer = Vec<LinkContainer>;

/// Contents of the `graph.bin` file.
#[derive(Deserialize, Serialize, Debug)]
pub(super) struct GraphLayerData<'a> {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,
    pub(super) entry_points: Cow<'a, EntryPoints>,
}

#[derive(Debug)]
pub struct GraphLayers {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) links: GraphLinks,
    pub(super) entry_points: EntryPoints,
    pub(super) visited_pool: VisitedPool,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_>;

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where F: FnMut(PointOffsetType);

    fn try_for_each_link<F>(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: F,
    ) -> ControlFlow<()>
    where
        F: FnMut(PointOffsetType) -> ControlFlow<()>;

    /// Get M based on current level
    fn get_m(&self, level: usize) -> usize;

    /// Greedy search for closest points within a single graph layer
    fn _search_on_level(
        &self,
        searcher: &mut SearchContext,
        level: usize,
        visited_list: &mut VisitedListHandle,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<()> {
        let limit = self.get_m(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = searcher.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < searcher.lower_bound() {
                break;
            }

            points_ids.clear();
            self.links_map(candidate.idx, level, |link| {
                if !visited_list.check(link) {
                    points_ids.push(link);
                }
            });

            let scores = points_scorer.score_points(&mut points_ids, limit);
            scores.iter().copied().for_each(|score_point| {
                searcher.process_candidate(score_point);
                visited_list.check_and_update_visited(score_point.idx);
            });
        }

        Ok(())
    }

    fn search_on_level(
        &self,
        level_entries: &[ScoredPointOffset],
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        let mut search_context = SearchContext::with_entries(ef, level_entries, &mut visited_list);

        self._search_on_level(
            &mut search_context,
            level,
            &mut visited_list,
            points_scorer,
            is_stopped,
        )?;
        Ok(search_context.nearest)
    }

    /// Variation of [`GraphLayersBase::search_on_level`] that implements the
    /// ACORN-1 algorithm: the links of the points the filter rejects are
    /// explored too, so that a sparse filter does not cut the graph apart.
    fn search_on_level_acorn(
        &self,
        level_entries: &[ScoredPointOffset],
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<FixedLengthPriorityQueue<ScoredPointOffset>> {
        // Each node in `hop1_visited_list` either:
        // a) Non-deleted node that going to be scored and added to
        //    `search_context` for further expansion. (or already added)
        // b) Deleted node that scheduled for exploration for 2-hop neighbors.
        let mut hop1_visited_list = self.get_visited_list_from_pool();

        // Nodes in `hop2_visited_list` are already explored as 2-hop neighbors.
        // Being in this list doesn't prevent the node to be handled again as
        // 1-hop neighbor.
        let mut hop2_visited_list = self.get_visited_list_from_pool();

        let mut search_context =
            SearchContext::with_entries(ef, level_entries, &mut hop1_visited_list);

        // Limits are per every explored 1-hop or 2-hop neighbors, not total.
        // This is necessary to avoid over-scoring when there are many
        // additional graph links.
        let m = self.get_m(level);
        let (hop1_limit, hop2_limit, capacity);
        if m != 0 {
            hop1_limit = m;
            hop2_limit = m;
            capacity = hop1_limit * hop2_limit.min(16);
        } else {
            // `m = 0` graphs only have payload links: take all, as `search_on_level` does.
            hop1_limit = usize::MAX;
            hop2_limit = usize::MAX;
            capacity = 16 * 16; // Guess: payload_m = 16
        }

        // Non-matches past the first `hop1_limit` links (the payload-block
        // tail) share this budget, evenly spaced.
        let hop1_tail_limit = hop1_limit;

        let mut to_score = Vec::with_capacity(capacity);
        let mut to_explore = Vec::with_capacity(capacity);
        let mut tail_bridges = Vec::new();

        while let Some(candidate) = search_context.candidates.pop() {
            check_process_stopped(is_stopped)?;

            if candidate.score < search_context.lower_bound() {
                break;
            }

            to_explore.clear();
            to_score.clear();
            tail_bridges.clear();

            // Collect 1-hop neighbors (direct neighbors)
            let mut rank = 0;
            _ = self.try_for_each_link(candidate.idx, level, |hop1| {
                let is_head = rank < hop1_limit;
                rank += 1;

                if hop1_visited_list.check(hop1) {
                    return ControlFlow::Continue(());
                }

                if points_scorer.check_vector(hop1) {
                    hop1_visited_list.check_and_update_visited(hop1);
                    to_score.push(hop1);
                    if to_score.len() >= hop1_limit {
                        return ControlFlow::Break(());
                    }
                } else if is_head {
                    hop1_visited_list.check_and_update_visited(hop1);
                    to_explore.push(hop1);
                } else {
                    tail_bridges.push(hop1);
                }
                ControlFlow::Continue(())
            });
            for hop1 in evenly_spaced(&tail_bridges, hop1_tail_limit) {
                hop1_visited_list.check_and_update_visited(hop1);
                to_explore.push(hop1);
            }

            // Collect 2-hop neighbors (neighbors of neighbors)
            for &hop1 in &to_explore {
                check_process_stopped(is_stopped)?;

                let total_limit = to_score.len().saturating_add(hop2_limit);
                _ = self.try_for_each_link(hop1, level, |hop2| {
                    if hop1_visited_list.check(hop2)
                        || hop2_visited_list.check_and_update_visited(hop2)
                    {
                        return ControlFlow::Continue(());
                    }

                    if points_scorer.check_vector(hop2) {
                        hop1_visited_list.check_and_update_visited(hop2);
                        to_score.push(hop2);
                        if to_score.len() >= total_limit {
                            return ControlFlow::Break(());
                        }
                    }
                    ControlFlow::Continue(())
                });
            }

            points_scorer
                .score_points_unfiltered(&to_score)
                .iter()
                .copied()
                .for_each(|score_point| search_context.process_candidate(score_point));
        }

        Ok(search_context.nearest)
    }

    /// Greedy searches for entry point of level `target_level`.
    /// Beam size is 1.
    fn search_entry(
        &self,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<ScoredPointOffset> {
        let mut links: Vec<PointOffsetType> = Vec::with_capacity(2 * self.get_m(0));

        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            check_process_stopped(is_stopped)?;

            let limit = self.get_m(level);

            let mut changed = true;
            while changed {
                changed = false;

                links.clear();
                self.links_map(current_point.idx, level, |link| {
                    links.push(link);
                });

                let scores = points_scorer.score_points(&mut links, limit);
                scores.iter().copied().for_each(|score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
            }
        }
        Ok(current_point)
    }
}

pub(super) fn evenly_spaced<T: Copy>(items: &[T], limit: usize) -> impl Iterator<Item = T> {
    let picks = items.len().min(limit);
    (0..picks).map(move |j| items[j * items.len() / picks])
}

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle<'_> {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where F: FnMut(PointOffsetType) {
        self.links.links(point_id, level).for_each(f);
    }

    fn try_for_each_link<F>(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: F,
    ) -> ControlFlow<()>
    where
        F: FnMut(PointOffsetType) -> ControlFlow<()>,
    {
        self.links.links(point_id, level).try_for_each(f)
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
    pub(crate) fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.links.memory_size() - std::mem::size_of::<GraphLinks>()
            + self.entry_points.memory_size()
            - std::mem::size_of::<EntryPoints>()
            + self
                .visited_pool
                .retained_memory_size(self.links.num_points())
            - std::mem::size_of::<VisitedPool>()
    }

    /// Returns the highest level this point is included in
    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.links.point_level(point_id)
    }

    fn get_entry_point(
        &self,
        points_scorer: &FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
        filtered_points: Option<&FilteredPoints<'_>>,
        seeds_num: usize,
    ) -> Option<SearchEntry> {
        // Custom entry points
        if let Some(custom_entry_points) = custom_entry_points {
            let custom_best = custom_entry_points
                .iter()
                .filter(|&&point_id| points_scorer.check_vector(point_id))
                .map(|&point_id| {
                    let level = self.point_level(point_id);
                    EntryPoint { point_id, level }
                })
                .max_by_key(|ep| ep.level);
            if let Some(entry_point) = custom_best {
                return Some(SearchEntry::Point(entry_point));
            }
        }

        // Graph entry points
        let entry_point = self
            .entry_points
            .get_entry_point(|point_id| points_scorer.check_vector(point_id));
        if let Some(entry_point) = entry_point {
            return Some(SearchEntry::Point(entry_point));
        }

        // Filtered points as seeds
        if let Some(filtered_points) = filtered_points {
            let mut seed_ids = filtered_points(seeds_num);
            seed_ids.retain(|&point_id| points_scorer.check_vector(point_id));
            if seed_ids.is_empty() {
                return None;
            }
            let seeds = seed_ids
                .into_iter()
                .map(|idx| ScoredPointOffset {
                    idx,
                    score: points_scorer.score_point(idx),
                })
                .collect();
            return Some(SearchEntry::Seeds(seeds));
        }

        None
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        algorithm: SearchAlgorithm,
        mut points_scorer: FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
        filtered_points: Option<&FilteredPoints<'_>>,
        is_stopped: &AtomicBool,
    ) -> Result<Vec<ScoredPointOffset>> {
        let seeds_num = SEEDS_PER_EF * ef.max(top);
        let Some(entry) = self.get_entry_point(
            &points_scorer,
            custom_entry_points,
            filtered_points,
            seeds_num,
        ) else {
            return Ok(Vec::default());
        };

        let level_entries: &[_] = match &entry {
            SearchEntry::Point(entry_point) => &[self.search_entry(
                entry_point.point_id,
                entry_point.level,
                0,
                &mut points_scorer,
                is_stopped,
            )?],
            SearchEntry::Seeds(seeds) => seeds,
        };
        let ef = max(ef, top);
        let nearest = match algorithm {
            SearchAlgorithm::Hnsw => {
                self.search_on_level(level_entries, 0, ef, &mut points_scorer, is_stopped)
            }
            SearchAlgorithm::Acorn => {
                self.search_on_level_acorn(level_entries, 0, ef, &mut points_scorer, is_stopped)
            }
        }?;
        Ok(nearest.into_iter_sorted().take(top).collect_vec())
    }

    #[allow(dead_code)]
    pub fn num_points(&self) -> usize {
        self.links.num_points()
    }
}

impl GraphLayers {
    pub fn open(links_slice: &[u8], data_slice: &[u8]) -> Result<Self> {
        let (graph_data, _): (GraphLayerData, _) =
            bincode::serde::decode_from_slice(data_slice, bincode::config::standard()).map_err(
                |e| ErrorCode::StorageOther(format!("failed to decode graph layer data {:?}", e)),
            )?;

        let graph_links = GraphLinks::load(links_slice)?;
        Ok(Self {
            m: graph_data.m,
            m0: graph_data.m0,
            links: graph_links,
            entry_points: graph_data.entry_points.into_owned(),
            visited_pool: VisitedPool::new(),
        })
    }

    #[allow(dead_code)]
    pub fn compress_ram(&mut self) {
        assert_eq!(self.links.format(), GraphLinksFormat::Plain);
        let dummy = GraphLinksSerializer::new(Vec::new(), GraphLinksFormat::Plain, 0, 0)
            .to_graph_links_ram();
        let links = std::mem::replace(&mut self.links, dummy);
        self.links = GraphLinksSerializer::new(
            links.into_edges(),
            GraphLinksFormat::Compressed,
            self.m,
            self.m0,
        )
        .to_graph_links_ram();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evenly_spaced() {
        assert_eq!(evenly_spaced(b"", 4).collect_vec(), b"");
        assert_eq!(evenly_spaced(b"012", 0).collect_vec(), b"");
        assert_eq!(evenly_spaced(b"012", 4).collect_vec(), b"012");
        assert_eq!(evenly_spaced(b"01234567", 4).collect_vec(), b"0246");
        assert_eq!(evenly_spaced(b"0123456789", 4).collect_vec(), b"0257");
    }
}
