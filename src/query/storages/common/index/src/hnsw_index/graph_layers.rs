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
    fn get_visited_list_from_pool(&self) -> VisitedListHandle;

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where F: FnMut(PointOffsetType);

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
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
        is_stopped: &AtomicBool,
    ) -> Result<FixedLengthPriorityQueue<ScoredPointOffset>> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef);

        self._search_on_level(
            &mut search_context,
            level,
            &mut visited_list,
            points_scorer,
            is_stopped,
        )?;
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

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where F: FnMut(PointOffsetType) {
        self.links.links(point_id, level).for_each(f);
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

/// Object contains links between nodes for HNSW search
///
/// Assume all scores are similarities. Larger score = closer points
impl GraphLayers {
    /// Returns the highest level this point is included in
    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.links.point_level(point_id)
    }

    fn get_entry_point(
        &self,
        points_scorer: &FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
    ) -> Option<EntryPoint> {
        // Try to get it from custom entry points
        custom_entry_points
            .and_then(|custom_entry_points| {
                custom_entry_points
                    .iter()
                    .filter(|&&point_id| points_scorer.check_vector(point_id))
                    .map(|&point_id| {
                        let level = self.point_level(point_id);
                        EntryPoint { point_id, level }
                    })
                    .max_by_key(|ep| ep.level)
            })
            .or_else(|| {
                // Otherwise use normal entry points
                self.entry_points
                    .get_entry_point(|point_id| points_scorer.check_vector(point_id))
            })
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        mut points_scorer: FilteredScorer,
        custom_entry_points: Option<&[PointOffsetType]>,
        is_stopped: &AtomicBool,
    ) -> Result<Vec<ScoredPointOffset>> {
        let Some(entry_point) = self.get_entry_point(&points_scorer, custom_entry_points) else {
            return Ok(Vec::default());
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
            is_stopped,
        )?;
        let nearest = self.search_on_level(
            zero_level_entry,
            0,
            max(top, ef),
            &mut points_scorer,
            is_stopped,
        )?;
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
