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
use std::cmp::min;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;

use bitvec::prelude::BitVec;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use parking_lot::RwLock;
use rand::Rng;
use rand::distributions::Uniform;

use super::graph_layers::GraphLayerData;
use super::graph_links::GraphLinksFormat;
use crate::hnsw_index::common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use crate::hnsw_index::common::types::PointOffsetType;
use crate::hnsw_index::common::types::ScoreType;
use crate::hnsw_index::common::types::ScoredPointOffset;
use crate::hnsw_index::entry_points::EntryPoints;
use crate::hnsw_index::graph_layers::GraphLayers;
use crate::hnsw_index::graph_layers::GraphLayersBase;
use crate::hnsw_index::graph_layers::LinkContainer;
use crate::hnsw_index::graph_links::GraphLinksSerializer;
use crate::hnsw_index::point_scorer::FilteredScorer;
use crate::hnsw_index::search_context::SearchContext;
use crate::hnsw_index::visited_pool::VisitedListHandle;
use crate::hnsw_index::visited_pool::VisitedPool;

pub type LockedLinkContainer = RwLock<LinkContainer>;
pub type LockedLayersContainer = Vec<LockedLinkContainer>;

/// Same as `GraphLayers`,  but allows to build in parallel
/// Convertible to `GraphLayers`
pub struct GraphLayersBuilder {
    max_level: AtomicUsize,
    m: usize,
    m0: usize,
    ef_construct: usize,
    // Factor of level probability
    level_factor: f64,
    // Exclude points according to "not closer than base" heuristic?
    use_heuristic: bool,
    links_layers: Vec<LockedLayersContainer>,
    entry_points: Mutex<EntryPoints>,

    // Fields used on construction phase only
    visited_pool: VisitedPool,

    // List of bool flags, which defines if the point is already indexed or not
    ready_list: RwLock<BitVec>,
}

impl GraphLayersBase for GraphLayersBuilder {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, mut f: F)
    where F: FnMut(PointOffsetType) {
        let links = self.links_layers[point_id as usize][level].read();
        let ready_list = self.ready_list.read();
        for link in links.iter() {
            if ready_list[*link as usize] {
                f(*link);
            }
        }
    }

    fn get_m(&self, level: usize) -> usize {
        if level == 0 { self.m0 } else { self.m }
    }
}

impl GraphLayersBuilder {
    #[allow(dead_code)]
    pub fn get_entry_points(&self) -> MutexGuard<EntryPoints> {
        self.entry_points.lock()
    }

    pub fn into_graph_data(self, format: GraphLinksFormat) -> Result<(Vec<u8>, Vec<u8>)> {
        let serializer =
            Self::links_layers_to_serializer(self.links_layers, format, self.m, self.m0);
        let mut links_buf = Vec::new();
        serializer.serialize_to_writer(&mut links_buf)?;

        let entry_points = self.entry_points.into_inner();
        let data = GraphLayerData {
            m: self.m,
            m0: self.m0,
            ef_construct: self.ef_construct,
            entry_points: Cow::Borrowed(&entry_points),
        };

        let data_buf =
            bincode::serde::encode_to_vec(data, bincode::config::standard()).map_err(|e| {
                ErrorCode::StorageOther(format!("failed to encode graph layer data {:?}", e))
            })?;

        Ok((links_buf, data_buf))
    }

    #[allow(dead_code)]
    pub fn into_graph_layers_ram(self, format: GraphLinksFormat) -> GraphLayers {
        GraphLayers {
            m: self.m,
            m0: self.m0,
            links: Self::links_layers_to_serializer(self.links_layers, format, self.m, self.m0)
                .to_graph_links_ram(),
            entry_points: self.entry_points.into_inner(),
            visited_pool: self.visited_pool,
        }
    }

    fn links_layers_to_serializer(
        link_layers: Vec<LockedLayersContainer>,
        format: GraphLinksFormat,
        m: usize,
        m0: usize,
    ) -> GraphLinksSerializer {
        let edges = link_layers
            .into_iter()
            .map(|l| l.into_iter().map(|l| l.into_inner()).collect())
            .collect();
        GraphLinksSerializer::new(edges, format, m, m0)
    }

    pub fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let links_layers = std::iter::repeat_with(|| {
            vec![RwLock::new(if reserve {
                Vec::with_capacity(m0)
            } else {
                vec![]
            })]
        })
        .take(num_vectors)
        .collect();

        let ready_list = RwLock::new(BitVec::repeat(false, num_vectors));

        Self {
            max_level: AtomicUsize::new(0),
            m,
            m0,
            ef_construct,
            level_factor: 1.0 / (max(m, 2) as f64).ln(),
            use_heuristic,
            links_layers,
            entry_points: Mutex::new(EntryPoints::new(entry_points_num)),
            visited_pool: VisitedPool::new(),
            ready_list,
        }
    }

    pub fn new(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
    ) -> Self {
        Self::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            use_heuristic,
            true,
        )
    }

    #[allow(dead_code)]
    pub fn merge_from_other(&mut self, other: GraphLayersBuilder) {
        self.max_level = AtomicUsize::new(max(
            self.max_level.load(std::sync::atomic::Ordering::Relaxed),
            other.max_level.load(std::sync::atomic::Ordering::Relaxed),
        ));
        let mut visited_list = self.visited_pool.get(self.num_points());
        if other.links_layers.len() > self.links_layers.len() {
            self.links_layers
                .resize_with(other.links_layers.len(), Vec::new);
        }
        for (point_id, layers) in other.links_layers.into_iter().enumerate() {
            let current_layers = &mut self.links_layers[point_id];
            for (level, other_links) in layers.into_iter().enumerate() {
                if current_layers.len() <= level {
                    current_layers.push(other_links);
                } else {
                    let other_links = other_links.into_inner();
                    visited_list.next_iteration();
                    let mut current_links = current_layers[level].write();
                    current_links.iter().copied().for_each(|x| {
                        visited_list.check_and_update_visited(x);
                    });
                    for other_link in other_links
                        .into_iter()
                        .filter(|x| !visited_list.check_and_update_visited(*x))
                    {
                        current_links.push(other_link);
                    }
                }
            }
        }
        self.entry_points
            .lock()
            .merge_from_other(other.entry_points.into_inner());
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }

    /// Generate random level for a new point, according to geometric distribution
    pub fn get_random_layer<R>(&self, rng: &mut R) -> usize
    where R: Rng + ?Sized {
        // let distribution = Uniform::new(0.0, 1.0).unwrap();
        let distribution = Uniform::new(0.0, 1.0);
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * self.level_factor;
        picked_level.round() as usize
    }

    pub(crate) fn get_point_level(&self, point_id: PointOffsetType) -> usize {
        self.links_layers[point_id as usize].len() - 1
    }

    pub fn set_levels(&mut self, point_id: PointOffsetType, level: usize) {
        if self.links_layers.len() <= point_id as usize {
            while self.links_layers.len() <= point_id as usize {
                self.links_layers.push(vec![]);
            }
        }
        let point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let links = Vec::with_capacity(self.m);
            point_layers.push(RwLock::new(links));
        }
        self.max_level
            .fetch_max(level, std::sync::atomic::Ordering::Relaxed);
    }

    /// Connect new point to links, so that links contains only closest points
    fn connect_new_point<F>(
        links: &mut LinkContainer,
        new_point_id: PointOffsetType,
        target_point_id: PointOffsetType,
        level_m: usize,
        mut score_internal: F,
    ) where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score_internal(target_point_id, new_point_id);

        let mut id_to_insert = links.len();
        for (i, &item) in links.iter().enumerate() {
            let target_to_link = score_internal(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if links.len() < level_m {
            links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != links.len() {
            links.pop();
            links.insert(id_to_insert, new_point_id);
        }
    }

    /// <https://github.com/nmslib/hnswlib/issues/99>
    fn select_candidate_with_heuristic_from_sorted<F>(
        candidates: impl Iterator<Item = ScoredPointOffset>,
        m: usize,
        mut score_internal: F,
    ) -> Vec<PointOffsetType>
    where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        let mut result_list = Vec::with_capacity(m);
        for current_closest in candidates {
            if result_list.len() >= m {
                break;
            }
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected = score_internal(current_closest.idx, selected_point);
                if dist_to_already_selected > current_closest.score {
                    is_good = false;
                    break;
                }
            }
            if is_good {
                result_list.push(current_closest.idx);
            }
        }

        result_list
    }

    /// <https://github.com/nmslib/hnswlib/issues/99>
    pub(crate) fn select_candidates_with_heuristic<F>(
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
        score_internal: F,
    ) -> Vec<PointOffsetType>
    where
        F: FnMut(PointOffsetType, PointOffsetType) -> ScoreType,
    {
        let closest_iter = candidates.into_iter_sorted();
        Self::select_candidate_with_heuristic_from_sorted(closest_iter, m, score_internal)
    }

    pub fn link_new_point(&self, point_id: PointOffsetType, mut points_scorer: FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        let level = self.get_point_level(point_id);

        let entry_point_opt = self
            .entry_points
            .lock()
            .get_entry_point(|point_id| points_scorer.check_vector(point_id));
        if let Some(entry_point) = entry_point_opt {
            let mut level_entry = if entry_point.level > level {
                // The entry point is higher than a new point
                // Let's find closest one on same level

                // greedy search for a single closest point
                self.search_entry(
                    entry_point.point_id,
                    entry_point.level,
                    level,
                    &mut points_scorer,
                    &AtomicBool::new(false),
                )
                .unwrap()
            } else {
                ScoredPointOffset {
                    idx: entry_point.point_id,
                    score: points_scorer.score_internal(point_id, entry_point.point_id),
                }
            };
            // minimal common level for entry points
            let linking_level = min(level, entry_point.level);

            for curr_level in (0..=linking_level).rev() {
                level_entry = self.link_new_point_on_level(
                    point_id,
                    curr_level,
                    &mut points_scorer,
                    level_entry,
                );
            }
        } else {
            // New point is a new empty entry (for this filter, at least)
            // We can't do much here, so just quit
        }
        let was_ready = self.ready_list.write().replace(point_id as usize, true);
        debug_assert!(!was_ready, "Point {point_id} was already marked as ready");
        self.entry_points
            .lock()
            .new_point(point_id, level, |point_id| {
                points_scorer.check_vector(point_id)
            });
    }

    /// Add a new point using pre-existing links.
    /// Mutually exclusive with [`Self::link_new_point`].
    #[allow(dead_code)]
    pub fn add_new_point(&self, point_id: PointOffsetType, levels: Vec<Vec<PointOffsetType>>) {
        let level = self.get_point_level(point_id);
        debug_assert_eq!(levels.len(), level + 1);

        for (level, neighbours) in levels.iter().enumerate() {
            let mut links = self.links_layers[point_id as usize][level].write();
            links.clear();
            links.extend_from_slice(neighbours);
        }

        let was_ready = self.ready_list.write().replace(point_id as usize, true);
        debug_assert!(!was_ready);
        self.entry_points
            .lock()
            .new_point(point_id, level, |_| true);
    }

    /// Link a new point on a specific level.
    /// Returns an entry point for the level below.
    fn link_new_point_on_level(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        points_scorer: &mut FilteredScorer,
        mut level_entry: ScoredPointOffset,
    ) -> ScoredPointOffset {
        let mut visited_list = self.get_visited_list_from_pool();

        visited_list.check_and_update_visited(level_entry.idx);

        let mut search_context = SearchContext::new(level_entry, self.ef_construct);

        self._search_on_level(
            &mut search_context,
            curr_level,
            &mut visited_list,
            points_scorer,
            &AtomicBool::new(false),
        )
        .unwrap();

        if let Some(the_nearest) = search_context.nearest.iter_unsorted().max() {
            level_entry = *the_nearest;
        }

        if self.use_heuristic {
            self.link_with_heuristic(
                point_id,
                curr_level,
                &visited_list,
                points_scorer,
                search_context,
            );
        } else {
            self.link_without_heuristic(point_id, curr_level, points_scorer, search_context);
        }

        level_entry
    }

    fn link_with_heuristic(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        visited_list: &VisitedListHandle,
        points_scorer: &FilteredScorer,
        mut search_context: SearchContext,
    ) {
        let level_m = self.get_m(curr_level);
        let scorer = |a, b| points_scorer.score_internal(a, b);

        let selected_nearest = {
            let mut existing_links = self.links_layers[point_id as usize][curr_level].write();
            {
                let ready_list = self.ready_list.read();
                for &existing_link in existing_links.iter() {
                    if !visited_list.check(existing_link) && ready_list[existing_link as usize] {
                        search_context.process_candidate(ScoredPointOffset {
                            idx: existing_link,
                            score: points_scorer.score_point(existing_link),
                        });
                    }
                }
            }

            let selected_nearest =
                Self::select_candidates_with_heuristic(search_context.nearest, level_m, scorer);
            existing_links.clone_from(&selected_nearest);
            selected_nearest
        };

        for &other_point in &selected_nearest {
            let mut other_point_links = self.links_layers[other_point as usize][curr_level].write();
            if other_point_links.len() < level_m {
                // If linked point is lack of neighbours
                other_point_links.push(point_id);
            } else {
                let mut candidates = BinaryHeap::with_capacity(level_m + 1);
                candidates.push(ScoredPointOffset {
                    idx: point_id,
                    score: points_scorer.score_internal(point_id, other_point),
                });
                for other_point_link in other_point_links.iter().take(level_m).copied() {
                    candidates.push(ScoredPointOffset {
                        idx: other_point_link,
                        score: points_scorer.score_internal(other_point_link, other_point),
                    });
                }
                let selected_candidates = Self::select_candidate_with_heuristic_from_sorted(
                    candidates.into_sorted_vec().into_iter().rev(),
                    level_m,
                    scorer,
                );
                other_point_links.clear(); // this do not free memory, which is good
                for selected in selected_candidates.iter().copied() {
                    other_point_links.push(selected);
                }
            }
        }
    }

    fn link_without_heuristic(
        &self,
        point_id: PointOffsetType,
        curr_level: usize,
        points_scorer: &FilteredScorer,
        search_context: SearchContext,
    ) {
        let level_m = self.get_m(curr_level);
        let scorer = |a, b| points_scorer.score_internal(a, b);
        for nearest_point in search_context.nearest.iter_unsorted() {
            {
                let mut links = self.links_layers[point_id as usize][curr_level].write();
                Self::connect_new_point(&mut links, nearest_point.idx, point_id, level_m, scorer);
            }

            {
                let mut links = self.links_layers[nearest_point.idx as usize][curr_level].write();
                Self::connect_new_point(&mut links, point_id, nearest_point.idx, level_m, scorer);
            }
        }
    }

    /// This function returns average number of links per node in HNSW graph
    /// on specified level.
    ///
    /// Useful for:
    /// - estimating memory consumption
    /// - percolation threshold estimation
    /// - debugging
    #[allow(dead_code)]
    pub fn get_average_connectivity_on_level(&self, level: usize) -> f32 {
        let mut sum = 0;
        let mut count = 0;
        for links in self.links_layers.iter() {
            if links.len() > level {
                sum += links[level].read().len();
                count += 1;
            }
        }
        if count == 0 {
            0.0
        } else {
            sum as f32 / count as f32
        }
    }
}
