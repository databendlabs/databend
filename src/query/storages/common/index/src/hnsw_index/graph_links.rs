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

use databend_common_exception::Result;

use crate::hnsw_index::common::types::PointOffsetType;

mod header;
mod serializer;
mod view;

pub use serializer::GraphLinksSerializer;
use view::CompressionInfo;
use view::GraphLinksView;
pub use view::LinksIterator;

// Links data for whole graph layers.
//
// sorted
// points:        points:
// points to lvl        012345         142350
// 0 -> 0
// 1 -> 4    lvl4:  7       lvl4: 7
// 2 -> 2    lvl3:  Z  Y    lvl3: ZY
// 3 -> 2    lvl2:  abcd    lvl2: adbc
// 4 -> 3    lvl1:  ABCDE   lvl1: ADBCE
// 5 -> 1    lvl0: 123456   lvl0: 123456  <- lvl 0 is not sorted
//
//
// lvl offset:        6       11     15     17
// │       │      │      │
// │       │      │      │
// ▼       ▼      ▼      ▼
// indexes:  012345   6789A   BCDE   FG     H
//
// flatten:  123456   ADBCE   adbc   ZY     7
// ▲ ▲ ▲   ▲ ▲    ▲      ▲
// │ │ │   │ │    │      │
// │ │ │   │ │    │      │
// │ │ │   │ │    │      │
// reindex:           142350  142350 142350 142350  (same for each level)
//
//
// for lvl > 0:
// links offset = level_offsets[level] + offsets[reindex[point_id]]

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GraphLinksFormat {
    #[allow(dead_code)]
    Plain,
    Compressed,
}

self_cell::self_cell! {
    pub struct GraphLinks {
        owner: Vec<u8>,
        #[covariant]
        dependent: GraphLinksView,
    }

    impl {Debug}
}

impl GraphLinks {
    pub fn load(data: &[u8]) -> Result<Self> {
        let format = GraphLinksFormat::Compressed;
        Self::try_new(data.to_vec(), |x| GraphLinksView::load(x, format))
    }

    fn view(&self) -> &GraphLinksView<'_> {
        self.borrow_dependent()
    }

    pub fn format(&self) -> GraphLinksFormat {
        match self.view().compression {
            CompressionInfo::Uncompressed { .. } => GraphLinksFormat::Plain,
            CompressionInfo::Compressed { .. } => GraphLinksFormat::Compressed,
        }
    }

    pub fn num_points(&self) -> usize {
        self.view().reindex.len()
    }

    #[allow(dead_code)]
    pub fn for_each_link(
        &self,
        point_id: PointOffsetType,
        level: usize,
        f: impl FnMut(PointOffsetType),
    ) {
        self.links(point_id, level).for_each(f);
    }

    #[inline]
    pub fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        self.view().links(point_id, level)
    }

    pub fn point_level(&self, point_id: PointOffsetType) -> usize {
        self.view().point_level(point_id)
    }

    /// Convert the graph links to a vector of edges, suitable for passing into
    /// [`GraphLinksSerializer::new`] or using in tests.
    pub fn into_edges(self) -> Vec<Vec<Vec<PointOffsetType>>> {
        let mut edges = Vec::with_capacity(self.num_points());
        for point_id in 0..self.num_points() {
            let num_levels = self.point_level(point_id as PointOffsetType) + 1;
            let mut levels = Vec::with_capacity(num_levels);
            for level in 0..num_levels {
                levels.push(self.links(point_id as PointOffsetType, level).collect());
            }
            edges.push(levels);
        }
        edges
    }
}
