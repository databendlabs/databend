//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_base::BlockingWait;
use common_exception::Result;
use common_planners::Extras;

use crate::datasources::table::fuse::util;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::MetaInfoReader;
use crate::datasources::table::fuse::SegmentInfo;
use crate::datasources::table::fuse::TableSnapshot;

struct TableSparseIndex {
    table_snapshot_loc: String,
    meta_reader: MetaInfoReader,
}

struct CacheMgr;

impl TableSparseIndex {
    pub fn open(
        table_snapshot: &TableSnapshot,
        meta_reader: &MetaInfoReader,
        _cache_mgr: &CacheMgr,
    ) -> Result<Self> {
        // FAKED, to be integrate with the real indexing layer
        let r = Self {
            table_snapshot_loc: util::snapshot_location(
                table_snapshot.snapshot_id.to_simple().to_string().as_str(), // TODO refine this
            ),
            meta_reader: meta_reader.clone(),
        };
        Ok(r)
    }

    // Returns an iterator or stream would be better
    pub fn apply(&self, _expression: &Option<Extras>) -> Result<Vec<BlockMeta>> {
        // FAKED, to be integrate with the real indexing layer
        let snapshot: TableSnapshot = common_dal::read_obj(
            self.meta_reader.data_accessor(),
            self.table_snapshot_loc.clone(),
        )
        .wait_in(self.meta_reader.runtime(), None)??;
        let metas = snapshot
            .segments
            .iter()
            .map(|seg_loc| {
                let segment: SegmentInfo = common_dal::read_obj(
                    self.meta_reader.data_accessor().clone(),
                    seg_loc.to_string(),
                )
                .wait_in(self.meta_reader.runtime(), None)??;
                Ok(segment.blocks)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(metas.into_iter().flatten().collect())
    }
}

pub fn range_filter(
    table_snapshot: &TableSnapshot,
    push_down: &Option<Extras>,
    meta_reader: MetaInfoReader,
) -> Result<Vec<BlockMeta>> {
    let cache_mgr = CacheMgr; // TODO passed in from context
    let range_index = TableSparseIndex::open(table_snapshot, &meta_reader, &cache_mgr)?;
    range_index.apply(push_down)
}
