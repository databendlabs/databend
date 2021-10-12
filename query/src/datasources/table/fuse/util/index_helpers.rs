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

use common_exception::Result;
use common_planners::Extras;

use crate::datasources::table::fuse::BlockLocation;
use crate::datasources::table::fuse::MetaInfoReader;
use crate::datasources::table::fuse::TableSnapshot;

struct TableSparseIndex {}
struct CacheMgr;

impl TableSparseIndex {
    pub fn load(
        _table_snapshot: &TableSnapshot,
        _meta_reader: &MetaInfoReader,
        _cache_mgr: &CacheMgr,
    ) -> Result<Self> {
        // load index, which may be cached (or partially cached)
        todo!()
    }

    // Returns an iterator or stream would be better
    // let's begin with
    pub fn apply(&self, _expression: &Option<Extras>) -> Result<Vec<BlockLocation>> {
        // prunes blocks
        todo!()
    }
}

pub fn range_filter(
    table_snapshot: &TableSnapshot,
    push_down: &Option<Extras>,
    meta_reader: MetaInfoReader,
) -> Result<Vec<BlockLocation>> {
    let cache_mgr = CacheMgr; // TODO passed in from context
    let range_index = TableSparseIndex::load(table_snapshot, &meta_reader, &cache_mgr)?;
    range_index.apply(push_down)
}
