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

use crate::datasources::fuse_table::meta::meta_info_reader::MetaInfoReader;
use crate::datasources::fuse_table::meta::table_snapshot::BlockLocation;
use crate::datasources::fuse_table::meta::table_snapshot::TableSnapshot;

struct TableSparseIndex {}
struct CacheMgr;

// non-distributed indexing
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
    pub fn apply(&self, _expression: &Extras) -> Result<Vec<BlockLocation>> {
        // prunes blocks
        todo!()
    }
}

pub fn range_filter(
    table_snapshot: &TableSnapshot,
    push_down: &Extras,
    // MetaInfoReader takes care of caching itself
    meta_reader: MetaInfoReader,
) -> Result<Vec<BlockLocation>> {
    let cache_mgr = CacheMgr; // TODO passed in from context
    let range_index = TableSparseIndex::load(table_snapshot, &meta_reader, &cache_mgr)?;
    range_index.apply(push_down)

    /*
    for seg_loc in &table_snapshot.segments {
        // TODO filter by seg.summary
        let seg = meta_reader.read_segment_info(seg_loc)?;
        let _seg_summary = &seg.summary;
        for x in seg.blocks {
            // filter by block_meta's ColStats if necessary
            let _col_stats = &x.col_stats;
            // keep the block we are fond of
            res.push(x);
        }
    }

    Ok(res)
    */
}
