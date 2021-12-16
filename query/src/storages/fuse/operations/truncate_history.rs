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

use std::collections::HashSet;
use std::sync::Arc;

use common_dal::DataAccessor;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::storages::fuse::io;
use crate::storages::fuse::io::snapshot_history;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;

impl FuseTable {
    pub async fn do_truncate_history(
        &self,
        ctx: Arc<QueryContext>,
        keep_last_snapshot: bool,
    ) -> Result<()> {
        let da = ctx.get_data_accessor()?;
        let tbl_info = self.get_table_info();
        let snapshot_loc = tbl_info.meta.options.get(TBL_OPT_KEY_SNAPSHOT_LOC);
        let mut snapshots = snapshot_history(da.as_ref(), snapshot_loc, ctx.clone()).await?;

        let min_history_len = if !keep_last_snapshot { 0 } else { 1 };

        // short cut
        if snapshots.len() <= min_history_len {
            return Ok(());
        }

        let current_segments: HashSet<&String>;
        let current_snapshot: TableSnapshot;
        if !keep_last_snapshot {
            // if truncate_all requested, gc root contains nothing;
            current_segments = HashSet::new();
        } else {
            current_snapshot = snapshots.remove(0);
            current_segments = HashSet::from_iter(&current_snapshot.segments);
        }

        let prevs = snapshots.iter().fold(HashSet::new(), |mut acc, s| {
            acc.extend(&s.segments);
            acc
        });

        // segments which no longer need to be kept
        let seg_delta = prevs.difference(&current_segments).collect::<Vec<_>>();

        // blocks to be removed
        let prev_blocks: HashSet<String> = self
            .blocks_of(da.clone(), seg_delta.iter(), ctx.clone())
            .await?;
        let current_blocks: HashSet<String> = self
            .blocks_of(da.clone(), current_segments.iter(), ctx.clone())
            .await?;
        let block_delta = prev_blocks.difference(&current_blocks);

        // NOTE: the following actions are NOT transactional yet

        // 1. remove blocks
        for x in block_delta {
            self.remove_location(da.clone(), x).await?;
        }

        // 2. remove the segments
        for x in seg_delta {
            self.remove_location(da.clone(), x).await?;
        }

        // 3. remove the snapshots
        for x in snapshots.iter().rev() {
            let loc = snapshot_location(&x.snapshot_id);
            self.remove_location(da.clone(), loc).await?
        }

        Ok(())
    }

    async fn blocks_of(
        &self,
        data_accessor: Arc<dyn DataAccessor>,
        locations: impl Iterator<Item = impl AsRef<str>>,
        ctx: Arc<QueryContext>,
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();
        for x in locations {
            let res: SegmentInfo =
                io::read_obj(data_accessor.as_ref(), x, ctx.get_table_cache()).await?;
            for block_meta in res.blocks {
                result.insert(block_meta.location.path);
            }
        }
        Ok(result)
    }

    async fn remove_location(
        &self,
        data_accessor: Arc<dyn DataAccessor>,
        location: impl AsRef<str>,
    ) -> Result<()> {
        data_accessor.remove(location.as_ref()).await
    }
}
