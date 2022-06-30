//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_cache::Cache;
use common_exception::Result;
use common_planners::OptimizeTablePlan;

use super::mutation::CompactMutator;
use crate::sessions::QueryContext;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use crate::storages::fuse::DEFAULT_ROW_PER_BLOCK;
use crate::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::storages::storage_table::Table;

impl FuseTable {
    pub async fn do_compact(&self, ctx: Arc<QueryContext>, plan: &OptimizeTablePlan) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot(ctx.as_ref()).await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(());
        };

        if snapshot.summary.block_count <= 1 {
            return Ok(());
        }

        let row_per_block = self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_ROW_PER_BLOCK);
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        let mut mutator = CompactMutator::try_create(
            &ctx,
            &self.meta_location_generator,
            &snapshot,
            row_per_block,
            block_per_seg,
        )?;

        mutator.compact(self).await?;
        let (new_snapshot, loc) = mutator.into_new_snapshot().await?;

        let operator = ctx.get_storage_operator()?;
        let result = Self::commit_to_meta_server(
            ctx.as_ref(),
            &plan.catalog,
            self.get_table_info(),
            loc.clone(),
            &new_snapshot.summary,
        )
        .await;

        match result {
            Ok(_) => {
                if let Some(snapshot_cache) =
                    ctx.get_storage_cache_manager().get_table_snapshot_cache()
                {
                    let cache = &mut snapshot_cache.write().await;
                    cache.put(loc, Arc::new(new_snapshot));
                }
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed, try to delete it.
                // "major GC" will collect this, if deletion failure (even after DAL retried)
                let _ = operator.object(&loc).delete().await;
                Err(e)
            }
        }
    }
}
