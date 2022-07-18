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

use common_exception::Result;
use common_meta_app::schema::TableStatistics;
use common_planners::OptimizeTablePlan;

use super::mutation::CompactMutator;
use crate::sessions::query_ctx::QryCtx;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::DEFAULT_BLOCK_PER_SEGMENT;
use crate::storages::fuse::DEFAULT_ROW_PER_BLOCK;
use crate::storages::fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::storages::fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::storages::storage_table::Table;

impl FuseTable {
    pub async fn do_compact(&self, ctx: Arc<dyn QryCtx>, plan: &OptimizeTablePlan) -> Result<()> {
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

        let new_snapshot = mutator.compact(self).await?;
        let mut new_table_meta = self.get_table_info().meta.clone(); // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: new_snapshot.summary.row_count,
            data_bytes: new_snapshot.summary.uncompressed_byte_size,
            compressed_data_bytes: new_snapshot.summary.compressed_byte_size,
            index_data_bytes: 0, // TODO we do not have it yet
        };
        let ctx: &dyn QryCtx = ctx.as_ref();
        self.update_table_meta(ctx, &plan.catalog, &new_snapshot, &mut new_table_meta)
            .await
    }
}
