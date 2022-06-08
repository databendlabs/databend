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

use std::sync::Arc;

use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;
use common_planners::Extras;
use common_tracing::tracing::debug;

use crate::sessions::QueryContext;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::operations::mutation::delete_from_block;
use crate::storages::fuse::operations::mutation::mutations_collector::Deletion;
use crate::storages::fuse::operations::mutation::mutations_collector::DeletionCollector;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;

impl FuseTable {
    pub async fn do_delete(&self, ctx: Arc<QueryContext>, plan: &DeletePlan) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot(ctx.as_ref()).await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no deletion
            return Ok(());
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(());
        }

        // check if unconditional deletion
        if let Some(filter) = &plan.selection {
            self.delete_rows(ctx, &snapshot, filter, plan).await
        } else {
            // deleting the whole table... just a truncate
            let purge = false;
            debug!(
                "unconditionally delete from table, {}.{}.{}",
                plan.catalog_name, plan.database_name, plan.table_name
            );
            return self
                .do_truncate(ctx.clone(), purge, plan.catalog_name.as_str())
                .await;
        }
    }

    async fn delete_rows(
        &self,
        ctx: Arc<QueryContext>,
        snapshot: &Arc<TableSnapshot>,
        filter: &Expression,
        plan: &DeletePlan,
    ) -> Result<()> {
        let mut deletion_collector =
            DeletionCollector::try_create(ctx.as_ref(), &self.meta_location_generator, snapshot)?;
        let schema = self.table_info.schema();
        // TODO refine pruner
        let extras = Extras {
            projection: Some(plan.projection.clone()),
            filters: vec![filter.clone()],
            limit: None,
            order_by: vec![],
        };
        let push_downs = Some(extras);
        let block_metas = BlockPruner::new(snapshot.clone())
            .apply(ctx.as_ref(), schema, &push_downs)
            .await?;

        // delete block one by one.
        // this could be executed in a distributed manner, till new planner, pipeline settled down totally
        for (seg_idx, block_meta) in block_metas {
            let proj = plan.projection.clone();
            match delete_from_block(self, &block_meta, &ctx, proj, filter).await? {
                Deletion::NothingDeleted => {
                    // false positive, we should keep the whole block
                    continue;
                }
                Deletion::Remains(r) => {
                    // after deletion, the data block `r` remains, let keep it  by replacing the block
                    // located at `block_meta.location`, of segment indexed by `seg_idx`, with a new block `r`
                    deletion_collector
                        .replace_with(seg_idx, block_meta.location.clone(), r)
                        .await?
                }
            }
        }
        self.commit_deletion(ctx.as_ref(), deletion_collector, &plan.catalog_name)
            .await
    }

    async fn commit_deletion(
        &self,
        ctx: &QueryContext,
        del_holder: DeletionCollector<'_>,
        catalog_name: &str,
    ) -> Result<()> {
        let (new_snapshot, loc) = del_holder.into_new_snapshot().await?;
        Self::commit_to_meta_server(
            ctx,
            catalog_name,
            self.get_table_info(),
            loc,
            &new_snapshot.summary,
        )
        .await?;
        // TODO check if error is recoverable, and try to resolve the conflict
        Ok(())
    }
}
