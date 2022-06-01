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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;
use common_planners::Extras;

use super::filter::delete_from_block;
use crate::sessions::QueryContext;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::operations::del::collector::Deletion;
use crate::storages::fuse::operations::del::collector::DeletionCollector;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;

impl FuseTable {
    pub async fn delete(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
        plan: &DeletePlan,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot(ctx.as_ref()).await?;

        // check if table is empty
        let snapshot = if let Some(s) = snapshot_opt {
            s
        } else {
            // no snapshot, no deletion
            return Ok(());
        };

        // check if unconditional deletion
        let filter = if let Some(Extras { filters, .. }) = &push_downs {
            if filters.len() != 1 {
                return Err(ErrorCode::LogicalError(
                    "there should be exact one expression(conj or disjunction) for the where-clause of a delete statement",
                ));
            } else {
                &filters[0]
            }
        } else {
            // deleting the whole table... just a truncate
            let purge = false;
            return self
                .do_truncate(ctx.clone(), purge, plan.catalog_name.as_str())
                .await;
        };

        self.delete_blocks(ctx, &snapshot, filter, push_downs, &plan.catalog_name)
            .await
    }

    async fn delete_blocks(
        &self,
        ctx: Arc<QueryContext>,
        snapshot: &Arc<TableSnapshot>,
        filter: &Expression,
        push_downs: &Option<Extras>,
        catalog_name: &str,
    ) -> Result<()> {
        let mut deletion_collector =
            DeletionCollector::new(ctx.as_ref(), &self.meta_location_generator, snapshot);
        let schema = self.table_info.schema();
        let block_metas = BlockPruner::new(snapshot.clone())
            .apply(ctx.as_ref(), schema, push_downs)
            .await?;

        // delete block one by one.
        // this could be executed in a distributed manner, till new planner, pipeline settled down totally
        for (seg_idx, block_meta) in block_metas {
            let proj = self.projection_of_push_downs(push_downs);
            match delete_from_block(self, &block_meta, &ctx, proj, filter).await? {
                Deletion::NothingDeleted => {
                    // false positive, we should keep the whole block
                    continue;
                }
                Deletion::Remains(r) => {
                    // after deletion, the data block `r` remains, let keep it  by replacing the block
                    // located at `block_meta.location`, of segment indexed by `seg_idx`, with a new block `r`
                    deletion_collector
                        .replace_with(seg_idx, &block_meta.location, r)
                        .await?
                }
            }
        }
        self.commit_deletion(ctx.as_ref(), deletion_collector, catalog_name)
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
