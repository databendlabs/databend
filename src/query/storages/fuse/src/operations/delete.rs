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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::TableSnapshot;
use common_planner::extras::Extras;
use common_planner::plans::DeletePlan;
use common_planner::PhysicalScalar;
use common_sql::PhysicalScalarParser;
use tracing::debug;

use crate::operations::mutation::delete_from_block;
use crate::operations::mutation::deletion_mutator::Deletion;
use crate::operations::mutation::DeletionMutator;
use crate::pruning::BlockPruner;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_delete(&self, ctx: Arc<dyn TableContext>, plan: &DeletePlan) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;

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
            let physical_scalars =
                PhysicalScalarParser::parse_exprs(ctx.clone(), plan.schema(), filter).await?;
            if physical_scalars.is_empty() {
                return Err(ErrorCode::IndexOutOfBounds(
                    "expression should be valid, but not",
                ));
            }
            self.delete_rows(ctx.clone(), &snapshot, &physical_scalars[0], plan)
                .await
        } else {
            // deleting the whole table... just a truncate
            let purge = false;
            debug!(
                "unconditionally delete from table, {}.{}.{}",
                plan.catalog_name, plan.database_name, plan.table_name
            );
            self.do_truncate(ctx.clone(), purge).await
        }
    }

    async fn delete_rows(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: &Arc<TableSnapshot>,
        filter: &PhysicalScalar,
        plan: &DeletePlan,
    ) -> Result<()> {
        let cluster_stats_gen = self.cluster_stats_gen(ctx.clone())?;
        let mut deletion_collector = DeletionMutator::try_create(
            ctx.clone(),
            self.get_operator(),
            self.meta_location_generator.clone(),
            snapshot.clone(),
            cluster_stats_gen,
        )?;
        let schema = self.table_info.schema();
        // TODO refine pruner
        let extras = Extras {
            projection: Some(plan.projection.clone()),
            filters: vec![filter.clone()],
            prewhere: None, // TBD: if delete rows need prewhere optimization
            limit: None,
            order_by: vec![],
        };
        let push_downs = Some(extras);
        let segments_location = snapshot.segments.clone();
        let block_metas = BlockPruner::prune(
            &ctx,
            self.operator.clone(),
            schema,
            &push_downs,
            segments_location,
        )
        .await?;

        // delete block one by one.
        // this could be executed in a distributed manner (till new planner, pipeline settled down)
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
                        .replace_with(
                            seg_idx,
                            block_meta.location.clone(),
                            block_meta.cluster_stats.clone(),
                            r,
                        )
                        .await?
                }
            }
        }

        self.commit_deletion(ctx, deletion_collector).await
    }

    async fn commit_deletion(
        &self,
        ctx: Arc<dyn TableContext>,
        del_holder: DeletionMutator,
    ) -> Result<()> {
        let (segments, summary, abort_operation) = del_holder.generate_segments().await?;

        self.commit_mutation(
            &ctx,
            del_holder.base_snapshot(),
            segments,
            summary,
            abort_operation,
        )
        .await
    }

    fn cluster_stats_gen(&self, ctx: Arc<dyn TableContext>) -> Result<ClusterStatsGenerator> {
        // todo(sundy)
        todo!()
    }
}
