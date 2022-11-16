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

use common_catalog::plan::Expression;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datavalues::DataField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::ExpressionParser;
use common_storages_table_meta::meta::TableSnapshot;
use common_storages_table_meta::meta::TableSnapshotStatistics;

use crate::operations::mutation::delete_from_block;
use crate::operations::mutation::deletion_mutator::Deletion;
use crate::operations::mutation::DeletionMutator;
use crate::pruning::BlockPruner;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        projection: &Projection,
        selection: &Option<String>,
    ) -> Result<()> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let prev_statistics_opt = self
            .read_table_snapshot_statistics(snapshot_opt.as_ref())
            .await?;

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

        let prev_statistics =
            prev_statistics_opt.map(|prev_statistics| prev_statistics.as_ref().clone());

        // check if unconditional deletion
        if let Some(filter) = &selection {
            let table_meta = Arc::new(self.clone());
            let physical_scalars = ExpressionParser::parse_exprs(table_meta, filter)?;
            if physical_scalars.is_empty() {
                return Err(ErrorCode::IndexOutOfBounds(
                    "expression should be valid, but not",
                ));
            }
            self.delete_rows(
                ctx.clone(),
                &snapshot,
                prev_statistics,
                &physical_scalars[0],
                projection,
            )
            .await
        } else {
            // deleting the whole table... just a truncate
            let purge = false;
            self.do_truncate(ctx.clone(), purge).await
        }
    }

    async fn delete_rows(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: &Arc<TableSnapshot>,
        statistics: Option<TableSnapshotStatistics>,
        filter: &Expression,
        projection: &Projection,
    ) -> Result<()> {
        let cluster_stats_gen = self.cluster_stats_gen()?;
        let mut deletion_collector = DeletionMutator::try_create(
            ctx.clone(),
            self.get_operator(),
            self.meta_location_generator.clone(),
            snapshot.clone(),
            statistics,
            cluster_stats_gen,
        )?;
        let schema = self.table_info.schema();
        // TODO refine pruner
        let extras = PushDownInfo {
            projection: Some(projection.clone()),
            filters: vec![filter.clone()],
            prewhere: None, // TBD: if delete rows need prewhere optimization
            limit: None,
            order_by: vec![],
            stage: None,
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
            let proj = projection.clone();
            match delete_from_block(
                self,
                &block_meta,
                &ctx,
                proj,
                filter,
                &mut deletion_collector.table_statistics,
            )
            .await?
            {
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
            del_holder.table_statistics,
            abort_operation,
        )
        .await
    }

    fn cluster_stats_gen(&self) -> Result<ClusterStatsGenerator> {
        if self.cluster_key_meta.is_none() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema = self.table_info.schema();
        let mut merged = input_schema.fields().clone();

        let cluster_keys = self.cluster_keys();
        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_index = Vec::with_capacity(cluster_keys.len());
        for expr in &cluster_keys {
            let cname = expr.column_name();
            let index = match merged.iter().position(|x| x.name() == &cname) {
                None => {
                    let field = DataField::new(&cname, expr.data_type());
                    merged.push(field);

                    extra_key_index.push(merged.len() - 1);
                    merged.len() - 1
                }
                Some(idx) => idx,
            };
            cluster_key_index.push(index);
        }

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_index,
            0,
            self.get_block_compact_thresholds(),
        ))
    }
}
