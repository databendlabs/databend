// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::SystemTime;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::CompactTarget;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::plans::OptimizeTableAction;
use databend_common_sql::plans::OptimizeTablePlan;
use databend_common_storages_factory::NavigationPoint;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::interpreters::interpreter_table_recluster::build_recluster_physical_plan;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<Self> {
        Ok(OptimizeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let plan = self.plan.clone();

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        match self.plan.action.clone() {
            OptimizeTableAction::CompactBlocks(limit) => {
                self.build_pipeline(catalog, CompactTarget::Blocks(limit), false)
                    .await
            }
            OptimizeTableAction::CompactSegments => {
                self.build_pipeline(catalog, CompactTarget::Segments, false)
                    .await
            }
            OptimizeTableAction::Purge(point) => {
                purge(ctx, catalog, plan, point).await?;
                Ok(PipelineBuildResult::create())
            }
            OptimizeTableAction::All => {
                self.build_pipeline(catalog, CompactTarget::Blocks(None), true)
                    .await
            }
        }
    }
}

impl OptimizeTableInterpreter {
    pub fn build_physical_plan(
        ctx: &dyn TableContext,
        parts: Partitions,
        table_info: TableInfo,
        snapshot: Arc<TableSnapshot>,
        is_distributed: bool,
    ) -> Result<PhysicalPlan> {
        let base_snapshot_timestamp = ctx
            .txn_mgr()
            .lock()
            .get_base_snapshot_timestamp(table_info.ident.table_id, snapshot.timestamp);
        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            plan_id: u32::MAX,
            base_snapshot_timestamp,
        }));

        if is_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
        }

        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            table_info,
            snapshot: Some(snapshot),
            mutation_kind: MutationKind::Compact,
            update_stream_meta: vec![],
            merge_meta,
            deduplicated_label: None,
            plan_id: u32::MAX,
            base_snapshot_timestamp,
        })))
    }

    async fn build_pipeline(
        &self,
        catalog: Arc<dyn Catalog>,
        target: CompactTarget,
        need_purge: bool,
    ) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                &self.plan.lock_opt,
            )
            .await?;

        let mut table = catalog
            .get_table(&tenant, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        let compaction_limits = match target {
            CompactTarget::Segments => {
                table
                    .compact_segments(self.ctx.clone(), self.plan.limit)
                    .await?;
                return Ok(PipelineBuildResult::create());
            }
            CompactTarget::Blocks(num_block_limit) => {
                let segment_limit = self.plan.limit;
                CompactionLimits::limits(segment_limit, num_block_limit)
            }
        };

        let res = table
            .compact_blocks(self.ctx.clone(), compaction_limits)
            .await?;

        let compact_is_distributed = (!self.ctx.get_cluster().is_empty())
            && self.ctx.get_settings().get_enable_distributed_compact()?;

        // build the compact pipeline.
        let mut compact_pipeline = if let Some((parts, snapshot)) = res {
            let physical_plan = Self::build_physical_plan(
                self.ctx.as_ref(),
                parts,
                table.get_table_info().clone(),
                snapshot,
                compact_is_distributed,
            )?;

            let build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
            build_res.main_pipeline
        } else {
            Pipeline::create()
        };

        // build the recluster pipeline.
        let mut build_res = PipelineBuildResult::create();
        let settings = self.ctx.get_settings();
        // check if the table need recluster, defined by cluster keys.
        let need_recluster = !table.cluster_keys(self.ctx.clone()).is_empty();
        if need_recluster {
            if !compact_pipeline.is_empty() {
                compact_pipeline.set_max_threads(settings.get_max_threads()? as usize);

                let executor_settings = ExecutorSettings::try_create(self.ctx.clone())?;
                let executor =
                    PipelineCompleteExecutor::try_create(compact_pipeline, executor_settings)?;

                self.ctx.set_executor(executor.get_inner())?;
                executor.execute()?;
                // Make sure the executor is dropped before recluster.
                drop(executor);

                // refresh table.
                table = catalog
                    .get_table(&tenant, &self.plan.database, &self.plan.table)
                    .await?;
            }

            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            if let Some(mutator) = fuse_table
                .build_recluster_mutator(self.ctx.clone(), None, self.plan.limit)
                .await?
            {
                if !mutator.tasks.is_empty() {
                    let is_distributed = mutator.is_distributed();
                    let reclustered_block_count = mutator.recluster_blocks_count;
                    let physical_plan = build_recluster_physical_plan(
                        self.ctx.as_ref(),
                        mutator.tasks,
                        table.get_table_info().clone(),
                        mutator.snapshot,
                        is_distributed,
                    )?;

                    build_res =
                        build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan)
                            .await?;

                    let ctx = self.ctx.clone();
                    let plan = self.plan.clone();
                    let start = SystemTime::now();
                    build_res.main_pipeline.set_on_finished(
                        move |info: &ExecutionInfo| match &info.res {
                            Ok(_) => InterpreterClusteringHistory::write_log(
                                &ctx,
                                start,
                                &plan.database,
                                &plan.table,
                                reclustered_block_count,
                            ),
                            Err(error_code) => Err(error_code.clone()),
                        },
                    );
                }
            }
        } else {
            build_res.main_pipeline = compact_pipeline;
        }

        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        if need_purge {
            if build_res.main_pipeline.is_empty() {
                purge(ctx, catalog, plan, None).await?;
            } else {
                build_res
                    .main_pipeline
                    .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                        Ok(_) => GlobalIORuntime::instance()
                            .block_on(async move { purge(ctx, catalog, plan, None).await }),
                        Err(error_code) => Err(error_code.clone()),
                    });
            }
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        Ok(build_res)
    }
}

async fn purge(
    ctx: Arc<QueryContext>,
    catalog: Arc<dyn Catalog>,
    plan: OptimizeTablePlan,
    instant: Option<NavigationPoint>,
) -> Result<()> {
    // currently, context caches the table, we have to "refresh"
    // the table by using the catalog API directly
    let table = catalog
        .get_table(&ctx.get_tenant(), &plan.database, &plan.table)
        .await?;
    // check mutability
    table.check_mutable()?;

    let keep_latest = true;
    let res = table
        .purge(ctx, instant, plan.limit, keep_latest, false)
        .await?;
    assert!(res.is_none());
    Ok(())
}
