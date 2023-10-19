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

use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::Partitions;
use common_catalog::table::CompactTarget;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::Pipeline;
use common_sql::executor::CommitSink;
use common_sql::executor::CompactSource;
use common_sql::executor::Exchange;
use common_sql::executor::FragmentKind;
use common_sql::executor::MutationKind;
use common_sql::executor::PhysicalPlan;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;
use common_storages_factory::NavigationPoint;
use storages_common_table_meta::meta::TableSnapshot;

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

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        match self.plan.action.clone() {
            OptimizeTableAction::CompactBlocks => {
                self.build_pipeline(CompactTarget::Blocks, false).await
            }
            OptimizeTableAction::CompactSegments => {
                self.build_pipeline(CompactTarget::Segments, false).await
            }
            OptimizeTableAction::Purge(point) => {
                purge(ctx, plan, point).await?;
                Ok(PipelineBuildResult::create())
            }
            OptimizeTableAction::All => self.build_pipeline(CompactTarget::Blocks, true).await,
        }
    }
}

impl OptimizeTableInterpreter {
    pub fn build_physical_plan(
        parts: Partitions,
        table_info: TableInfo,
        snapshot: Arc<TableSnapshot>,
        catalog_info: CatalogInfo,
        is_distributed: bool,
    ) -> Result<PhysicalPlan> {
        let merge_meta = parts.is_lazy;
        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
        }));

        if is_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
                ignore_exchange: false,
            });
        }

        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            table_info,
            catalog_info,
            snapshot,
            mutation_kind: MutationKind::Compact,
            merge_meta,
        })))
    }

    async fn build_pipeline(
        &self,
        target: CompactTarget,
        need_purge: bool,
    ) -> Result<PipelineBuildResult> {
        let mut table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;

        let table_info = table.get_table_info().clone();
        // check if the table is locked.
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let reply = catalog
            .list_table_lock_revs(table_info.ident.table_id)
            .await?;
        if !reply.is_empty() {
            return Err(ErrorCode::TableAlreadyLocked(format!(
                "table '{}' is locked, please retry compaction later",
                self.plan.table
            )));
        }

        if matches!(target, CompactTarget::Segments) {
            table
                .compact_segments(self.ctx.clone(), self.plan.limit)
                .await?;
            return Ok(PipelineBuildResult::create());
        }

        let res = table
            .compact_blocks(self.ctx.clone(), self.plan.limit)
            .await?;

        let is_distributed = (!self.ctx.get_cluster().is_empty())
            && self.ctx.get_settings().get_enable_distributed_compact()?;

        let catalog_info = catalog.info();
        let mut compact_pipeline = if let Some((parts, snapshot)) = res {
            let physical_plan = Self::build_physical_plan(
                parts,
                table_info,
                snapshot,
                catalog_info,
                is_distributed,
            )?;

            let build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                    .await?;
            build_res.main_pipeline
        } else {
            Pipeline::create()
        };

        let mut build_res = PipelineBuildResult::create();
        let settings = self.ctx.get_settings();
        let mut reclustered_block_count = 0;
        let need_recluster = !table.cluster_keys(self.ctx.clone()).is_empty();
        if need_recluster {
            if !compact_pipeline.is_empty() {
                compact_pipeline.set_max_threads(settings.get_max_threads()? as usize);

                let query_id = self.ctx.get_id();
                let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
                let executor =
                    PipelineCompleteExecutor::try_create(compact_pipeline, executor_settings)?;

                self.ctx.set_executor(executor.get_inner())?;
                executor.execute()?;

                // refresh table.
                table = table.as_ref().refresh(self.ctx.as_ref()).await?;
            }

            reclustered_block_count = table
                .recluster(
                    self.ctx.clone(),
                    None,
                    self.plan.limit,
                    &mut build_res.main_pipeline,
                )
                .await?;
        } else {
            build_res.main_pipeline = compact_pipeline;
        }

        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        if build_res.main_pipeline.is_empty() {
            if need_purge {
                purge(ctx, plan, None).await?;
            }
        } else {
            let start = SystemTime::now();
            build_res
                .main_pipeline
                .set_on_finished(move |may_error| match may_error {
                    None => {
                        if need_recluster {
                            InterpreterClusteringHistory::write_log(
                                &ctx,
                                start,
                                &plan.database,
                                &plan.table,
                                reclustered_block_count,
                            )?;
                        }
                        if need_purge {
                            GlobalIORuntime::instance()
                                .block_on(async move { purge(ctx, plan, None).await })?;
                        }
                        Ok(())
                    }
                    Some(error_code) => Err(error_code.clone()),
                });
        }

        Ok(build_res)
    }
}

async fn purge(
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
    instant: Option<NavigationPoint>,
) -> Result<()> {
    // currently, context caches the table, we have to "refresh"
    // the table by using the catalog API directly
    let table = ctx
        .get_catalog(&plan.catalog)
        .await?
        .get_table(ctx.get_tenant().as_str(), &plan.database, &plan.table)
        .await?;

    let keep_latest = true;
    let res = table
        .purge(ctx, instant, plan.limit, keep_latest, false)
        .await?;
    assert!(res.is_none());
    Ok(())
}
