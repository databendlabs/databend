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
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::CompactTarget;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::executor::physical_plans::ReclusterInfoSideCar;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::plans::OptimizeTableAction;
use databend_common_sql::plans::OptimizeTablePlan;
use databend_common_storages_factory::NavigationPoint;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
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

        let compact_target = match self.plan.action.clone() {
            OptimizeTableAction::CompactBlocks(limit) => CompactTarget::Blocks(limit),
            OptimizeTableAction::CompactSegments => CompactTarget::Segments,
            OptimizeTableAction::Purge(point) => {
                purge(ctx, catalog, plan, point).await?;
                return Ok(PipelineBuildResult::create());
            }
            OptimizeTableAction::All => CompactTarget::Blocks(None),
        };

        let mut build_res = self.build_pipeline(catalog.clone(), compact_target).await?;
        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        if matches!(self.plan.action, OptimizeTableAction::All) {
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
        Ok(build_res)
    }
}

impl OptimizeTableInterpreter {
    pub fn build_physical_plan(
        parts: Partitions,
        table_info: TableInfo,
        snapshot: Arc<TableSnapshot>,
        is_distributed: bool,
    ) -> Result<PhysicalPlan> {
        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            plan_id: u32::MAX,
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
            recluster_info: None,
        })))
    }

    async fn build_pipeline(
        &self,
        catalog: Arc<dyn Catalog>,
        target: CompactTarget,
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

        let table = catalog
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

        let table_info = table.get_table_info().clone();
        let do_recluster = !table.cluster_keys(self.ctx.clone()).is_empty();
        let mut physical_plan = if do_recluster {
            let Some((parts, snapshot)) = table
                .recluster(self.ctx.clone(), None, self.plan.limit)
                .await?
            else {
                return Ok(PipelineBuildResult::create());
            };
            if parts.is_empty() {
                return Ok(PipelineBuildResult::create());
            }

            let is_distributed = parts.is_distributed(self.ctx.clone());
            match parts {
                ReclusterParts::Recluster {
                    tasks,
                    remained_blocks,
                    removed_segment_indexes,
                    removed_segment_summary,
                } => {
                    let mut root = PhysicalPlan::Recluster(Box::new(Recluster {
                        tasks,
                        table_info: table_info.clone(),
                        plan_id: u32::MAX,
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
                    PhysicalPlan::CommitSink(Box::new(CommitSink {
                        input: Box::new(root),
                        table_info,
                        snapshot: Some(snapshot),
                        mutation_kind: MutationKind::Recluster,
                        update_stream_meta: vec![],
                        merge_meta: false,
                        deduplicated_label: None,
                        plan_id: u32::MAX,
                        recluster_info: Some(ReclusterInfoSideCar {
                            merged_blocks: remained_blocks,
                            removed_segment_indexes,
                            removed_statistics: removed_segment_summary,
                        }),
                    }))
                }
                ReclusterParts::Compact(parts) => {
                    Self::build_physical_plan(parts, table_info, snapshot, is_distributed)?
                }
            }
        } else {
            let res = table
                .compact_blocks(self.ctx.clone(), compaction_limits)
                .await?;

            let compact_is_distributed = (!self.ctx.get_cluster().is_empty())
                && self.ctx.get_settings().get_enable_distributed_compact()?;

            if let Some((parts, snapshot)) = res {
                Self::build_physical_plan(parts, table_info, snapshot, compact_is_distributed)?
            } else {
                return Ok(PipelineBuildResult::create());
            }
        };
        physical_plan.adjust_plan_id(&mut 0);

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        if do_recluster {
            let start = SystemTime::now();
            let database = self.plan.database.clone();
            let table = self.plan.table.clone();
            let ctx = self.ctx.clone();
            build_res
                .main_pipeline
                .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                    Ok(_) => {
                        let _ =
                            InterpreterClusteringHistory::write_log(&ctx, start, &database, &table);
                        Ok(())
                    }
                    Err(error_code) => Err(error_code.clone()),
                });
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
