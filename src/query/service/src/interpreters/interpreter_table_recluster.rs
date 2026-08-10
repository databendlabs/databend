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
use std::time::Duration;
use std::time::SystemTime;

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::TypeChecker;
use databend_common_sql::bind_table;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_storages_fuse::FUSE_OPT_KEY_AGGRESSIVE_RECLUSTER;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::ReclusterFinalCarry;
use databend_common_storages_fuse::operations::ReclusterMode;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;
use log::error;
use log::warn;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::physical_plans::CommitSink;
use crate::physical_plans::CommitType;
use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::Recluster;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextQueryState;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::sessions::TableContextTelemetry;

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ReclusterPlan,
    lock_opt: LockTableOption,
}

impl ReclusterTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: ReclusterPlan,
        lock_opt: LockTableOption,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            plan,
            lock_opt,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for ReclusterTableInterpreter {
    fn name(&self) -> &str {
        "ReclusterTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let recluster_timeout_secs = ctx.get_settings().get_recluster_timeout_secs()?;

        let mut times = 0;
        let mut push_downs = None;
        // FINAL carry is scoped to this fixed-scan statement loop.
        // A new FINAL statement starts from the table head again.
        let mut linear_final_carry = ReclusterFinalCarry::default();
        let start = SystemTime::now();
        let timeout = Duration::from_secs(recluster_timeout_secs);
        let is_final = self.plan.is_final;
        loop {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "recluster: statement aborted, server is shutting down or the query was killed, round={}",
                    times + 1
                );
                return Err(err.with_context("failed to execute"));
            }

            let res = self
                .execute_recluster(&mut push_downs, &mut linear_final_carry)
                .await;

            match res {
                Ok(is_break) => {
                    if is_break {
                        debug!(
                            "recluster: final loop stop reason=no_recluster_parts round={}",
                            times + 1,
                        );
                        break;
                    }
                }
                Err(e) => {
                    if is_final
                        && matches!(
                            e.code(),
                            ErrorCode::TABLE_LOCK_EXPIRED
                                | ErrorCode::TABLE_ALREADY_LOCKED
                                | ErrorCode::TABLE_VERSION_MISMATCHED
                                | ErrorCode::UNRESOLVABLE_CONFLICT
                        )
                    {
                        // Keep FINAL carry across retryable conflicts. FINAL is
                        // a bounded fixed scan and does not restart from table
                        // head to chase concurrent snapshot drift.
                        warn!(
                            "recluster: final loop retry reason=retryable_conflict round={} code={} error={:?}",
                            times + 1,
                            e.code(),
                            e,
                        );
                    } else {
                        error!(
                            "recluster: final loop stop reason=error round={} code={} error={:?}",
                            times + 1,
                            e.code(),
                            e,
                        );
                        return Err(e);
                    }
                }
            }

            let elapsed_time = SystemTime::now().duration_since(start).unwrap_or_default();
            times += 1;
            // Status.
            {
                let status = format!(
                    "[FUSE-RECLUSTER] Run recluster tasks:{} times, cost:{:?}",
                    times, elapsed_time
                );
                ctx.set_status_info(&status);
            }

            if !is_final {
                break;
            }

            if elapsed_time >= timeout {
                warn!(
                    "recluster: final loop stop reason=timeout round={} timeout={:?}",
                    times, timeout,
                );
                break;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

impl ReclusterTableInterpreter {
    async fn execute_recluster(
        &self,
        push_downs: &mut Option<PushDownInfo>,
        linear_final_carry: &mut ReclusterFinalCarry,
    ) -> Result<bool> {
        self.ctx.clear_table_meta_timestamps_cache();
        let start = SystemTime::now();
        let settings = self.ctx.get_settings();

        let ReclusterPlan {
            catalog,
            database,
            table,
            limit,
            ..
        } = &self.plan;
        // try to add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(catalog, database, table, &self.lock_opt)
            .await?;

        let tbl = self.ctx.get_table(catalog, database, table).await?;
        // check mutability
        tbl.check_mutable()?;
        if tbl.cluster_key_meta().is_none() {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}.{}'",
                database, table,
            )));
        }

        self.build_push_downs(push_downs, &tbl)?;

        let physical_plan = self
            .build_linear_plan(tbl.as_ref(), push_downs, *limit, linear_final_carry)
            .await?;
        let Some(mut physical_plan) = physical_plan else {
            return Ok(true);
        };
        physical_plan.adjust_plan_id(&mut 0);
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        {
            let ctx = self.ctx.clone();
            let catalog = self.plan.catalog.clone();
            let database = self.plan.database.clone();
            let table = self.plan.table.clone();
            build_res.main_pipeline.set_on_finished(always_callback(
                move |info: &ExecutionInfo| {
                    ctx.written_segment_locations().clear();
                    ctx.evict_table_from_cache(&catalog, &database, &table)?;

                    ctx.unload_spill_meta();
                    hook_clear_m_cte_temp_table(&ctx)?;
                    hook_vacuum_temp_files(&ctx)?;
                    // RECLUSTER FINAL runs independent pipelines under one query id.
                    // We allow hook vacuum to be best-effort for each round, and avoid
                    // carrying this round's spill progress into later rounds.
                    // Leftovers beyond the hook vacuum limit are handled by normal vacuum.
                    ctx.clear_cluster_spill_progress();
                    hook_disk_temp_dir(&ctx)?;
                    match &info.res {
                        Ok(_) => {
                            InterpreterClusteringHistory::write_log(
                                &ctx, start, &database, &table,
                            )?;

                            Ok(())
                        }
                        Err(error) => Err(error.clone()),
                    }
                },
            ));
        }

        debug_assert!(build_res.main_pipeline.is_complete_pipeline()?);

        let max_threads = settings.get_max_threads()? as usize;
        build_res.set_max_threads(max_threads);

        let executor_settings = ExecutorSettings::try_create(self.ctx.clone())?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor =
            PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
        self.ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute().await?;

        // make sure the executor is dropped before the next loop.
        drop(complete_executor);
        // make sure the lock guard is dropped before the next loop.
        drop(lock_guard);

        Ok(false)
    }

    async fn build_linear_plan(
        &self,
        tbl: &dyn Table,
        push_downs: &mut Option<PushDownInfo>,
        limit: Option<usize>,
        linear_final_carry: &mut ReclusterFinalCarry,
    ) -> Result<Option<PhysicalPlan>> {
        let fuse_table = FuseTable::try_from_table(tbl)?;
        // Missing `aggressive_recluster` marks a pre-option clustered table. Keep
        // those tables on the conservative strategy until CREATE/ALTER CLUSTER BY
        // materializes the option with value 1.
        let mode = if self.plan.is_final
            && fuse_table.get_option(FUSE_OPT_KEY_AGGRESSIVE_RECLUSTER, 0u32) != 0
        {
            ReclusterMode::Aggressive
        } else {
            ReclusterMode::Conservative
        };
        let Some((parts, snapshot)) = fuse_table
            .do_recluster(
                self.ctx.clone(),
                push_downs.clone(),
                limit,
                mode,
                linear_final_carry,
            )
            .await?
        else {
            return Ok(None);
        };
        if parts.is_empty() {
            return Ok(None);
        }
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(tbl, Some(snapshot.clone()))?;

        let table_info = tbl.get_table_info().clone();
        let is_distributed = parts.is_distributed(self.ctx.clone());
        let ReclusterParts {
            tasks,
            remained_blocks,
            removed_segment_indexes,
            removed_segment_summary,
        } = parts;
        let root = PhysicalPlan::new(Recluster {
            tasks,
            table_meta_timestamps,

            table_info: table_info.clone(),
            meta: PhysicalPlanMeta::new("Recluster"),
        });

        let plan = Self::add_commit_sink(
            root,
            is_distributed,
            table_info,
            snapshot,
            false,
            Some(ReclusterInfoSideCar {
                merged_blocks: remained_blocks,
                removed_segment_indexes,
                removed_statistics: removed_segment_summary,
            }),
            table_meta_timestamps,
        );
        Ok(Some(plan))
    }

    fn build_push_downs(
        &self,
        push_downs: &mut Option<PushDownInfo>,
        tbl: &Arc<dyn Table>,
    ) -> Result<()> {
        if push_downs.is_none() {
            if let Some(expr) = &self.plan.selection {
                let settings = self.ctx.get_settings();
                let (mut bind_context, metadata) = bind_table(tbl.clone())?;
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let mut type_checker = TypeChecker::try_create(
                    &mut bind_context,
                    self.ctx.clone(),
                    &name_resolution_ctx,
                    metadata,
                    &[],
                    true,
                )?;
                let (scalar, _) = *type_checker.resolve(expr)?;
                // prepare the filter expression
                let filter = cast_expr_to_non_null_boolean(
                    scalar
                        .as_expr()?
                        .project_column_ref(|col| Ok(col.column_name.clone()))?,
                )?;
                // prepare the inverse filter expression
                let inverted_filter =
                    check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;
                *push_downs = Some(PushDownInfo {
                    filters: Some(Filters {
                        filter: filter.as_remote_expr(),
                        inverted_filter: inverted_filter.as_remote_expr(),
                    }),
                    ..PushDownInfo::default()
                });
            }
        }
        Ok(())
    }

    fn add_commit_sink(
        mut input: PhysicalPlan,
        is_distributed: bool,
        table_info: TableInfo,
        snapshot: Arc<TableSnapshot>,
        merge_meta: bool,
        recluster_info: Option<ReclusterInfoSideCar>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> PhysicalPlan {
        if is_distributed {
            input = PhysicalPlan::new(Exchange {
                input,
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("Exchange"),
            });
        }

        let mut kind = MutationKind::Compact;

        if recluster_info.is_some() {
            kind = MutationKind::Recluster
        }

        PhysicalPlan::new(CommitSink {
            input,
            table_info,
            snapshot: Some(snapshot),
            commit_type: CommitType::Mutation { kind, merge_meta },
            update_stream_meta: vec![],
            deduplicated_label: None,
            table_meta_timestamps,
            recluster_info,
            meta: PhysicalPlanMeta::new("CommitSink"),
        })
    }
}
