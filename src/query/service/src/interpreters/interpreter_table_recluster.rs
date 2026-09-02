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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_metrics::storage::metrics_inc_segment_claim_conflicts;
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
use databend_common_storages_fuse::operations::is_auto_vacuum_enabled;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;
use log::error;
use log::warn;
use rand::Rng;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::interpreters::common::check_maintenance_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::locks::CoordinationManager;
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
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextQueryState;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::sessions::TableContextTelemetry;

const MAX_SEGMENT_CLAIM_RETRIES: usize = 3;

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
        let mut committed = false;
        let result = loop {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "recluster: statement aborted, server is shutting down or the query was killed, round={}",
                    times + 1
                );
                break Err(err.with_context("failed to execute"));
            }

            let res = self
                .execute_recluster(&mut push_downs, &mut linear_final_carry)
                .await;

            match res {
                Ok(true) => {
                    debug!(
                        "recluster: final loop stop reason=no_recluster_parts round={}",
                        times + 1,
                    );
                    break Ok(());
                }
                Ok(false) => committed = true,
                Err(e) => {
                    if is_final
                        && matches!(
                            e.code(),
                            ErrorCode::LEASE_EXPIRED
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
                        break Err(e);
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
                break Ok(());
            }

            if elapsed_time >= timeout {
                warn!(
                    "recluster: final loop stop reason=timeout round={} timeout={:?}",
                    times, timeout,
                );
                break Ok(());
            }
        };

        if committed {
            self.vacuum_table_history().await;
        }

        result?;
        Ok(PipelineBuildResult::create())
    }
}

impl ReclusterTableInterpreter {
    async fn vacuum_table_history(&self) {
        if LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)
            .is_err()
        {
            return;
        }

        let result = async {
            // RECLUSTER resolves the table by name for every round. The deferred vacuum follows
            // the same name-bound semantics and bypasses the query-level table cache instead of
            // pinning the table ID for the statement.
            let table = self
                .ctx
                .get_catalog(&self.plan.catalog)
                .await?
                .get_table(
                    &self.ctx.get_tenant(),
                    &self.plan.database,
                    &self.plan.table,
                )
                .await?;
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            // Transient tables retain their existing per-commit PurgeAllHistory behavior.
            if fuse_table.is_transient() {
                return Ok::<_, ErrorCode>(());
            }
            if is_auto_vacuum_enabled(self.ctx.as_ref(), fuse_table)? {
                fuse_table
                    .vacuum_table(self.ctx.clone(), &get_vacuum_handler(), true)
                    .await;
            }
            Ok::<_, ErrorCode>(())
        }
        .await;

        if let Err(error) = result {
            warn!("recluster: final table-history vacuum failed: {error}");
        }
    }

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
        // `NoLock` means the caller already owns the lifecycle lock (for example, MV refresh).
        // Otherwise table and materialized-view reclustering initially use segment claims while
        // planning and the shared table lock as a short commit gate.
        let mut use_segment_claims =
            settings.get_enable_table_lock()? && self.lock_opt != LockTableOption::NoLock;
        let outer_lock_guard = if use_segment_claims {
            None
        } else {
            self.ctx
                .clone()
                .acquire_table_lock(catalog, database, table, &self.lock_opt)
                .await?
        };

        // The former outer table lock evicted this cache. Concurrent planning must do
        // that explicitly so the claim set is matched against the latest snapshot.
        if use_segment_claims {
            self.ctx.evict_table_from_cache(catalog, database, table)?;
        }
        let mut tbl = self.ctx.get_table(catalog, database, table).await?;
        // Temporary tables are session-local, and their IDs are intentionally unsupported by
        // the segment-claim and lock catalog APIs.
        if tbl.is_temp() {
            use_segment_claims = false;
        }
        let claim_manager = use_segment_claims.then(CoordinationManager::instance);
        let mut claim_retries = 0;
        let (parts, snapshot, claim_guard) = loop {
            check_maintenance_target(tbl.as_ref(), &self.plan.target)?;
            if tbl.cluster_key_meta().is_none() {
                return Err(ErrorCode::UnclusteredTable(format!(
                    "Unclustered table '{}.{}'",
                    database, table,
                )));
            }
            self.build_push_downs(push_downs, &tbl)?;

            let claimed_segments = if let Some(claim_manager) = &claim_manager {
                claim_manager
                    .claimed_segments(self.ctx.as_ref(), tbl.get_id())
                    .await?
            } else {
                HashSet::new()
            };
            let candidate = self
                .build_linear_candidate(
                    tbl.as_ref(),
                    push_downs,
                    *limit,
                    linear_final_carry,
                    &claimed_segments,
                )
                .await?;
            let Some((parts, snapshot, segments)) = candidate else {
                return Ok(true);
            };
            let Some(claim_manager) = &claim_manager else {
                break (parts, snapshot, None);
            };

            let claim = claim_manager
                .try_segment_claim(self.ctx.clone(), tbl.get_id(), segments)
                .await?;
            if claim.is_some() {
                break (parts, snapshot, claim);
            }

            metrics_inc_segment_claim_conflicts();
            if claim_retries >= MAX_SEGMENT_CLAIM_RETRIES {
                warn!(
                    "recluster: stop after {} segment claim retries",
                    MAX_SEGMENT_CLAIM_RETRIES
                );
                return Ok(true);
            }
            claim_retries += 1;

            // Let concurrent losers spread out before selecting another candidate set.
            *linear_final_carry = ReclusterFinalCarry::default();
            let delay_ms = rand::thread_rng().gen_range(5..=20);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;

            self.ctx.evict_table_from_cache(catalog, database, table)?;
            tbl = self.ctx.get_table(catalog, database, table).await?;
        };
        let mut physical_plan =
            self.build_linear_plan(tbl.as_ref(), parts, snapshot, use_segment_claims)?;
        physical_plan.adjust_plan_id(&mut 0);
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        build_res.main_pipeline.add_lock_guard(claim_guard);
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
        let execution_result = complete_executor.execute().await;

        // Make sure the executor and any pipeline-held claim start releasing before an error is
        // returned to the FINAL loop for another round. Claim release remains asynchronous, so a
        // retry may rarely observe this round's claim and skip those segments. This best-effort
        // window is acceptable: refresh and task planning normally outlast claim cleanup, and any
        // skipped work remains eligible for a later recluster.
        drop(complete_executor);
        drop(outer_lock_guard);

        execution_result?;
        Ok(false)
    }

    async fn build_linear_candidate(
        &self,
        tbl: &dyn Table,
        push_downs: &mut Option<PushDownInfo>,
        limit: Option<usize>,
        linear_final_carry: &mut ReclusterFinalCarry,
        claimed_segments: &HashSet<String>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>, Vec<String>)>> {
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
                claimed_segments,
            )
            .await?
        else {
            return Ok(None);
        };
        if parts.is_empty() {
            return Ok(None);
        }

        let claimed_sources = parts
            .removed_segment_indexes
            .iter()
            .map(|index| {
                snapshot
                    .segments
                    .get(*index)
                    .map(|(location, _)| location.clone())
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "recluster source segment index {index} is outside snapshot"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some((parts, snapshot, claimed_sources)))
    }

    fn build_linear_plan(
        &self,
        tbl: &dyn Table,
        parts: ReclusterParts,
        snapshot: Arc<TableSnapshot>,
        acquire_commit_lock: bool,
    ) -> Result<PhysicalPlan> {
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(tbl, Some(snapshot.clone()))?;
        let table_info = tbl.get_table_info().clone();
        let ReclusterParts {
            tasks,
            remained_blocks,
            removed_segment_indexes,
            removed_segment_summary,
        } = parts;
        let is_distributed = tasks.len() > 1;
        let mut input = PhysicalPlan::new(Recluster {
            tasks,
            table_meta_timestamps,
            table_info: table_info.clone(),
            meta: PhysicalPlanMeta::new("Recluster"),
        });
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

        Ok(PhysicalPlan::new(CommitSink {
            input,
            table_info,
            snapshot: Some(snapshot),
            commit_type: CommitType::Mutation {
                kind: MutationKind::Recluster,
                merge_meta: false,
            },
            update_stream_meta: vec![],
            deduplicated_label: None,
            table_meta_timestamps,
            recluster_info: Some(ReclusterInfoSideCar {
                merged_blocks: remained_blocks,
                removed_segment_indexes,
                removed_statistics: removed_segment_summary,
                acquire_commit_lock,
            }),
            meta: PhysicalPlanMeta::new("CommitSink"),
        }))
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
}
