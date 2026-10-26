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

databend_common_tracing::register_module_tag!("[FUSE-RECLUSTER]");

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

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
use log::error;
use log::info;
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

#[derive(Debug, PartialEq, Eq)]
enum ReclusterRoundOutcome {
    Committed,
    NoParts,
    ClaimRetriesExhausted,
}

impl ReclusterRoundOutcome {
    fn stop_reason(&self) -> Option<&'static str> {
        match self {
            Self::Committed => None,
            Self::NoParts => Some("no_recluster_parts"),
            Self::ClaimRetriesExhausted => Some("claim_retries_exhausted"),
        }
    }
}

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ReclusterPlan,
    allow_segment_claims: bool,
}

impl ReclusterTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: ReclusterPlan,
        allow_segment_claims: bool,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            plan,
            allow_segment_claims,
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
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let ctx = self.ctx.clone();
            let recluster_timeout_secs = ctx.get_settings().get_recluster_timeout_secs()?;

            let mut rounds = 0;
            let mut push_downs = None;
            // FINAL carry is scoped to this fixed-scan statement loop.
            // A new FINAL statement starts from the table head again.
            let mut linear_final_carry = ReclusterFinalCarry::default();
            let start = SystemTime::now();
            let timeout = Duration::from_secs(recluster_timeout_secs);
            let is_final = self.plan.is_final;
            let mut committed_rounds = 0;
            let (result, stop_reason) = loop {
                if let Err(err) = ctx.check_aborting() {
                    error!(
                        event = "recluster.aborted",
                        rounds;
                        "Recluster aborted before next round"
                    );
                    break (Err(err.with_context("failed to execute")), "aborted");
                }

                rounds += 1;
                let res = self
                    .execute_recluster(&mut push_downs, &mut linear_final_carry)
                    .await;

                match res {
                    Ok(outcome) => {
                        if let Some(reason) = outcome.stop_reason() {
                            break (Ok(()), reason);
                        }
                        committed_rounds += 1;
                    }
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
                                event = "recluster.retry",
                                reason = "retryable_conflict",
                                round = rounds,
                                code = e.code(),
                                error :? = e;
                                "Recluster round failed with retryable conflict"
                            );
                        } else {
                            error!(
                                event = "recluster.failed",
                                round = rounds,
                                code = e.code(),
                                error :? = e;
                                "Recluster round failed"
                            );
                            break (Err(e), "error");
                        }
                    }
                }

                let elapsed_time = SystemTime::now().duration_since(start).unwrap_or_default();
                ctx.set_status_info(&format!(
                    "[FUSE-RECLUSTER] Executed rounds={} committed_rounds={} elapsed={:?}",
                    rounds, committed_rounds, elapsed_time,
                ));

                if !is_final {
                    break (Ok(()), "single_round_completed");
                }

                if elapsed_time >= timeout {
                    warn!(
                        event = "recluster.timeout",
                        rounds,
                        timeout_secs = recluster_timeout_secs;
                        "Recluster stopped at time limit"
                    );
                    break (Ok(()), "timeout");
                }
            };

            info!(
                event = "recluster.finished",
                catalog = self.plan.catalog.as_str(),
                database = self.plan.database.as_str(),
                table = self.plan.table.as_str(),
                is_final,
                rounds,
                committed_rounds,
                stop_reason,
                success = result.is_ok(),
                elapsed :? = SystemTime::now().duration_since(start).unwrap_or_default();
                "Recluster finished"
            );

            if committed_rounds > 0 {
                self.vacuum_table_history().await;
            }

            result?;
            Ok(PipelineBuildResult::create())
        })
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
    ) -> Result<ReclusterRoundOutcome> {
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
        // Callers that already own the lifecycle lock (for example, MV refresh) disable segment
        // claims to avoid reacquiring the shared table lock at the commit gate.
        let mut use_segment_claims =
            self.allow_segment_claims && settings.get_enable_table_lock()?;

        // Concurrent planning must evict this cache so the claim set is matched against the
        // latest snapshot.
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
                return Ok(ReclusterRoundOutcome::NoParts);
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
                    event = "recluster.claim_retries_exhausted",
                    table_id = tbl.get_id(),
                    claim_retries;
                    "Recluster stopped at segment claim retry limit"
                );
                return Ok(ReclusterRoundOutcome::ClaimRetriesExhausted);
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
        let io_before = self.ctx.get_io_stats();
        let blocked_before = self.ctx.get_recluster_blocked_nanos();
        let execute_started = std::time::Instant::now();
        let execution_result = complete_executor.execute().await;
        log_recluster_processor_profile(
            &complete_executor.get_inner().get_proc_profiles(),
            execute_started.elapsed(),
            self.ctx.get_io_stats().delta_since(&io_before),
            self.ctx
                .get_recluster_blocked_nanos()
                .saturating_sub(blocked_before),
        );

        // Make sure the executor and any pipeline-held claim start releasing before an error is
        // returned to the FINAL loop for another round. Claim release remains asynchronous, so a
        // retry may rarely observe this round's claim and skip those segments. This best-effort
        // window is acceptable: refresh and task planning normally outlast claim cleanup, and any
        // skipped work remains eligible for a later recluster.
        drop(complete_executor);

        execution_result?;
        Ok(ReclusterRoundOutcome::Committed)
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
            expected_ttl: None,
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

/// Emit one `recluster.profile.processor` line per processor kind with CPU / wait time
/// summed over all instances of that kind, so the stage distribution of a round (group
/// sources, final merge, compact, serialize, commit) is visible even though the whole
/// pipeline shares a single `Recluster` plan node in `system.query_profile`.
///
/// For `SyncSource`-based group sources, `cpu_ms` includes time blocked on object storage
/// inside `generate()`; the `recluster.profile.group_source` lines break that down further.
fn log_recluster_processor_profile(
    profiles: &[Arc<databend_common_base::runtime::profile::Profile>],
    execute_elapsed: Duration,
    io: databend_common_base::runtime::IoStatsSnapshot,
    blocked_nanos: u64,
) {
    use std::sync::atomic::Ordering;

    use databend_common_base::runtime::profile::ProfileStatisticsName;

    #[derive(Default)]
    struct Agg {
        instances: usize,
        cpu_nanos: u64,
        wait_nanos: u64,
        output_rows: u64,
        max_cpu_nanos: u64,
    }

    let mut by_name: BTreeMap<String, Agg> = BTreeMap::new();
    let mut total_cpu_nanos = 0u64;
    for p in profiles {
        let stat = |name: ProfileStatisticsName| {
            p.statistics[name as usize].load(Ordering::Relaxed) as u64
        };
        let cpu = stat(ProfileStatisticsName::CpuTime);
        let agg = by_name.entry(p.p_name.clone()).or_default();
        agg.instances += 1;
        agg.cpu_nanos += cpu;
        agg.wait_nanos += stat(ProfileStatisticsName::WaitTime);
        agg.output_rows += stat(ProfileStatisticsName::OutputRows);
        agg.max_cpu_nanos = agg.max_cpu_nanos.max(cpu);
        total_cpu_nanos += cpu;
    }

    let wall_ms = execute_elapsed.as_secs_f64() * 1e3;
    let total_cpu_ms = total_cpu_nanos as f64 / 1e6;
    let blocked_ms = blocked_nanos as f64 / 1e6;
    // Executor CpuTime for sync sources includes time blocked on S3; take it back out.
    let true_cpu_ms = (total_cpu_ms - blocked_ms).max(0.0);
    // Average executor threads occupied (busy or blocked) over the round.
    let thread_occupancy = if wall_ms > 0.0 {
        total_cpu_ms / wall_ms
    } else {
        0.0
    };
    // Average threads actually computing.
    let cpu_parallelism = if wall_ms > 0.0 {
        true_cpu_ms / wall_ms
    } else {
        0.0
    };
    // Object-storage read time summed over requests vs. round wall clock: how many
    // reads were in flight on average. Compare with the number of group sources.
    let io_read_concurrency = if wall_ms > 0.0 {
        io.read_duration_ms as f64 / wall_ms
    } else {
        0.0
    };
    info!(
        event = "recluster.profile.round",
        wall_ms = format!("{wall_ms:.1}"),
        processors = profiles.len(),
        // sum of executor CpuTime over all processors
        total_cpu_ms = format!("{total_cpu_ms:.1}"),
        // sum over group sources of time blocked in recv_blocking
        blocked_ms = format!("{blocked_ms:.1}"),
        true_cpu_ms = format!("{true_cpu_ms:.1}"),
        thread_occupancy = format!("{thread_occupancy:.2}"),
        cpu_parallelism = format!("{cpu_parallelism:.2}"),
        // per-query opendal stats for this round
        io_read_count = io.read_count,
        io_read_bytes = io.read_bytes,
        io_read_ms = io.read_duration_ms,
        io_read_concurrency = format!("{io_read_concurrency:.2}"),
        io_write_count = io.write_count,
        io_write_bytes = io.write_bytes,
        io_write_ms = io.write_duration_ms;
        "Recluster round processor profile"
    );
    for (name, agg) in by_name {
        let cpu_ms = agg.cpu_nanos as f64 / 1e6;
        let max_cpu_ms = agg.max_cpu_nanos as f64 / 1e6;
        // Only the recluster group source blocks on S3 inside process(); for it, show
        // how much of cpu_ms was really I/O wait. Zero for every other kind.
        let kind_blocked_ms = if name == "HorizontalMultiwayReclusterSource" {
            blocked_ms
        } else {
            0.0
        };
        let kind_true_cpu_ms = (cpu_ms - kind_blocked_ms).max(0.0);
        // 1.0 = CPU spread evenly across instances; N = one instance did all the work.
        let cpu_skew = if agg.cpu_nanos > 0 {
            agg.max_cpu_nanos as f64 * agg.instances as f64 / agg.cpu_nanos as f64
        } else {
            0.0
        };
        // Slowest instance's CPU as a share of the round: the critical-path candidate.
        let max_instance_cpu_pct_of_wall = if wall_ms > 0.0 {
            max_cpu_ms / wall_ms * 100.0
        } else {
            0.0
        };
        info!(
            event = "recluster.profile.processor",
            name = name.as_str(),
            instances = agg.instances,
            cpu_ms = format!("{cpu_ms:.1}"),
            blocked_ms = format!("{kind_blocked_ms:.1}"),
            true_cpu_ms = format!("{kind_true_cpu_ms:.1}"),
            wait_ms = format!("{:.1}", agg.wait_nanos as f64 / 1e6),
            max_instance_cpu_ms = format!("{max_cpu_ms:.1}"),
            cpu_skew = format!("{cpu_skew:.2}"),
            max_instance_cpu_pct_of_wall = format!("{max_instance_cpu_pct_of_wall:.1}"),
            output_rows = agg.output_rows;
            "Recluster processor stage profile"
        );
    }
}
