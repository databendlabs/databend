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

// Logs from this module will show up as "[TABLE-HOOK] ...".
databend_common_tracing::register_module_tag!("[TABLE-HOOK]");

use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SharedLockGuard;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_sql::plans::RelOperator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::AnalyzeOptions;
use log::info;
use log::warn;

use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeCompactBlockInterpreter;
use crate::interpreters::ReclusterTableInterpreter;
use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::hook::analyze_hook::execute_analyze;
use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::interpreters::hook::table_id_matches_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TableHookTaskKey {
    pub(super) catalog: String,
    pub(super) table_id: u64,
}

pub(super) trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;

    fn merge(&mut self, other: Self);
}

pub struct TableHookTarget {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: u64,
}

impl TableHookTarget {
    /// Resolve separately for each stage: maintenance can outlive a rename or drop.
    /// Resolution refreshes the table cache and clears cached table metadata timestamps.
    async fn resolve(&self, ctx: &Arc<QueryContext>, hook_name: &str) -> Result<Option<Self>> {
        let Some((database, table)) = resolve_current_table_name_by_id(
            ctx,
            hook_name,
            &self.catalog,
            &self.database,
            &self.table,
            Some(self.table_id),
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            catalog: self.catalog.clone(),
            database,
            table,
            table_id: self.table_id,
        }))
    }

    async fn resolve_compact(&self, ctx: &Arc<QueryContext>) -> Result<Option<Self>> {
        let Some(resolved) = self.resolve(ctx, "compact").await? else {
            return Ok(None);
        };

        move_compaction_hint_to_resolved_table(ctx, &self.table, &resolved.table);
        Ok(Some(resolved))
    }
}

pub struct TableHookTask {
    pub ctx: Arc<QueryContext>,
    pub target: TableHookTarget,
    pub hook_settings: TableHookTaskSettings,
    pub lock_opt: LockTableOption,
    /// Only synchronous autocommit tasks carry a statement lock. Queued and post-commit
    /// tasks carry none and acquire maintenance locks where required.
    pub lock_guard: Option<SharedLockGuard>,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

impl TableHookTask {
    /// Run the same maintenance chain for autocommit and post-commit hooks, in both modes.
    /// Release any handed-over statement lock after refresh and before analyze.
    pub(crate) async fn execute(mut self, is_async: bool) {
        if is_async {
            info!(
                "Start async table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Start sync table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
        if self.hook_settings.compact_after_write {
            self.execute_compact().await.unwrap_or_else(|e| {
                warn!(
                    "Table hook compact failed, table_id={}: {e}",
                    self.target.table_id
                )
            });
        }
        self.execute_refresh().await;
        if let Some(guard) = self.lock_guard.take() {
            drop(guard.try_take());
        }
        self.execute_analyze().await;
        if is_async {
            info!(
                "Async table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Sync table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
    }

    async fn execute_compact(&self) -> Result<()> {
        let ctx = self.ctx.clone();
        // Maintenance follows the write, whose own reads are done by now.
        ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let op_name = &self.operation_name;
        // Hints are attached to the task's original table name. With no work, keep the
        // usual logging and metrics without resolving a maintenance target.
        let resolved_target = if ctx.get_compaction_num_block_hint(&self.target.table) == 0 {
            None
        } else {
            let Some(target) = self.target.resolve_compact(&ctx).await? else {
                return Ok(());
            };
            Some(target)
        };
        let target = resolved_target.as_ref().unwrap_or(&self.target);

        metrics_inc_compact_hook_main_operation_time_ms(
            op_name,
            self.main_operation_start.elapsed().as_millis() as u64,
        );
        info!("Operation {op_name} completed successfully, starting table optimization job.");

        let compact_start_at = Instant::now();
        let compaction_num_block_hint = ctx.get_compaction_num_block_hint(&target.table);
        info!(
            "Table {} requires compaction of {} blocks",
            target.table, compaction_num_block_hint
        );
        if compaction_num_block_hint == 0 {
            return Ok(());
        }
        let compaction_limits = CompactionLimits {
            segment_limit: None,
            block_limit: Some(compaction_num_block_hint as usize),
        };

        let write_progress = ctx.get_write_progress();
        let write_progress_value = write_progress.as_ref().get_values();
        let scan_progress = ctx.get_scan_progress();
        let scan_progress_value = scan_progress.as_ref().get_values();

        let lock_opt = self.lock_opt.clone();
        // Keep failures inside this scope so progress is restored before leaving the stage.
        let result: Result<()> = async {
            let settings = ctx.get_settings();
            let Some(compact_target) = target.resolve_compact(&ctx).await? else {
                return Ok(());
            };

            // evict the table from cache
            ctx.evict_table_from_cache(
                &compact_target.catalog,
                &compact_target.database,
                &compact_target.table,
            )?;
            ctx.clear_table_meta_timestamps_cache();

            let compact_start = Instant::now();
            let compact_result: Result<()> = async {
                // do compact.
                let compact_block = RelOperator::CompactBlock(OptimizeCompactBlock {
                    catalog: compact_target.catalog.clone(),
                    database: compact_target.database.clone(),
                    table: compact_target.table.clone(),
                    limit: compaction_limits.clone(),
                });
                let s_expr = SExpr::create_leaf(Arc::new(compact_block));
                let compact_interpreter = OptimizeCompactBlockInterpreter::try_create(
                    ctx.clone(),
                    s_expr,
                    lock_opt.clone(),
                )?;
                let mut build_res = compact_interpreter.execute2().await?;
                // execute the compact pipeline
                if build_res.main_pipeline.is_complete_pipeline()? {
                    let query_ctx = ctx.clone();
                    build_res.main_pipeline.set_on_finished(always_callback(
                        move |_info: &ExecutionInfo| {
                            hook_clear_m_cte_temp_table(&query_ctx)?;
                            hook_vacuum_temp_files(&query_ctx)?;
                            hook_disk_temp_dir(&query_ctx)?;
                            Ok(())
                        },
                    ));

                    build_res.set_max_threads(settings.get_max_threads()? as usize);
                    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;

                    let mut pipelines = build_res.sources_pipelines;
                    pipelines.push(build_res.main_pipeline);

                    let complete_executor =
                        PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;

                    // Preserve parent-write hints for untouched segments and failed compaction.
                    // CommitSink adds published outputs; refresh filters out no-longer-live sources.
                    ctx.set_executor(complete_executor.get_inner())?;
                    complete_executor.execute().await?;
                    drop(complete_executor);
                }
                Ok(())
            }
            .await;
            info!(
                event = "compact_hook.stage_finished",
                stage = "compact",
                catalog = compact_target.catalog.as_str(),
                database = compact_target.database.as_str(),
                table = compact_target.table.as_str(),
                elapsed_ms = compact_start.elapsed().as_millis() as u64,
                success = compact_result.is_ok(),
                error :? = compact_result.as_ref().err();
                "Compact hook stage finished"
            );
            compact_result?;

            let table = ctx
                .get_table(
                    &compact_target.catalog,
                    &compact_target.database,
                    &compact_target.table,
                )
                .await?;
            if !table_id_matches_target(
                "compact",
                Some(compact_target.table_id),
                table.get_id(),
                &compact_target.catalog,
                &compact_target.database,
                &compact_target.table,
            ) {
                return Ok(());
            }
            if table.cluster_key_meta().is_none() {
                return Ok(());
            }

            let recluster_start = Instant::now();
            let recluster_result: Result<()> = async {
                // evict the table from cache
                ctx.evict_table_from_cache(
                    &compact_target.catalog,
                    &compact_target.database,
                    &compact_target.table,
                )?;
                // Spill is disabled to keep this synchronous hook fast (spilling adds
                // disk IO). Admission must then keep tasks inside the memory budget.
                // TODO: remove once the async hook is enabled.
                ctx.set_enable_sort_spill(false);
                let target = if is_materialized_view_engine(table.engine()) {
                    MaintenanceTarget::MaterializedView {
                        table_id: table.get_id(),
                    }
                } else {
                    MaintenanceTarget::Table
                };
                let recluster = ReclusterPlan {
                    catalog: compact_target.catalog.clone(),
                    database: compact_target.database.clone(),
                    table: compact_target.table.clone(),
                    target,
                    limit: Some(settings.get_auto_compaction_segments_limit()? as usize),
                    selection: None,
                    is_final: false,
                };
                let allow_segment_claims = lock_opt != LockTableOption::NoLock;
                let recluster_interpreter = ReclusterTableInterpreter::try_create(
                    ctx.clone(),
                    recluster,
                    allow_segment_claims,
                )?;
                // Recluster will be done in `ReclusterTableInterpreter::execute2` directly,
                // we do not need to use `PipelineCompleteExecutor` to execute it.
                let build_res = recluster_interpreter.execute2().await?;
                debug_assert!(build_res.main_pipeline.is_empty());
                Ok(())
            }
            .await;
            info!(
                event = "compact_hook.stage_finished",
                stage = "recluster",
                catalog = compact_target.catalog.as_str(),
                database = compact_target.database.as_str(),
                table = compact_target.table.as_str(),
                elapsed_ms = recluster_start.elapsed().as_millis() as u64,
                success = recluster_result.is_ok(),
                error :? = recluster_result.as_ref().err();
                "Compact hook stage finished"
            );
            recluster_result?;

            Ok(())
        }
        .await;
        match result {
            Ok(_) => {
                info!("Operation {op_name} and table optimization job completed successfully.");
            }
            Err(e) => {
                info!(
                    "Operation {op_name} completed but table optimization job failed: {:?}",
                    e
                );
            }
        }

        write_progress.set(&write_progress_value);
        scan_progress.set(&scan_progress_value);
        metrics_inc_compact_hook_compact_time_ms(
            op_name,
            compact_start_at.elapsed().as_millis() as u64,
        );
        Ok(())
    }

    async fn execute_refresh(&self) {
        self.ctx
            .set_cache_policy(self.ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        info!("Table hook starting refresh job");
        let result: Result<()> = async {
            let ctx = self.ctx.clone();
            let Some(target) = self.target.resolve(&ctx, "refresh").await? else {
                return Ok(());
            };

            // Target resolution refreshed the table cache and checked its ID. Reuse that
            // current table to filter hints against the post-maintenance snapshot.
            let table = ctx
                .get_table(&target.catalog, &target.database, &target.table)
                .await?;
            if !table_id_matches_target(
                "refresh",
                Some(target.table_id),
                table.get_id(),
                &target.catalog,
                &target.database,
                &target.table,
            ) {
                return Ok(());
            }
            let names = table
                .get_table_info()
                .meta
                .indexes
                .values()
                .filter(|index| !index.sync_creation)
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            if names.is_empty() {
                return Ok(());
            }
            let mut pipeline = Pipeline::create();
            FuseTable::try_from_table(table.as_ref())?
                .do_refresh_indexes(
                    ctx.clone(),
                    &names,
                    Some(ctx.written_segment_locations().list()),
                    &mut pipeline,
                )
                .await?;
            if pipeline.is_empty() {
                return Ok(());
            }

            let query_ctx = ctx.clone();
            pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
                hook_clear_m_cte_temp_table(&query_ctx)?;
                hook_vacuum_temp_files(&query_ctx)?;
                hook_disk_temp_dir(&query_ctx)?;
                Ok(())
            }));
            let executor = PipelineCompleteExecutor::try_create(
                pipeline,
                ExecutorSettings::try_create(ctx.clone())?,
            )?;
            ctx.set_executor(executor.get_inner())?;
            executor.execute().await
        }
        .await;
        match result {
            Ok(_) => info!("Refresh job completed successfully"),
            Err(e) => info!("Refresh job failed: {:?}", e),
        }
    }

    async fn execute_analyze(&self) {
        self.ctx
            .set_cache_policy(self.ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        info!("Table hook starting analyze job");
        if !self.ctx.get_enable_auto_analyze() {
            return;
        }
        let result: Result<()> = async {
            let ctx = self.ctx.clone();
            let Some(target) = self.target.resolve(&ctx, "analyze").await? else {
                return Ok(());
            };

            // Reuse the table freshly loaded and ID-checked by this stage's resolution.
            let table = ctx
                .get_table(&target.catalog, &target.database, &target.table)
                .await?;
            if !table_id_matches_target(
                "analyze",
                Some(target.table_id),
                table.get_id(),
                &target.catalog,
                &target.database,
                &target.table,
            ) {
                return Ok(());
            }
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let options =
                AnalyzeOptions::from_table_options(fuse_table.get_table_info().options())?
                    .no_scan();
            execute_analyze(ctx, fuse_table, options).await
        }
        .await;
        match result {
            Ok(_) => info!("Analyze job completed successfully"),
            Err(e) => warn!("Analyze job failed (code {}): {}", e.code(), e),
        }
    }
}

impl SchedulerTask for TableHookTask {
    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            catalog: self.target.catalog.clone(),
            table_id: self.target.table_id,
        }
    }

    /// Combine pending inputs while keeping the latest task's execution context.
    fn merge(&mut self, mut other: Self) {
        other.hook_settings.merge(self.hook_settings);

        other
            .ctx
            .written_segment_locations()
            .extend(self.ctx.written_segment_locations().list());

        let compaction_num_block_hint = self
            .ctx
            .get_compaction_num_block_hint(&self.target.table)
            .saturating_add(other.ctx.get_compaction_num_block_hint(&other.target.table));
        if compaction_num_block_hint > 0 {
            other
                .ctx
                .set_compaction_num_block_hint(&other.target.table, compaction_num_block_hint);
        }

        if self.ctx.get_enable_auto_analyze() {
            other.ctx.set_enable_auto_analyze(true);
        }

        other.main_operation_start = self.main_operation_start;
        other.operation_name = self.operation_name.clone();

        *self = other;
    }
}

#[derive(Clone, Copy)]
pub struct TableHookTaskSettings {
    pub compact_after_write: bool,
}

impl TableHookTaskSettings {
    pub fn create(ctx: &Arc<QueryContext>) -> Self {
        Self {
            compact_after_write: Self::compact_after_write_enabled(ctx),
        }
    }

    pub fn compact_after_write_enabled(ctx: &Arc<QueryContext>) -> bool {
        match ctx.get_settings().get_enable_compact_after_write() {
            Ok(false) => {
                info!("Auto compaction is disabled");
                false
            }
            Err(e) => {
                // swallow the exception, compaction hook should not prevent the main operation.
                log::warn!(
                    "Failed to retrieve compaction settings, continuing without compaction: {}",
                    e
                );
                false
            }
            Ok(true) => true,
        }
    }

    fn merge(&mut self, other: Self) {
        self.compact_after_write |= other.compact_after_write;
    }
}

fn move_compaction_hint_to_resolved_table(
    ctx: &Arc<QueryContext>,
    original_table: &str,
    resolved_table: &str,
) {
    if original_table == resolved_table {
        return;
    }

    let original_hint = ctx.get_compaction_num_block_hint(original_table);
    if original_hint == 0 {
        return;
    }

    let resolved_hint = ctx.get_compaction_num_block_hint(resolved_table);
    ctx.set_compaction_num_block_hint(resolved_table, resolved_hint.saturating_add(original_hint));
    ctx.set_compaction_num_block_hint(original_table, 0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_kits::TestFixture;

    fn table_hook_task(ctx: Arc<QueryContext>, table: &str) -> TableHookTask {
        let hook_settings = TableHookTaskSettings::create(&ctx);
        TableHookTask {
            ctx,
            target: TableHookTarget {
                catalog: "default".to_string(),
                database: "default".to_string(),
                table: table.to_string(),
                table_id: 1,
            },
            hook_settings,
            lock_opt: LockTableOption::NoLock,
            lock_guard: None,
            operation_name: "insert".to_string(),
            main_operation_start: Instant::now(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_accumulates_compaction_hint() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx.set_compaction_num_block_hint("t", 7);
        latest_ctx.set_compaction_num_block_hint("t", 5);
        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert_eq!(task.ctx.get_compaction_num_block_hint("t"), 12);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_task_merge_preserves_hook_settings() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "1".to_string())?;
        latest_ctx
            .get_settings()
            .set_setting("enable_compact_after_write".to_string(), "0".to_string())?;

        let mut task = table_hook_task(old_ctx, "t");
        task.merge(table_hook_task(latest_ctx, "t"));

        assert!(task.hook_settings.compact_after_write);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_move_compaction_hint_to_resolved_table() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        ctx.set_compaction_num_block_hint("old_table", 7);
        ctx.set_compaction_num_block_hint("new_table", 5);

        move_compaction_hint_to_resolved_table(&ctx, "old_table", "new_table");

        assert_eq!(ctx.get_compaction_num_block_hint("old_table"), 0);
        assert_eq!(ctx.get_compaction_num_block_hint("new_table"), 12);
        Ok(())
    }
}
