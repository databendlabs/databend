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

// Logs from this module will show up as "[COMPACT-HOOK] ...".
databend_common_tracing::register_module_tag!("[COMPACT-HOOK]");

use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::CompactionLimits;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::RelOperator;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeCompactBlockInterpreter;
use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::hook::table_hook_task::TableHookTarget;
use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::interpreters::hook::table_hook_task::keep_progress;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableManagement;

pub(crate) fn compact_after_write_enabled(ctx: &Arc<QueryContext>) -> bool {
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

impl TableHookTask {
    /// Compact the table the write left fragmented. Returns the resolved target when a
    /// compaction ran and succeeded, so the caller can follow it with a recluster round.
    pub(super) async fn execute_compact(&self) -> Result<Option<TableHookTarget>> {
        let ctx = self.ctx.clone();
        // Maintenance after the write: `CachePolicy::MAINTENANCE` for its reads, on the context
        // of the write, whose own reads are done by now.
        ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let op_name = &self.operation_name;
        // Hints are attached to the task's original table name. With no work, keep the
        // usual logging and metrics without resolving a maintenance target.
        let resolved_target = if ctx.get_compaction_num_block_hint(&self.target.table) == 0 {
            None
        } else {
            let Some(target) = self.target.resolve_compact(&ctx).await? else {
                return Ok(None);
            };
            Some(target)
        };
        let compact_target = resolved_target.as_ref().unwrap_or(&self.target);

        metrics_inc_compact_hook_main_operation_time_ms(
            op_name,
            self.main_operation_start.elapsed().as_millis() as u64,
        );
        info!("Operation {op_name} completed successfully, starting table optimization job.");

        let compact_start_at = Instant::now();
        let compaction_num_block_hint = ctx.get_compaction_num_block_hint(&compact_target.table);
        info!(
            "Table {} requires compaction of {} blocks",
            compact_target.table, compaction_num_block_hint
        );
        if compaction_num_block_hint == 0 {
            return Ok(None);
        }
        let compaction_limits = CompactionLimits {
            segment_limit: None,
            block_limit: Some(compaction_num_block_hint as usize),
        };

        let compacted = match keep_progress(
            &ctx,
            compact_table(
                ctx.clone(),
                compact_target,
                compaction_limits,
                self.lock_opt.clone(),
            ),
        )
        .await
        {
            Ok(_) => {
                info!("Operation {op_name} and table optimization job completed successfully.");
                true
            }
            Err(e) => {
                info!(
                    "Operation {op_name} completed but table optimization job failed: {:?}",
                    e
                );
                false
            }
        };

        metrics_inc_compact_hook_compact_time_ms(
            op_name,
            compact_start_at.elapsed().as_millis() as u64,
        );

        // A non-zero hint means the target was resolved above.
        Ok(resolved_target.filter(|_| compacted))
    }
}

/// Compact the blocks of the target table.
///
/// `compact_target` must already be resolved by `execute_compact`.
async fn compact_table(
    ctx: Arc<QueryContext>,
    compact_target: &TableHookTarget,
    compaction_limits: CompactionLimits,
    lock_opt: LockTableOption,
) -> Result<()> {
    let settings = ctx.get_settings();

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
            expected_table_id: Some(compact_target.table_id),
            limit: compaction_limits.clone(),
        });
        let s_expr = SExpr::create_leaf(Arc::new(compact_block));
        let compact_interpreter =
            OptimizeCompactBlockInterpreter::try_create(ctx.clone(), s_expr, lock_opt.clone())?;
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
    compact_result
}

impl TableHookTarget {
    async fn resolve_compact(&self, ctx: &Arc<QueryContext>) -> Result<Option<Self>> {
        let Some(resolved) = self.resolve(ctx, "compact").await? else {
            return Ok(None);
        };

        move_compaction_hint_to_resolved_table(ctx, &self.table, &resolved.table);
        Ok(Some(resolved))
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
