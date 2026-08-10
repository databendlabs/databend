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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::CompactionLimits;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_sql::plans::RelOperator;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeCompactBlockInterpreter;
use crate::interpreters::ReclusterTableInterpreter;
use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::interpreters::hook::table_id_matches_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

#[derive(Clone)]
pub struct CompactTargetTableDescription {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: Option<u64>,
}

pub struct CompactHookTraceCtx {
    pub start: Instant,
    pub operation_name: String,
}

/// Hook compact action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_compact(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    compact_target: CompactTargetTableDescription,
    trace_ctx: CompactHookTraceCtx,
    lock_opt: LockTableOption,
) {
    let op_name = trace_ctx.operation_name.clone();
    if let Err(e) = do_hook_compact(ctx, pipeline, compact_target, trace_ctx, lock_opt).await {
        info!("Operation {} failed with error (ignored): {}", op_name, e);
    }
}

/// hook the compact action with a on-finished callback.
async fn do_hook_compact(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    compact_target: CompactTargetTableDescription,
    trace_ctx: CompactHookTraceCtx,
    lock_opt: LockTableOption,
) -> Result<()> {
    if pipeline.is_empty() {
        return Ok(());
    }

    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            let _ = GlobalIORuntime::instance().block_on(execute_compact_hook(
                ctx,
                compact_target,
                trace_ctx,
                lock_opt,
            ));
        }

        Ok(())
    });

    Ok(())
}

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

pub(crate) async fn execute_compact_hook(
    ctx: Arc<QueryContext>,
    compact_target: CompactTargetTableDescription,
    trace_ctx: CompactHookTraceCtx,
    lock_opt: LockTableOption,
) -> Result<()> {
    let op_name = &trace_ctx.operation_name;
    let Some(compact_target) = resolve_compact_target(&ctx, compact_target).await? else {
        return Ok(());
    };

    metrics_inc_compact_hook_main_operation_time_ms(
        op_name,
        trace_ctx.start.elapsed().as_millis() as u64,
    );
    info!("Operation {op_name} completed successfully, starting table optimization job.");

    let compact_start_at = Instant::now();
    let compaction_num_block_hint = ctx.get_compaction_num_block_hint(&compact_target.table);
    info!(
        "Table {} requires compaction of {} blocks",
        compact_target.table, compaction_num_block_hint
    );
    if compaction_num_block_hint == 0 {
        return Ok(());
    }
    let compaction_limits = CompactionLimits {
        segment_limit: None,
        block_limit: Some(compaction_num_block_hint as usize),
    };

    // keep the original progress value
    let write_progress = ctx.get_write_progress();
    let write_progress_value = write_progress.as_ref().get_values();
    let scan_progress = ctx.get_scan_progress();
    let scan_progress_value = scan_progress.as_ref().get_values();

    match compact_table(ctx, compact_target, compaction_limits, lock_opt).await {
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

    // reset the progress value
    write_progress.set(&write_progress_value);
    scan_progress.set(&scan_progress_value);
    metrics_inc_compact_hook_compact_time_ms(
        &trace_ctx.operation_name,
        compact_start_at.elapsed().as_millis() as u64,
    );

    Ok(())
}

/// compact the target table, will do optimize table actions, including:
///  - compact blocks
///  - re-cluster if the cluster keys are defined
pub(crate) async fn compact_table(
    ctx: Arc<QueryContext>,
    compact_target: CompactTargetTableDescription,
    compaction_limits: CompactionLimits,
    lock_opt: LockTableOption,
) -> Result<()> {
    let settings = ctx.get_settings();
    let Some(compact_target) = resolve_compact_target(&ctx, compact_target).await? else {
        return Ok(());
    };

    // evict the table from cache
    ctx.evict_table_from_cache(
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
    )?;

    ctx.clear_table_meta_timestamps_cache();

    {
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
            false,
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

            // Clears previously generated segment locations to avoid duplicate data in the refresh phase
            ctx.written_segment_locations().clear();
            ctx.set_executor(complete_executor.get_inner())?;
            complete_executor.execute().await?;
            drop(complete_executor);
        }
    }

    {
        let table = ctx
            .get_table(
                &compact_target.catalog,
                &compact_target.database,
                &compact_target.table,
            )
            .await?;
        if !resolved_table_id_matches(&compact_target, table.get_id()) {
            return Ok(());
        }
        // do recluster.
        if table.cluster_key_meta().is_some() {
            // evict the table from cache
            ctx.evict_table_from_cache(
                &compact_target.catalog,
                &compact_target.database,
                &compact_target.table,
            )?;
            ctx.set_enable_sort_spill(false);
            let recluster = ReclusterPlan {
                catalog: compact_target.catalog,
                database: compact_target.database,
                table: compact_target.table,
                limit: Some(settings.get_auto_compaction_segments_limit()? as usize),
                selection: None,
                is_final: false,
            };
            let recluster_interpreter =
                ReclusterTableInterpreter::try_create(ctx.clone(), recluster, lock_opt)?;
            // Recluster will be done in `ReclusterTableInterpreter::execute2` directly,
            // we do not need to use `PipelineCompleteExecutor` to execute it.
            let build_res = recluster_interpreter.execute2().await?;
            debug_assert!(build_res.main_pipeline.is_empty());
        }
    }

    Ok(())
}

async fn resolve_compact_target(
    ctx: &Arc<QueryContext>,
    mut compact_target: CompactTargetTableDescription,
) -> Result<Option<CompactTargetTableDescription>> {
    let Some((database, table)) = resolve_current_table_name_by_id(
        ctx,
        "compact",
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
        compact_target.table_id,
    )
    .await?
    else {
        return Ok(None);
    };

    move_compaction_hint_to_resolved_table(ctx, &compact_target.table, &table);
    compact_target.database = database;
    compact_target.table = table;
    Ok(Some(compact_target))
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

fn resolved_table_id_matches(
    compact_target: &CompactTargetTableDescription,
    actual_table_id: u64,
) -> bool {
    table_id_matches_target(
        "compact",
        compact_target.table_id,
        actual_table_id,
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
    )
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
