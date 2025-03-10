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
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::always_callback;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::plans::ReclusterPlan;
use databend_common_sql::plans::RelOperator;
use databend_storages_common_table_meta::table::ClusterType;
use log::info;

use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeCompactBlockInterpreter;
use crate::interpreters::ReclusterTableInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct CompactTargetTableDescription {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub mutation_kind: MutationKind,
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
        info!("compact hook ({}) with error (ignored): {}", op_name, e);
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
            let op_name = &trace_ctx.operation_name;
            metrics_inc_compact_hook_main_operation_time_ms(op_name, trace_ctx.start.elapsed().as_millis() as u64);
            info!("execute {op_name} finished successfully. running table optimization job.");

            let compact_start_at = Instant::now();
            let compaction_limits = match compact_target.mutation_kind {
                MutationKind::Insert => {
                    let compaction_num_block_hint = ctx.get_compaction_num_block_hint(&compact_target.table);
                    info!("table {} hint number of blocks need to be compacted {}", compact_target.table, compaction_num_block_hint);
                    if compaction_num_block_hint == 0 {
                        return Ok(());
                    }
                    CompactionLimits {
                        segment_limit: None,
                        block_limit: Some(compaction_num_block_hint as usize),
                    }
                }
                _ => {
                    let auto_compaction_segments_limit = ctx.get_settings().get_auto_compaction_segments_limit()?;
                    CompactionLimits {
                        segment_limit: Some(auto_compaction_segments_limit as usize),
                        block_limit: None,
                    }
                }
            };

            // keep the original progress value
            let write_progress = ctx.get_write_progress();
            let write_progress_value = write_progress.as_ref().get_values();
            let scan_progress = ctx.get_scan_progress();
            let scan_progress_value = scan_progress.as_ref().get_values();

            match GlobalIORuntime::instance().block_on({
                compact_table(ctx, compact_target, compaction_limits, lock_opt)
            }) {
                Ok(_) => {
                    info!("execute {op_name} finished successfully. table optimization job finished.");
                }
                Err(e) => { info!("execute {op_name} finished successfully. table optimization job failed. {:?}", e); }
            }

            // reset the progress value
            write_progress.set(&write_progress_value);
            scan_progress.set(&scan_progress_value);
            metrics_inc_compact_hook_compact_time_ms(&trace_ctx.operation_name, compact_start_at.elapsed().as_millis() as u64);
        }

        Ok(())
    });

    Ok(())
}

/// compact the target table, will do optimize table actions, including:
///  - compact blocks
///  - re-cluster if the cluster keys are defined
async fn compact_table(
    ctx: Arc<QueryContext>,
    compact_target: CompactTargetTableDescription,
    compaction_limits: CompactionLimits,
    lock_opt: LockTableOption,
) -> Result<()> {
    let table = ctx
        .get_table(
            &compact_target.catalog,
            &compact_target.database,
            &compact_target.table,
        )
        .await?;
    let settings = ctx.get_settings();

    // evict the table from cache
    ctx.evict_table_from_cache(
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
    )?;

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
            ctx.clear_written_segment_locations()?;
            ctx.set_executor(complete_executor.get_inner())?;
            complete_executor.execute()?;
            drop(complete_executor);
        }
    }

    {
        // do recluster.
        if let Some(cluster_type) = table.cluster_type() {
            if cluster_type == ClusterType::Linear {
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
    }

    Ok(())
}
