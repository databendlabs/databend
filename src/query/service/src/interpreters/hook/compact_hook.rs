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
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::OptimizeTableAction;
use databend_common_sql::plans::OptimizeTablePlan;
use log::info;

use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeTableInterpreter;
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
        let compaction_limits = match compact_target.mutation_kind {
            MutationKind::Insert => {
                let compaction_num_block_hint = ctx.get_compaction_num_block_hint();
                info!("hint number of blocks need to be compacted {}", compaction_num_block_hint);
                if compaction_num_block_hint == 0 {
                    return Ok(());
                }
                CompactionLimits {
                    segment_limit: None,
                    block_limit: Some(compaction_num_block_hint as usize),
                }
            }
            _ =>
            // for mutations other than Insertions, we use an empirical value of 3 segments as the
            // limit for compaction. to be refined later.
                {
                    CompactionLimits {
                        segment_limit: Some(3),
                        block_limit: None,
                    }
                }
        };

        let op_name = &trace_ctx.operation_name;
        metrics_inc_compact_hook_main_operation_time_ms(op_name, trace_ctx.start.elapsed().as_millis() as u64);

        let compact_start_at = Instant::now();
        if info.res.is_ok() {
            info!("execute {op_name} finished successfully. running table optimization job.");
            match GlobalIORuntime::instance().block_on({
                compact_table(ctx, compact_target, compaction_limits, lock_opt)
            }) {
                Ok(_) => {
                    info!("execute {op_name} finished successfully. table optimization job finished.");
                }
                Err(e) => { info!("execute {op_name} finished successfully. table optimization job failed. {:?}", e) }
            }
        }
        metrics_inc_compact_hook_compact_time_ms(&trace_ctx.operation_name, compact_start_at.elapsed().as_millis() as u64);

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
    // evict the table from cache
    ctx.evict_table_from_cache(
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
    )?;

    // build the optimize table pipeline with compact action.
    let optimize_interpreter =
        OptimizeTableInterpreter::try_create(ctx.clone(), OptimizeTablePlan {
            catalog: compact_target.catalog,
            database: compact_target.database,
            table: compact_target.table,
            action: OptimizeTableAction::CompactBlocks(compaction_limits.block_limit),
            limit: compaction_limits.segment_limit,
            lock_opt,
        })?;

    let mut build_res = optimize_interpreter.execute2().await?;

    if build_res.main_pipeline.is_empty() {
        return Ok(());
    }

    // execute the compact pipeline (for table with cluster keys, re-cluster will also be executed)
    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

        // keep the original progress value
        let progress_value = ctx.get_write_progress_value();
        // Clears previously generated segment locations to avoid duplicate data in the refresh phase
        ctx.clear_segment_locations()?;
        ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
        drop(complete_executor);

        // reset the progress value
        ctx.get_write_progress().set(&progress_value);
    }
    Ok(())
}
