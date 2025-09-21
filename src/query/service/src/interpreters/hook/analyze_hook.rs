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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_storages_fuse::FuseTable;
use log::info;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct AnalyzeDesc {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

/// Hook analyze action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_analyze(ctx: Arc<QueryContext>, pipeline: &mut Pipeline, desc: AnalyzeDesc) {
    if pipeline.is_empty() {
        return;
    }

    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            info!("[ANALYZE-HOOK] Pipeline execution completed successfully, starting analyze job");
            if !ctx.get_enable_auto_analyze() {
                return Ok(());
            }

            match GlobalIORuntime::instance().block_on(do_analyze(ctx, desc)) {
                Ok(_) => {
                    info!("[ANALYZE-HOOK] Analyze job completed successfully");
                }
                Err(e) => {
                    info!("[ANALYZE-HOOK] Analyze job failed: {:?}", e);
                }
            }
        }
        Ok(())
    });
}

/// hook the analyze action with a on-finished callback.
async fn do_analyze(ctx: Arc<QueryContext>, desc: AnalyzeDesc) -> Result<()> {
    // evict the table from cache
    ctx.evict_table_from_cache(&desc.catalog, &desc.database, &desc.table)?;
    ctx.clear_table_meta_timestamps_cache();

    let table = ctx
        .get_table(&desc.catalog, &desc.database, &desc.table)
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let mut pipeline = Pipeline::create();
    let Some(table_snapshot) = fuse_table.read_table_snapshot().await? else {
        return Ok(());
    };
    fuse_table.do_analyze(
        ctx.clone(),
        table_snapshot,
        &mut pipeline,
        HashMap::new(),
        true,
    )?;
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    let pipelines = vec![pipeline];
    let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(complete_executor.get_inner())?;
    complete_executor.execute()?;
    Ok(())
}
