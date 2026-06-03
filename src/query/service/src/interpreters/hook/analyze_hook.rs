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

// Logs from this module will show up as "[ANALYZE-HOOK] ...".
databend_common_tracing::register_module_tag!("[ANALYZE-HOOK]");

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_storages_fuse::FuseTable;
use log::info;

use crate::interpreters::hook::table_id_matches_target;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct AnalyzeDesc {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: Option<u64>,
}

/// Hook analyze action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_analyze(ctx: Arc<QueryContext>, pipeline: &mut Pipeline, desc: AnalyzeDesc) {
    if pipeline.is_empty() {
        return;
    }

    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            let _ = GlobalIORuntime::instance().block_on(execute_analyze_hook(ctx, desc));
        }
        Ok(())
    });
}

pub(crate) fn analyze_after_write_enabled(ctx: &Arc<QueryContext>) -> bool {
    ctx.get_enable_auto_analyze()
}

pub(crate) async fn execute_analyze_hook(ctx: Arc<QueryContext>, desc: AnalyzeDesc) -> Result<()> {
    info!("Table hook starting analyze job");
    if !analyze_after_write_enabled(&ctx) {
        return Ok(());
    }

    match do_analyze(ctx, desc).await {
        Ok(_) => {
            info!("Analyze job completed successfully");
        }
        Err(e) => {
            info!("Analyze job failed: {:?}", e);
        }
    }

    Ok(())
}

/// hook the analyze action with a on-finished callback.
pub(crate) async fn do_analyze(ctx: Arc<QueryContext>, desc: AnalyzeDesc) -> Result<()> {
    // evict the table from cache
    ctx.evict_table_from_cache(&desc.catalog, &desc.database, &desc.table)?;
    ctx.clear_table_meta_timestamps_cache();

    let table = ctx
        .get_table(&desc.catalog, &desc.database, &desc.table)
        .await?;
    if !table_id_matches_target(
        "analyze",
        desc.table_id,
        table.get_id(),
        &desc.catalog,
        &desc.database,
        &desc.table,
    ) {
        return Ok(());
    }
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
        false,
    )?;
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    let pipelines = vec![pipeline];
    let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(complete_executor.get_inner())?;
    complete_executor.execute().await?;
    Ok(())
}
