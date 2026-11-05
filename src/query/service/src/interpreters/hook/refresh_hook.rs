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

// Logs from this module will show up as "[REFRESH-HOOK] ...".
databend_common_tracing::register_module_tag!("[REFRESH-HOOK]");

use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_storages_fuse::FuseTable;
use log::info;

use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::interpreters::hook::table_id_matches_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct RefreshDesc {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: Option<u64>,
}

/// Hook refresh action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_refresh(ctx: Arc<QueryContext>, pipeline: &mut Pipeline, desc: RefreshDesc) {
    if pipeline.is_empty() {
        return;
    }

    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_ok() {
            let _ = GlobalIORuntime::instance().block_on(execute_refresh_hook(ctx, desc));
        }
        Ok(())
    });
}

pub(crate) async fn execute_refresh_hook(ctx: Arc<QueryContext>, desc: RefreshDesc) -> Result<()> {
    info!("Table hook starting refresh job");
    match do_refresh(ctx, desc).await {
        Ok(_) => info!("Refresh job completed successfully"),
        Err(e) => info!("Refresh job failed: {:?}", e),
    }
    Ok(())
}

pub(crate) async fn do_refresh(ctx: Arc<QueryContext>, desc: RefreshDesc) -> Result<()> {
    let Some(desc) = resolve_refresh_desc(&ctx, desc).await? else {
        return Ok(());
    };

    // Sync hooks reuse the write context too. Filter accumulated write and
    // maintenance hints against the post-maintenance snapshot, not a cached one.
    ctx.evict_table_from_cache(&desc.catalog, &desc.database, &desc.table)?;
    let table = ctx
        .get_table(&desc.catalog, &desc.database, &desc.table)
        .await?;
    if !table_id_matches_target(
        "refresh",
        desc.table_id,
        table.get_id(),
        &desc.catalog,
        &desc.database,
        &desc.table,
    ) {
        return Ok(());
    }
    ctx.clear_table_meta_timestamps_cache();
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
    let executor =
        PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx.clone())?)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute().await
}

async fn resolve_refresh_desc(
    ctx: &Arc<QueryContext>,
    mut desc: RefreshDesc,
) -> Result<Option<RefreshDesc>> {
    let Some((database, table)) = resolve_current_table_name_by_id(
        ctx,
        "refresh",
        &desc.catalog,
        &desc.database,
        &desc.table,
        desc.table_id,
    )
    .await?
    else {
        return Ok(None);
    };
    desc.database = database;
    desc.table = table;
    Ok(Some(desc))
}
