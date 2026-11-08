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

use std::sync::Arc;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::AnalyzeOptions;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;

/// Run ANALYZE over the table's current snapshot to completion. A table without a
/// snapshot has nothing to analyze.
///
/// Maintenance however it is started: ANALYZE, the after-write hook, or `ALTER TABLE ... SET
/// OPTIONS(enable_auto_analyze = 1)`. Its reads run under `CachePolicy::MAINTENANCE`; the
/// statement's own policy is restored after, for the rest of an ALTER TABLE.
pub(crate) async fn execute_analyze(
    ctx: Arc<QueryContext>,
    table: &FuseTable,
    options: AnalyzeOptions,
) -> Result<()> {
    let policy = ctx.cache_policy();
    ctx.set_cache_policy(policy.at_most(CachePolicy::MAINTENANCE));
    let result = run_analyze(ctx.clone(), table, options).await;
    ctx.set_cache_policy(policy);
    result
}

async fn run_analyze(
    ctx: Arc<QueryContext>,
    table: &FuseTable,
    options: AnalyzeOptions,
) -> Result<()> {
    let Some(snapshot) = table
        .read_table_snapshot_with_policy(ctx.cache_policy())
        .await?
    else {
        return Ok(());
    };
    let mut pipeline = Pipeline::create();
    table.do_analyze(ctx.clone(), snapshot, &mut pipeline, options)?;
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    let executor = PipelineCompleteExecutor::from_pipelines(vec![pipeline], executor_settings)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute().await
}
