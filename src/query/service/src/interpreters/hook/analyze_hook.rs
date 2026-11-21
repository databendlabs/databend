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
use std::time::Duration;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_storages_fuse::FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::operations::AnalyzeOptions;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::table_maintenance::scheduler::TaskKind;
use crate::table_maintenance::task::MaintenanceOutcome;

impl TableHookTask {
    pub(super) async fn execute_analyze(&self) {
        if !self.ctx.get_enable_auto_analyze() {
            return;
        }
        let table_id = self.target.table_id;
        match CatchUnwindFuture::create(self.analyze_inline()).await {
            Ok(Ok(())) => (),
            Ok(Err(error)) => {
                let _guard = LimitMemGuard::enter_unlimited();
                log::warn!(table_id, error :? = error; "Synchronous Analyze hook failed");
            }
            Err(error) => {
                let _guard = LimitMemGuard::enter_unlimited();
                log::warn!(table_id, error :? = error; "Synchronous Analyze hook panicked");
            }
        }
    }

    async fn analyze_inline(&self) -> Result<()> {
        self.ctx
            .set_cache_policy(self.ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let Some(desc) = self.target.resolve(&self.ctx, "analyze").await? else {
            return Ok(());
        };
        let table = self
            .ctx
            .get_table(&desc.catalog, &desc.database, &desc.table)
            .await?;
        if desc.matches_id("analyze", table.get_id()) {
            let fuse = FuseTable::try_from_table(table.as_ref())?;
            if fuse.get_option(FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE, 0u32) != 0 {
                analyze_no_scan(self.ctx.clone(), fuse).await?;
            }
        }
        Ok(())
    }
}

/// An independent notification to check current statistics, not an Analyze algorithm.
pub(crate) async fn run(
    ctx: Arc<QueryContext>,
    target: &TableHookTarget,
) -> Result<MaintenanceOutcome> {
    match analyze_if_needed(ctx, target).await {
        Ok(()) => Ok(MaintenanceOutcome::Completed),
        Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
            // Recheck the latest snapshot after yielding; repeated conflicts use the
            // scheduler's no-progress deadline rather than sleeping inside a grant.
            Ok(MaintenanceOutcome::Deferred(Duration::from_millis(
                1000 + rand::random::<u64>() % 4000,
            )))
        }
        Err(e) => Err(e),
    }
}

async fn analyze_if_needed(ctx: Arc<QueryContext>, target: &TableHookTarget) -> Result<()> {
    ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
    let Some((_, table)) = target.resolve_background(&ctx, TaskKind::Analyze).await? else {
        return Ok(());
    };
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    if fuse_table.get_option(FUSE_OPT_KEY_ENABLE_AUTO_ANALYZE, 0u32) == 0 {
        return Ok(());
    }
    // A queued hint may be stale after coalescing or a concurrent manual ANALYZE.
    if let Some(snapshot) = fuse_table
        .read_table_snapshot_with_policy(ctx.cache_policy())
        .await?
    {
        if snapshot
            .summary
            .additional_stats_meta
            .as_ref()
            .is_some_and(|stats| {
                let actual = snapshot
                    .summary
                    .row_count
                    .saturating_sub(stats.unstats_rows);
                stats.row_count.abs_diff(actual).saturating_mul(10) < actual
            })
        {
            return Ok(());
        }
    }
    analyze_no_scan(ctx, fuse_table).await
}

async fn analyze_no_scan(ctx: Arc<QueryContext>, fuse_table: &FuseTable) -> Result<()> {
    let options =
        AnalyzeOptions::from_table_options(fuse_table.get_table_info().options())?.no_scan();
    execute_analyze(ctx, fuse_table, options).await
}

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
