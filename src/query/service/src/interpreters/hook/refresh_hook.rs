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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::table_context::RefreshAnchor;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::table_maintenance::scheduler::TaskKind;
use crate::table_maintenance::task::MaintenanceOutcome;

const MAX_ANCHOR_HOPS: usize = 32;

/// A coalesced notification checks the physical changes since its oldest write.
pub(crate) async fn run(
    ctx: Arc<QueryContext>,
    target: &TableHookTarget,
    anchor: &RefreshAnchor,
) -> Result<MaintenanceOutcome> {
    ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
    let Some((_, table)) = target.resolve_background(&ctx, TaskKind::Refresh).await? else {
        return Ok(MaintenanceOutcome::Completed);
    };
    let names = table
        .get_table_info()
        .meta
        .indexes
        .values()
        .filter(|index| !index.sync_creation)
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Ok(MaintenanceOutcome::Completed);
    }
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let policy = ctx.cache_policy();
    let Some(snapshot) = fuse.read_table_snapshot_with_policy(policy).await? else {
        return Ok(MaintenanceOutcome::Completed);
    };
    let Some(location) = fuse.snapshot_loc() else {
        return Ok(MaintenanceOutcome::Completed);
    };
    let segments = match find_base(
        fuse,
        &snapshot,
        &location,
        &anchor.snapshot_location,
        policy,
    )
    .await?
    {
        RefreshBase::Snapshot(base) => {
            let previous = base.segments.iter().collect::<HashSet<_>>();
            snapshot
                .segments
                .iter()
                .filter(|loc| !previous.contains(loc))
                .cloned()
                .collect::<Vec<_>>()
        }
        RefreshBase::Empty => snapshot.segments.clone(),
        RefreshBase::Unavailable(reason) => {
            info!(event = "refresh.scope_skipped", table_id = target.table_id, reason,
                snapshot = location.as_str(), anchor = anchor.snapshot_location.as_str();
                "Skip Refresh because its write scope could not be resolved");
            return Ok(MaintenanceOutcome::Completed);
        }
    };
    drop(snapshot);
    info!(event = "refresh.scope_started", table_id = target.table_id,
        snapshot = location.as_str(), candidate_segments = segments.len(), incremental = true;
        "Refresh snapshot difference in one publication");
    // Publication conflicts go through the scheduler's bounded retry backoff.
    refresh_indexes(ctx, fuse, &names, segments).await?;
    Ok(MaintenanceOutcome::Completed)
}

async fn refresh_indexes(
    ctx: Arc<QueryContext>,
    fuse: &FuseTable,
    names: &[String],
    segments: Vec<Location>,
) -> Result<()> {
    let mut pipeline = Pipeline::create();
    fuse.do_refresh_indexes(ctx.clone(), names, Some(segments), &mut pipeline)
        .await?;
    if pipeline.is_empty() {
        return Ok(());
    }
    pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
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

/// Not finding the baseline is distinct from proving the write had no parent.
enum RefreshBase {
    Snapshot(Arc<TableSnapshot>),
    Empty,
    Unavailable(&'static str),
}

async fn find_base(
    table: &FuseTable,
    head: &TableSnapshot,
    head_location: &str,
    anchor_location: &str,
    policy: CachePolicy,
) -> Result<RefreshBase> {
    // Don't populate the cache with historical snapshots just to prove ancestry.
    let policy = policy.at_most(CachePolicy::READ_ONLY);
    let mut previous = head.prev_snapshot_id;
    let mut matches_anchor = head_location == anchor_location;
    let mut hops = 0;
    loop {
        let Some((id, version)) = previous else {
            return Ok(if matches_anchor {
                RefreshBase::Empty
            } else {
                RefreshBase::Unavailable("anchor_not_ancestor")
            });
        };
        if !matches_anchor && hops == MAX_ANCHOR_HOPS {
            return Ok(RefreshBase::Unavailable("anchor_hop_limit"));
        }
        let location = table
            .meta_location_generator()
            .gen_snapshot_location(&id, version)?;
        let is_anchor = location == anchor_location;
        let Some(parent) = read_history_snapshot(table, location, policy).await? else {
            return Ok(RefreshBase::Unavailable(if matches_anchor {
                "missing_base"
            } else {
                "missing_history"
            }));
        };
        if matches_anchor {
            return Ok(RefreshBase::Snapshot(parent));
        }
        previous = parent.prev_snapshot_id;
        matches_anchor = is_anchor;
        hops += 1;
    }
}

async fn read_history_snapshot(
    table: &FuseTable,
    location: String,
    policy: CachePolicy,
) -> Result<Option<Arc<TableSnapshot>>> {
    match table
        .read_table_snapshot_with_location_and_policy(Some(location), policy)
        .await
    {
        Ok(snapshot) => Ok(snapshot),
        Err(error) if error.code() == ErrorCode::STORAGE_NOT_FOUND => Ok(None),
        Err(error) => Err(error),
    }
}
