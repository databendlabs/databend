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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;
use parking_lot::Mutex;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::table_maintenance::scheduler::TableKey;
use crate::table_maintenance::scheduler::TaskKind;
use crate::table_maintenance::task::MaintenanceOutcome;

/// Only confirmed, definition-bound coverage may become a baseline. Eviction is safe:
/// the next notification performs a full check, using the ordinary refresh planner.
pub(crate) struct RefreshCoverage {
    entries: Mutex<HashMap<TableKey, (String, String)>>,
    capacity: usize,
}

impl RefreshCoverage {
    pub(crate) fn create(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
        })
    }

    fn base(&self, key: &TableKey, definition: &str) -> Option<String> {
        self.entries
            .lock()
            .get(key)
            .filter(|(covered, _)| covered == definition)
            .map(|(_, location)| location.clone())
    }

    fn record(&self, key: TableKey, definition: String, location: String) {
        let mut entries = self.entries.lock();
        if entries.len() >= self.capacity && !entries.contains_key(&key) {
            if let Some(evict) = entries.keys().next().cloned() {
                entries.remove(&evict);
            }
        }
        entries.insert(key, (definition, location));
    }
}

/// A coalesced notification checks the snapshot difference rather than statement-local hints.
pub(crate) async fn run(
    ctx: Arc<QueryContext>,
    target: &TableHookTarget,
    coverage: &RefreshCoverage,
) -> Result<MaintenanceOutcome> {
    ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
    let Some((desc, table)) = target.resolve_background(&ctx, TaskKind::Refresh).await? else {
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
    let key = TableKey {
        tenant: ctx.get_tenant().tenant_name().to_string(),
        catalog: target.catalog.clone(),
        table_id: target.table_id,
    };
    let definition = serde_json::to_string(&(
        &table.get_table_info().meta.schema,
        &table.get_table_info().meta.indexes,
    ))?;
    let baseline = match coverage.base(&key, &definition) {
        Some(base) => read_baseline(fuse, base, policy).await?,
        None => None,
    };
    let segments = baseline.map(|base| {
        let covered = base.segments.iter().collect::<HashSet<_>>();
        snapshot
            .segments
            .iter()
            .filter(|segment| !covered.contains(segment))
            .cloned()
            .collect()
    });
    info!(event = "refresh.scope_started", table_id = target.table_id,
            snapshot = location.as_str(), incremental = segments.is_some();
            "Refresh snapshot difference in one publication");
    if !refresh_indexes(ctx.clone(), fuse, &names, segments).await? {
        // All selected indexes were already current. Unlike a refreshed input,
        // this immutable snapshot itself really is covered.
        coverage.record(key, definition, location);
        return Ok(MaintenanceOutcome::Completed);
    }

    // Never label the input snapshot covered: its old segment metadata can be
    // restored by FLASHBACK. The snapshot's predecessor table sequence and parent
    // identify our fixed-snapshot publication without tracking written segments.
    // Meta sequences are global, not per-table increments. Concurrent writes/rollback
    // make the current head ambiguous; retain the older baseline in that case.
    let current = ctx
        .get_catalog(&desc.catalog)
        .await?
        .get_table(&ctx.get_tenant(), &desc.database, &desc.table)
        .await?;
    if current.get_id() == target.table_id {
        let current = FuseTable::try_from_table(current.as_ref())?;
        if let Some(committed) = current.read_table_snapshot_with_policy(policy).await? {
            if committed.prev_table_seq == Some(table.get_table_info().ident.seq)
                && committed
                    .prev_snapshot_id
                    .is_some_and(|(id, _)| id == snapshot.snapshot_id)
            {
                if let Some(location) = current.snapshot_loc() {
                    coverage.record(key, definition, location);
                }
            }
        }
    }
    Ok(MaintenanceOutcome::Completed)
}

/// Returns whether an index publication was needed.
async fn refresh_indexes(
    ctx: Arc<QueryContext>,
    fuse: &FuseTable,
    names: &[String],
    segments: Option<Vec<Location>>,
) -> Result<bool> {
    let mut pipeline = Pipeline::create();
    fuse.do_refresh_indexes(ctx.clone(), names, segments, &mut pipeline)
        .await?;
    if pipeline.is_empty() {
        return Ok(false);
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
    executor.execute().await?;
    Ok(true)
}

/// Baseline segments are covered immutable metadata, not an input processing watermark.
/// A rollback restoring old, unrefreshed segments therefore includes them in the difference
/// even on another branch. No ancestry walk or output-segment lineage is necessary.
async fn read_baseline(
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
