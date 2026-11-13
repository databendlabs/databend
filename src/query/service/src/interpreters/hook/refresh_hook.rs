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

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;
use log::info;

use crate::interpreters::hook::table_hook_scheduler::TableHookScheduler;
use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::interpreters::hook::table_hook_task::TableHookTaskKey;
use crate::interpreters::hook::table_id_matches_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;

/// Frozen coverage boundary. Snapshot references, a cursor and sorted baseline positions
/// survive between batches, never block plans, query contexts or executors. The position
/// index is limited to the unmatched baseline range and charged to maintenance memory.
pub(super) struct RefreshProgress {
    snapshot: String,
    definition: String,
    offset: usize,
    range_end: Option<usize>,
    baseline_order: Vec<usize>,
    replacement: Option<Location>,
    // Set before execution and cleared only after replacement bookkeeping. An error may
    // hide a published partial refresh; its old source disappearing is not completion.
    publication_pending: bool,
    // Remains Some only while the scope's snapshot evolves exclusively by our own
    // refresh commits. Concurrent writes do not block coverage, but prohibit publishing
    // their unverified segments as part of a covered snapshot baseline.
    covered_head: Option<String>,
}

impl RefreshProgress {
    fn initialize_range(
        &mut self,
        source: &[Location],
        baseline: Option<&[Location]>,
        index_budget: u64,
    ) {
        let mut range_end = source.len();
        if let Some(baseline) = baseline {
            let prefix = source
                .iter()
                .zip(baseline)
                .take_while(|(source, baseline)| source == baseline)
                .count();
            let suffix = source[prefix..]
                .iter()
                .rev()
                .zip(baseline[prefix..].iter().rev())
                .take_while(|(source, baseline)| source == baseline)
                .count();
            self.offset = prefix;
            range_end -= suffix;
            // Snapshots have unique segments. Entries in the common edges cannot
            // occur again in the middle, so only index unmatched baseline positions.
            let unmatched = prefix..baseline.len() - suffix;
            let index_bytes = unmatched.len().saturating_mul(size_of::<usize>());
            if index_bytes as u64 <= index_budget {
                self.baseline_order = unmatched.collect();
                self.baseline_order
                    .sort_unstable_by(|a, b| baseline[*a].cmp(&baseline[*b]));
            }
            // Above the explicit index allowance, conservatively check the middle
            // instead of retaining a full-table index or using unbudgeted spill.
        }
        self.range_end = Some(range_end);
    }

    fn complete(&self, key: TableHookTaskKey) -> bool {
        if let (Some(scheduler), Some(location)) = (
            TableHookScheduler::try_instance(),
            self.covered_head.as_ref(),
        ) {
            scheduler.record_refresh_coverage(key, self.definition.clone(), location.clone());
        }
        true
    }
}

impl TableHookTask {
    pub(super) async fn execute_refresh(&self) -> Result<()> {
        // Maintenance after the write: `CachePolicy::MAINTENANCE` for its reads, on the context
        // of the write, whose own reads are done by now.
        self.ctx
            .set_cache_policy(self.ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        info!("Table hook starting refresh job");
        self.do_refresh(self.ctx.clone()).await
    }

    /// Cover one source segment with bounded block/byte work. Return true only when the
    /// frozen scope is covered. Replacement segment references preserve intra-segment
    /// progress when a bounded refresh rewrites only some of its blocks.
    pub(super) async fn execute_refresh_batch(
        &self,
        base: &mut Option<String>,
        progress: &mut Option<RefreshProgress>,
    ) -> Result<bool> {
        let ctx = &self.ctx;
        ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let Some(desc) = self.target.resolve(ctx, "refresh").await? else {
            return Ok(true);
        };
        let table = ctx
            .get_table(&desc.catalog, &desc.database, &desc.table)
            .await?;
        let Ok(fuse) = FuseTable::try_from_table(table.as_ref()) else {
            return Ok(true);
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
            return Ok(true);
        }
        let key = TableHookTaskKey {
            tenant: ctx.get_tenant().tenant_name().to_string(),
            catalog: desc.catalog.clone(),
            table_id: desc.table_id,
        };
        let definition = serde_json::to_string(&(
            &table.get_table_info().meta.schema,
            &table.get_table_info().meta.indexes,
        ))?;
        let retry_publication = progress
            .as_ref()
            .is_some_and(|progress| progress.publication_pending);
        if retry_publication {
            info!(event = "refresh.publication_unknown", table_id = desc.table_id;
                "Recheck current index coverage after incomplete publication bookkeeping");
        }
        if retry_publication
            || progress
                .as_ref()
                .is_some_and(|progress| progress.definition != definition)
        {
            *progress = None;
            *base = None;
        }
        if progress.is_none() {
            let Some(snapshot) = fuse.snapshot_loc() else {
                return Ok(true);
            };
            if !retry_publication {
                *base = TableHookScheduler::try_instance()
                    .and_then(|scheduler| scheduler.refresh_base(&key, &definition));
            }
            info!(event = "refresh.scope_started", table_id = desc.table_id,
                snapshot = snapshot.as_str(), incremental = base.is_some();
                "Start frozen index coverage scope");
            *progress = Some(RefreshProgress {
                covered_head: Some(snapshot.clone()),
                snapshot,
                definition,
                offset: 0,
                range_end: None,
                baseline_order: Vec::new(),
                replacement: None,
                publication_pending: false,
            });
        }
        let state = progress.as_mut().expect("refresh progress initialized");
        if state.covered_head != fuse.snapshot_loc() {
            state.covered_head = None;
        }
        let source = match fuse
            .read_table_snapshot_with_location(Some(state.snapshot.clone()))
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return Ok(true),
            Err(error) if error.code() == ErrorCode::STORAGE_NOT_FOUND => {
                // History GC/Flashback may invalidate a reference. Recheck current coverage;
                // never interpret an unreadable historical boundary as successful coverage.
                info!(table_id = desc.table_id; "Refresh boundary expired; schedule FullCheck");
                *progress = None;
                *base = None;
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let baseline = if let Some(location) = base
            .as_ref()
            .filter(|_| state.range_end.is_none() || !state.baseline_order.is_empty())
        {
            match fuse
                .read_table_snapshot_with_location(Some(location.clone()))
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) if error.code() == ErrorCode::STORAGE_NOT_FOUND => {
                    *base = None;
                    state.baseline_order = Vec::new();
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        if state.range_end.is_none() {
            state.initialize_range(
                &source.segments,
                baseline
                    .as_ref()
                    .map(|snapshot| snapshot.segments.as_slice()),
                ctx.get_settings().get_max_query_memory_usage()? / 64,
            );
        }
        let scope_end = state.range_end.expect("refresh range initialized");
        // Compare the frozen boundaries once, then reuse a compact exact membership
        // index. Appends need no baseline index or units to revisit the covered suffix.
        let end = scope_end.min(state.offset.saturating_add(64));
        while state.offset < end {
            let original = &source.segments[state.offset];
            if state.replacement.is_none()
                && baseline.as_ref().is_some_and(|baseline| {
                    state
                        .baseline_order
                        .binary_search_by(|&index| baseline.segments[index].cmp(original))
                        .is_ok()
                })
            {
                state.offset += 1;
                continue;
            }
            let location = state.replacement.as_ref().unwrap_or(original).clone();
            let mut pipeline = Pipeline::create();
            let count = fuse
                .do_refresh_indexes_batch(
                    ctx.clone(),
                    &names,
                    vec![location],
                    &mut pipeline,
                    (32, ctx.get_settings().get_max_query_memory_usage()? / 4),
                )
                .await?;
            if pipeline.is_empty() {
                state.offset += 1;
                state.replacement = None;
                return Ok(state.offset >= scope_end && state.complete(key));
            }
            pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
            let query_ctx = ctx.clone();
            pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
                hook_clear_m_cte_temp_table(&query_ctx)?;
                hook_vacuum_temp_files(&query_ctx)?;
                hook_disk_temp_dir(&query_ctx)?;
                Ok(())
            }));
            let executor = PipelineCompleteExecutor::try_create(
                pipeline,
                ExecutorSettings::try_create(ctx.clone())?,
            )?;
            ctx.set_executor(executor.get_inner())?;
            state.publication_pending = true;
            executor.execute().await?;
            let outputs = ctx.written_segment_locations().list();
            if outputs.len() != 1 {
                return Err(ErrorCode::Internal(
                    "A one-segment refresh must publish exactly one replacement",
                ));
            }
            state.replacement = outputs.into_iter().next();
            if state.covered_head.is_some() {
                let previous = fuse
                    .read_table_snapshot()
                    .await?
                    .ok_or_else(|| ErrorCode::Internal("Refresh source snapshot disappeared"))?;
                let current = ctx
                    .get_catalog(&desc.catalog)
                    .await?
                    .get_table(&ctx.get_tenant(), &desc.database, &desc.table)
                    .await?;
                let current = FuseTable::try_from_table(current.as_ref())?;
                let committed = current.read_table_snapshot().await?;
                state.covered_head = committed
                    .filter(|snapshot| {
                        snapshot
                            .prev_snapshot_id
                            .is_some_and(|(id, _)| id == previous.snapshot_id)
                    })
                    .and_then(|_| current.snapshot_loc());
            }
            state.publication_pending = false;
            info!(table_id = desc.table_id, segment_offset = state.offset, refreshed_blocks = count;
                "Refresh batch committed; coverage remains pending");
            return Ok(false);
        }
        Ok(state.offset >= scope_end && state.complete(key))
    }

    async fn do_refresh(&self, ctx: Arc<QueryContext>) -> Result<()> {
        let Some(desc) = self.target.resolve(&ctx, "refresh").await? else {
            return Ok(());
        };

        // Target resolution refreshed the table cache and checked its ID. Reuse that
        // current table to filter hints against the post-maintenance snapshot.
        let table = ctx
            .get_table(&desc.catalog, &desc.database, &desc.table)
            .await?;
        if !table_id_matches_target(
            "refresh",
            Some(desc.table_id),
            table.get_id(),
            &desc.catalog,
            &desc.database,
            &desc.table,
        ) {
            return Ok(());
        }
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

        // The shared builder has per-index worker budgets; automatic maintenance uses
        // one total pipeline budget rather than multiplying it by the number of indexes.
        pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
        let query_ctx = ctx.clone();
        pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
            hook_clear_m_cte_temp_table(&query_ctx)?;
            hook_vacuum_temp_files(&query_ctx)?;
            hook_disk_temp_dir(&query_ctx)?;
            Ok(())
        }));
        let executor = PipelineCompleteExecutor::try_create(
            pipeline,
            ExecutorSettings::try_create(ctx.clone())?,
        )?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute().await
    }
}
