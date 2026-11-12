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

// Logs from this module will show up as "[TABLE-HOOK] ...".
databend_common_tracing::register_module_tag!("[TABLE-HOOK]");

use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_pipeline::core::SharedLockGuard;
use databend_storages_common_table_meta::meta::Location;
use log::info;
use log::warn;

use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextQueryIdentity;
use crate::table_maintenance::MaintenanceContext;
use crate::table_maintenance::MaintenanceIdentity;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TableHookTaskKey {
    pub(super) tenant: String,
    pub(super) catalog: String,
    pub(super) table_id: u64,
}

pub(super) trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;

    fn merge(&mut self, other: Self);
}

#[derive(Clone)]
pub struct TableHookTarget {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub table_id: u64,
}

impl TableHookTarget {
    /// Resolve separately for each stage: maintenance can outlive a rename or drop.
    /// Resolution refreshes the table cache and clears cached table metadata timestamps.
    pub(super) async fn resolve(
        &self,
        ctx: &Arc<QueryContext>,
        hook_name: &str,
    ) -> Result<Option<Self>> {
        let Some((database, table)) = resolve_current_table_name_by_id(
            ctx,
            hook_name,
            &self.catalog,
            &self.database,
            &self.table,
            Some(self.table_id),
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            catalog: self.catalog.clone(),
            database,
            table,
            table_id: self.table_id,
        }))
    }
}

pub struct TableHookTask {
    pub ctx: Arc<QueryContext>,
    pub session_local: bool,
    /// Tracking of a context created for this task. `None` when the task runs on the
    /// triggering statement's context and is accounted to that statement.
    pub tracking: Option<Arc<TrackingPayload>>,
    pub target: TableHookTarget,
    /// The target the compact stage compacted. A recluster round follows it.
    pub recluster_target: Option<TableHookTarget>,
    pub hook_settings: TableHookTaskSettings,
    pub lock_opt: LockTableOption,
    /// Only synchronous autocommit tasks carry a statement lock. Queued and post-commit
    /// tasks carry none and acquire maintenance locks where required.
    pub lock_guard: Option<SharedLockGuard>,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

impl TableHookTask {
    /// Compact before the triggering statement returns, in both modes. Records the compacted
    /// target so that the remaining stages follow it with a recluster round.
    pub(crate) async fn execute_compact_stage(&mut self) {
        match self.tracking.clone() {
            Some(tracking) => tracking.tracking(self.compact_stage()).await,
            None => self.compact_stage().await,
        }
    }

    /// Run the stages after compaction: recluster after a successful compaction, refresh,
    /// then analyze. Synchronous tasks run them on the caller; asynchronous tasks run them
    /// in the background scheduler.
    pub(crate) async fn execute_remaining_stages(self, is_async: bool) {
        match self.tracking.clone() {
            Some(tracking) => tracking.tracking(self.remaining_stages(is_async)).await,
            None => self.remaining_stages(is_async).await,
        }
    }

    async fn compact_stage(&mut self) {
        if !self.hook_settings.compact_after_write {
            return;
        }
        match self.execute_compact().await {
            Ok(compacted) => self.recluster_target = compacted,
            Err(e) => warn!(
                "Table hook compact failed, table_id={}: {e}",
                self.target.table_id
            ),
        }
    }

    /// Release any handed-over statement lock after refresh and before analyze.
    ///
    /// Each stage lives with its own hook module: `execute_compact` in `compact_hook.rs`,
    /// `execute_recluster` in `recluster_hook.rs`, `execute_refresh` in `refresh_hook.rs`,
    /// and `execute_analyze` in `analyze_hook.rs`.
    async fn remaining_stages(mut self, is_async: bool) {
        if is_async {
            info!(
                "Start async table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Start sync table hook job for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
        if let Some(compacted) = self.recluster_target.take() {
            self.execute_recluster(&compacted)
                .await
                .unwrap_or_else(|e| {
                    warn!(
                        "Table hook recluster failed, table_id={}: {e}",
                        self.target.table_id
                    )
                });
        }
        self.execute_refresh().await;
        if let Some(guard) = self.lock_guard.take() {
            drop(guard.try_take());
        }
        self.execute_analyze().await;
        if is_async {
            info!(
                "Async table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        } else {
            info!(
                "Sync table hook job completed for operation {}, table_id={}, table={}",
                self.operation_name, self.target.table_id, self.target.table
            );
        }
    }
}

/// Queued maintenance contains values only. In particular, it must not keep a write's
/// QueryContext, transaction manager, executor, session or handed-over lock alive.
pub(super) struct TableHookRequest {
    pub identity: MaintenanceIdentity,
    pub target: TableHookTarget,
    pub recluster_target: Option<TableHookTarget>,
    pub written_segments: Vec<Location>,
    pub enable_auto_analyze: bool,
    pub source_query_id: String,
    pub operation_name: String,
    pub main_operation_start: Instant,
}

impl TableHookRequest {
    pub(super) async fn execute(self) -> Result<()> {
        let MaintenanceContext { ctx, tracking } =
            MaintenanceContext::from_identity(&self.identity).await?;
        ctx.written_segment_locations()
            .extend(self.written_segments);
        ctx.set_enable_auto_analyze(self.enable_auto_analyze);
        info!(source_query_id = self.source_query_id.as_str(), table_id = self.target.table_id;
            "Create independent table maintenance context");
        TableHookTask {
            ctx,
            session_local: false,
            tracking: Some(tracking),
            target: self.target,
            recluster_target: self.recluster_target,
            hook_settings: TableHookTaskSettings {
                compact_after_write: false,
            },
            lock_opt: LockTableOption::LockWithRetry,
            lock_guard: None,
            operation_name: self.operation_name,
            main_operation_start: self.main_operation_start,
        }
        .execute_remaining_stages(true)
        .await;
        Ok(())
    }
}

impl TableHookTask {
    pub(super) async fn into_request(self) -> Result<TableHookRequest> {
        Ok(TableHookRequest {
            identity: MaintenanceIdentity::capture(&self.ctx).await?,
            written_segments: self.ctx.written_segment_locations().list(),
            enable_auto_analyze: self.ctx.get_enable_auto_analyze(),
            source_query_id: self.ctx.get_id(),
            target: self.target,
            recluster_target: self.recluster_target,
            operation_name: self.operation_name,
            main_operation_start: self.main_operation_start,
        })
    }
}

impl SchedulerTask for TableHookRequest {
    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            tenant: self.identity.tenant.tenant_name().to_string(),
            catalog: self.target.catalog.clone(),
            table_id: self.target.table_id,
        }
    }

    /// Combine pending values, never mutate another query's context.
    fn merge(&mut self, mut other: Self) {
        other.written_segments.append(&mut self.written_segments);
        other.written_segments.sort_unstable();
        other.written_segments.dedup();
        other.enable_auto_analyze |= self.enable_auto_analyze;

        // Each compacted write asks for a recluster round; one round covers them all.
        if other.recluster_target.is_none() {
            other.recluster_target = self.recluster_target.take();
        }

        other.main_operation_start = self.main_operation_start;
        other.operation_name = self.operation_name.clone();

        *self = other;
    }
}

#[derive(Clone, Copy)]
pub struct TableHookTaskSettings {
    pub compact_after_write: bool,
}

impl TableHookTaskSettings {
    pub fn create(ctx: &Arc<QueryContext>) -> Self {
        Self {
            compact_after_write: compact_after_write_enabled(ctx),
        }
    }
}

/// Run maintenance work without adding its reads and writes to the progress of `ctx`. On an
/// autocommit task the context still belongs to the triggering statement.
pub(super) async fn keep_progress<T>(ctx: &Arc<QueryContext>, work: impl Future<Output = T>) -> T {
    let write_progress = ctx.get_write_progress();
    let write_progress_value = write_progress.as_ref().get_values();
    let scan_progress = ctx.get_scan_progress();
    let scan_progress_value = scan_progress.as_ref().get_values();

    let output = work.await;

    write_progress.set(&write_progress_value);
    scan_progress.set(&scan_progress_value);
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_kits::TestFixture;

    fn table_hook_task(ctx: Arc<QueryContext>, table: &str) -> TableHookTask {
        let hook_settings = TableHookTaskSettings::create(&ctx);
        TableHookTask {
            ctx,
            session_local: false,
            tracking: None,
            recluster_target: None,
            target: TableHookTarget {
                catalog: "default".to_string(),
                database: "default".to_string(),
                table: table.to_string(),
                table_id: 1,
            },
            hook_settings,
            lock_opt: LockTableOption::NoLock,
            lock_guard: None,
            operation_name: "insert".to_string(),
            main_operation_start: Instant::now(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_request_merge_releases_write_context() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx
            .written_segment_locations()
            .extend(vec![("a".into(), 1)]);
        latest_ctx
            .written_segment_locations()
            .extend(vec![("b".into(), 1)]);
        let old_weak = Arc::downgrade(&old_ctx);
        let mut task = table_hook_task(old_ctx, "t").into_request().await?;
        task.merge(table_hook_task(latest_ctx, "t").into_request().await?);

        assert_eq!(task.written_segments, vec![
            ("a".into(), 1),
            ("b".into(), 1)
        ]);
        assert!(
            old_weak.upgrade().is_none(),
            "queued values must not retain the write context"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_table_hook_request_merge_preserves_analyze_demand() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let old_ctx = fixture.new_query_ctx().await?;
        let latest_ctx = fixture.new_query_ctx().await?;

        old_ctx.set_enable_auto_analyze(true);
        latest_ctx.set_enable_auto_analyze(false);
        let mut task = table_hook_task(old_ctx, "t").into_request().await?;
        task.merge(table_hook_task(latest_ctx, "t").into_request().await?);

        assert!(task.enable_auto_analyze);
        Ok(())
    }
}
