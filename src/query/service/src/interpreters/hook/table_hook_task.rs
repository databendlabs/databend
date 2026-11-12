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
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_stopped;
use databend_common_pipeline::core::SharedLockGuard;
use log::info;
use log::warn;
use tokio_util::sync::CancellationToken;

use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::recluster_hook::ReclusterProgress;
use crate::interpreters::hook::recluster_hook::ReclusterUnitOutcome;
use crate::interpreters::hook::refresh_hook::RefreshProgress;
use crate::interpreters::hook::resolve_current_table_name_by_id;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextQueryIdentity;
use crate::table_maintenance::MaintenanceContext;
use crate::table_maintenance::MaintenanceIdentity;
use crate::table_maintenance::run_maintenance_unit;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct TableHookTaskKey {
    pub(super) tenant: String,
    pub(super) catalog: String,
    pub(super) table_id: u64,
}

pub(super) trait SchedulerTask: Send + 'static {
    fn key(&self) -> TableHookTaskKey;

    fn merge(&mut self, other: Self);

    fn ready_at(&self) -> Instant {
        Instant::now()
    }
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
    pub refresh_base: Option<String>,
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
        self.execute_refresh()
            .await
            .unwrap_or_else(|e| warn!("Table refresh failed: {e}"));
        if let Some(guard) = self.lock_guard.take() {
            drop(guard.try_take());
        }
        self.execute_analyze()
            .await
            .unwrap_or_else(|e| warn!("Table analyze failed: {e}"));
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
    recluster_progress: Option<ReclusterProgress>,
    recluster_again: bool,
    pub refresh_base: Option<String>,
    refresh_progress: Option<RefreshProgress>,
    refresh_again: bool,
    pub refresh_pending: bool,
    pub enable_auto_analyze: bool,
    analyze_after_recluster: bool,
    pub source_query_id: String,
    pub operation_name: String,
    pub main_operation_start: Instant,
    ready_at: [Instant; 3],
    failures: [u32; 3],
    blocked_since: [Option<Instant>; 3],
    turn: usize,
}

const MAX_ACTION_FAILURES: u32 = 8;
const MAX_ACTION_BLOCKED: Duration = Duration::from_secs(10 * 60);
const ACTION_NAMES: [&str; 3] = ["refresh", "analyze", "recluster"];

enum MaintenanceOutcome {
    Completed,
    Progress,
    Deferred(Duration),
}

impl TableHookRequest {
    fn pending(&self, action: usize) -> bool {
        match action {
            0 => self.refresh_pending,
            1 => self.enable_auto_analyze,
            _ => self.recluster_target.is_some(),
        }
    }

    /// Execute only one batch/round, then relinquish all execution resources. Each action
    /// has its own backoff; a failing heavy action cannot hold up index or statistics work.
    pub(super) async fn execute(mut self, cancellation: CancellationToken) -> Option<Self> {
        let now = Instant::now();
        let action = (0..3)
            .map(|offset| (self.turn + offset) % 3)
            .find(|&action| self.pending(action) && self.ready_at[action] <= now)?;
        self.turn = (action + 1) % 3;
        let result = match CatchUnwindFuture::create(
            self.execute_action(action, cancellation.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                // Unwinding can drop carry taken out of a job, or hide a committed refresh.
                // Never resume that partially mutated progress with a missing frozen scope.
                self.reset_progress(action);
                Err(ErrorCode::Internal(format!(
                    "Maintenance action panicked: {error}"
                )))
            }
        };
        if cancellation.is_cancelled() {
            return None;
        }
        if result
            .as_ref()
            .is_err_and(|error| error.code() == ErrorCode::ABORTED_QUERY)
        {
            self.reset_progress(action);
        }
        self.apply_outcome(action, result);
        (0..3).any(|action| self.pending(action)).then_some(self)
    }

    fn apply_outcome(&mut self, action: usize, result: Result<MaintenanceOutcome>) {
        match result {
            Ok(MaintenanceOutcome::Progress) => {
                self.failures[action] = 0;
                self.blocked_since[action] = None;
            }
            Ok(MaintenanceOutcome::Deferred(delay)) => {
                if self.blocked_too_long(action) {
                    self.stop_action(action, "no_progress");
                } else {
                    self.ready_at[action] = Instant::now() + delay;
                }
            }
            Ok(MaintenanceOutcome::Completed) => {
                self.failures[action] = 0;
                self.blocked_since[action] = None;
                match action {
                    0 => {
                        self.refresh_pending = self.refresh_again;
                        self.refresh_again = false;
                        self.refresh_progress = None;
                        self.refresh_base = None;
                    }
                    1 => self.enable_auto_analyze = false,
                    _ => {
                        self.recluster_progress = None;
                        if self.recluster_again {
                            self.recluster_again = false;
                            self.ready_at[2] = Instant::now() + Duration::from_secs(5);
                        } else {
                            self.recluster_target = None;
                        }
                    }
                }
            }
            Err(e)
                if matches!(
                    e.code(),
                    ErrorCode::UNKNOWN_USER
                        | ErrorCode::UNKNOWN_ROLE
                        | ErrorCode::PERMISSION_DENIED
                        | ErrorCode::AUTHENTICATE_FAILURE
                        | ErrorCode::UNCLUSTERED_TABLE
                        | ErrorCode::UNKNOWN_TABLE
                        | ErrorCode::UNKNOWN_DATABASE
                ) =>
            {
                warn!(table_id = self.target.table_id, action, error :? = e;
                    "Stop maintenance action because its target or execution identity is no longer valid");
                self.stop_action(action, "invalid_target_or_identity");
            }
            Err(e) => {
                if action == 2 {
                    // A failed callback may hide a successful commit. A subsequent
                    // generation must check replacements absent from the old scope.
                    self.recluster_again = true;
                    self.refresh_pending = true;
                    self.refresh_again |= self.refresh_progress.is_some();
                    self.ready_at[0] = Instant::now();
                }
                self.failures[action] = self.failures[action].saturating_add(1);
                if self.failures[action] >= MAX_ACTION_FAILURES || self.blocked_too_long(action) {
                    warn!(table_id = self.target.table_id, action, error :? = e;
                        "Abandon bounded best-effort maintenance attempt; another notification may retry");
                    self.stop_action(action, "retry_exhausted");
                    return;
                }
                let delay = Duration::from_secs(1u64 << self.failures[action].min(8))
                    + Duration::from_millis(rand::random::<u64>() % 1000);
                self.ready_at[action] = Instant::now() + delay;
                warn!(table_id = self.target.table_id, action, delay_secs = delay.as_secs(), error :? = e;
                    "Defer maintenance action without holding execution resources");
            }
        }
    }

    fn reset_progress(&mut self, action: usize) {
        match action {
            0 => {
                self.refresh_progress = None;
                self.refresh_base = None;
            }
            2 => self.recluster_progress = None,
            _ => {}
        }
    }

    fn blocked_too_long(&mut self, action: usize) -> bool {
        self.blocked_since[action]
            .get_or_insert_with(Instant::now)
            .elapsed()
            >= MAX_ACTION_BLOCKED
    }

    fn stop_action(&mut self, action: usize, reason: &'static str) {
        record_stopped(ACTION_NAMES[action], reason);
        self.failures[action] = 0;
        self.blocked_since[action] = None;
        self.ready_at[action] = Instant::now();
        match action {
            0 => {
                self.refresh_pending = false;
                self.refresh_again = false;
                self.refresh_progress = None;
                self.refresh_base = None;
            }
            1 => self.enable_auto_analyze = false,
            _ => {
                self.recluster_target = None;
                self.recluster_progress = None;
                self.recluster_again = false;
            }
        }
    }

    async fn execute_action(
        &mut self,
        action: usize,
        cancellation: CancellationToken,
    ) -> Result<MaintenanceOutcome> {
        let MaintenanceContext { ctx, tracking } = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ErrorCode::AbortedQuery("Maintenance scheduler closed")),
            context = tokio::time::timeout(Duration::from_secs(60), MaintenanceContext::from_identity(&self.identity)) => {
                context.map_err(|_| ErrorCode::AbortedQuery("Maintenance context creation timed out"))??
            }
        };
        let cancellation_ctx = ctx.clone();
        ctx.set_enable_auto_analyze(self.enable_auto_analyze);
        info!(source_query_id = self.source_query_id.as_str(), table_id = self.target.table_id, action;
            "Start async table hook job");
        let task = TableHookTask {
            ctx,
            session_local: false,
            refresh_base: None,
            tracking: Some(tracking.clone()),
            target: self.target.clone(),
            recluster_target: None,
            hook_settings: TableHookTaskSettings {
                compact_after_write: false,
            },
            lock_opt: LockTableOption::LockWithRetry,
            lock_guard: None,
            operation_name: self.operation_name.clone(),
            main_operation_start: self.main_operation_start,
        };
        run_maintenance_unit(
            &cancellation_ctx,
            cancellation,
            tracking.tracking(async {
                match action {
                    0 => task
                        .execute_refresh_batch(&mut self.refresh_base, &mut self.refresh_progress)
                        .await
                        .map(|covered| {
                            if covered {
                                MaintenanceOutcome::Completed
                            } else {
                                MaintenanceOutcome::Progress
                            }
                        }),
                    1 => task
                        .execute_analyze()
                        .await
                        .map(|_| MaintenanceOutcome::Completed),
                    _ => {
                        let target = self
                            .recluster_target
                            .as_ref()
                            .ok_or_else(|| ErrorCode::Internal("Missing recluster target"))?;
                        match task
                            .execute_recluster_round(target, &mut self.recluster_progress)
                            .await?
                        {
                            ReclusterUnitOutcome::Committed => {
                                self.refresh_pending = true;
                                self.refresh_again |= self.refresh_progress.is_some();
                                self.refresh_base = None;
                                self.ready_at[0] = Instant::now();
                                self.enable_auto_analyze |= self.analyze_after_recluster;
                                self.ready_at[2] = Instant::now() + Duration::from_millis(100);
                                Ok(MaintenanceOutcome::Progress)
                            }
                            ReclusterUnitOutcome::ScanProgress => Ok(MaintenanceOutcome::Progress),
                            ReclusterUnitOutcome::Converged => Ok(MaintenanceOutcome::Completed),
                            ReclusterUnitOutcome::Deferred(delay) => {
                                Ok(MaintenanceOutcome::Deferred(delay))
                            }
                        }
                    }
                }
            }),
        )
        .await
    }
}

impl TableHookTask {
    pub(super) async fn into_request(self) -> Result<TableHookRequest> {
        let now = Instant::now();
        Ok(TableHookRequest {
            identity: MaintenanceIdentity::capture(&self.ctx).await?,
            recluster_progress: None,
            recluster_again: false,
            refresh_base: self.refresh_base,
            refresh_progress: None,
            refresh_again: false,
            enable_auto_analyze: self.ctx.get_enable_auto_analyze(),
            analyze_after_recluster: self.ctx.get_enable_auto_analyze(),
            source_query_id: self.ctx.get_id(),
            target: self.target,
            recluster_target: self.recluster_target,
            operation_name: self.operation_name,
            main_operation_start: self.main_operation_start,
            refresh_pending: true,
            ready_at: [now, now, now + Duration::from_secs(5)],
            failures: [0; 3],
            blocked_since: [None; 3],
            turn: 0,
        })
    }
}

impl SchedulerTask for TableHookRequest {
    fn ready_at(&self) -> Instant {
        (0..3)
            .filter(|&action| self.pending(action))
            .map(|action| self.ready_at[action])
            .min()
            .unwrap_or_else(Instant::now)
    }

    fn key(&self) -> TableHookTaskKey {
        TableHookTaskKey {
            tenant: self.identity.tenant.tenant_name().to_string(),
            catalog: self.target.catalog.clone(),
            table_id: self.target.table_id,
        }
    }

    /// Combine pending values, never mutate another query's context.
    fn merge(&mut self, mut other: Self) {
        // Continue the already-claimed frozen scope. New demand gets a subsequent FullCheck
        // rather than restarting a large in-flight scan on every write.
        if self.refresh_progress.is_some() {
            other.refresh_progress = self.refresh_progress.take();
            other.refresh_base = self.refresh_base.take();
            other.refresh_again = true;
        } else if self.refresh_pending {
            // Combining different base snapshots cannot be ordered by UUID. Widen safely.
            other.refresh_base = None;
        }
        other.refresh_again |= self.refresh_again;
        other.enable_auto_analyze |= self.enable_auto_analyze;
        other.analyze_after_recluster |= self.analyze_after_recluster;
        other.refresh_pending |= self.refresh_pending;
        other.turn = self.turn;
        for action in 0..3 {
            // New notifications do not bypass a failing action's backoff.
            other.ready_at[action] = other.ready_at[action].max(self.ready_at[action]);
            other.failures[action] = other.failures[action].max(self.failures[action]);
            other.blocked_since[action] =
                self.blocked_since[action].or(other.blocked_since[action]);
        }

        // Preserve active scope/carry. Compactions arriving during it request one
        // subsequent generation, never widen or restart the current job.
        if self.recluster_progress.is_some() {
            other.recluster_again |= other.recluster_target.is_some();
            other.recluster_progress = self.recluster_progress.take();
        }
        other.recluster_again |= self.recluster_again;
        if self.recluster_target.is_some() {
            // Accumulate from the first dirty notification, not the most recent write.
            other.ready_at[2] = self.ready_at[2];
        }
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
    use crate::sessions::TableContextSettings;
    use crate::test_kits::TestFixture;

    fn table_hook_task(ctx: Arc<QueryContext>, table: &str) -> TableHookTask {
        let hook_settings = TableHookTaskSettings::create(&ctx);
        TableHookTask {
            ctx,
            session_local: false,
            refresh_base: None,
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

        let old_weak = Arc::downgrade(&old_ctx);
        let mut old_task = table_hook_task(old_ctx, "t");
        old_task.refresh_base = Some("old_snapshot".into());
        old_task.recluster_target = Some(old_task.target.clone());
        let mut task = old_task.into_request().await?;
        let dirty_deadline = task.ready_at[2];
        let mut latest = table_hook_task(latest_ctx, "t");
        latest.refresh_base = Some("new_snapshot".into());
        task.merge(latest.into_request().await?);
        assert!(task.recluster_target.is_some());
        assert_eq!(
            task.ready_at[2], dirty_deadline,
            "new writes must not postpone the first dirty deadline"
        );

        assert!(
            task.refresh_base.is_none(),
            "merging incomparable bases must widen to FullCheck"
        );
        assert!(
            old_weak.upgrade().is_none(),
            "queued values must not retain the write context"
        );
        let maintenance = MaintenanceContext::from_identity(&task.identity).await?;
        assert!(maintenance.ctx.is_automatic_maintenance());
        assert!(maintenance.ctx.get_settings().get_max_threads()? <= 2);
        let reservation = crate::table_maintenance::MaintenanceResources::reserve()?;
        assert!(
            crate::table_maintenance::MaintenanceResources::reserve().is_err(),
            "all action memory is reserved"
        );
        drop(reservation);
        assert!(
            crate::table_maintenance::MaintenanceResources::reserve().is_ok(),
            "dropping an action releases memory capacity"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bounded_failures_survive_notifications_and_allow_new_attempts() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let mut task = table_hook_task(ctx.clone(), "t").into_request().await?;
        task.enable_auto_analyze = true;
        task.recluster_target = Some(task.target.clone());
        for attempt in 1..=MAX_ACTION_FAILURES {
            task.apply_outcome(0, Err(ErrorCode::Internal("persistent index error")));
            if attempt < MAX_ACTION_FAILURES {
                assert!(task.refresh_pending);
                let deadline = task.ready_at[0];
                task.merge(table_hook_task(ctx.clone(), "t").into_request().await?);
                assert_eq!(task.failures[0], attempt);
                assert!(task.ready_at[0] >= deadline);
            }
        }
        assert!(
            !task.refresh_pending,
            "fresh writes must not reset the failure limit"
        );
        assert!(
            task.enable_auto_analyze,
            "a failing index must not stop statistics"
        );
        assert!(task.recluster_target.is_some());
        task.merge(table_hook_task(ctx, "t").into_request().await?);
        assert!(
            task.refresh_pending,
            "a later notification may open a fresh attempt"
        );
        assert_eq!(task.failures[0], 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_no_progress_is_bounded_but_real_progress_resets_cooldown() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let mut task = table_hook_task(ctx, "t").into_request().await?;
        task.recluster_target = Some(task.target.clone());
        task.blocked_since[2] = Some(Instant::now() - MAX_ACTION_BLOCKED);
        task.apply_outcome(2, Ok(MaintenanceOutcome::Deferred(Duration::from_secs(1))));
        assert!(task.recluster_target.is_none());
        assert!(task.refresh_pending);

        task.failures[0] = MAX_ACTION_FAILURES - 1;
        task.blocked_since[0] = Some(Instant::now() - MAX_ACTION_BLOCKED);
        task.apply_outcome(0, Ok(MaintenanceOutcome::Progress));
        assert_eq!(task.failures[0], 0);
        assert!(task.blocked_since[0].is_none());
        task.apply_outcome(0, Err(ErrorCode::Internal("transient error")));
        assert!(task.refresh_pending);
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
