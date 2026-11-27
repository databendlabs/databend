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

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::take_alloc_error_panic;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::RefreshAnchor;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_rejected;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SharedLockGuard;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_session::ReclusterWriteHint;
use databend_storages_common_session::TableHookScope;
use databend_storages_common_session::TxnTableHook;
use log::warn;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSession;
use crate::sessions::TableContextTableAccess;
use crate::table_maintenance::MaintenanceContext;
use crate::table_maintenance::TableMaintenanceService;
use crate::table_maintenance::task::MaintenanceTask;

/// Register the release point of a handed-over table lock on the finished-callback chain.
///
/// The release is a normal callback so it runs in chain order: callbacks registered before it
/// still run under the lock, callbacks registered after it run without the lock. An always
/// callback is added as a safety net in case an earlier callback failed and interrupted the
/// normal chain. Both are no-ops once the guard has been taken.
fn register_lock_release(pipeline: &mut Pipeline, lock_guard: &SharedLockGuard) {
    let guard = lock_guard.clone();
    pipeline.set_on_finished(move |_info: &ExecutionInfo| {
        drop(guard.try_take());
        Ok(())
    });

    let guard = lock_guard.clone();
    pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
        drop(guard.try_take());
        Ok(())
    }));
}

/// Hook operator.
pub struct HookOperator {
    ctx: Arc<QueryContext>,
    catalog: String,
    database: String,
    table: String,
    mutation_kind: MutationKind,
    lock_opt: LockTableOption,
    scope: TableHookScope,
    /// The table lock acquired by the main operation, if any.
    ///
    /// The main pipeline and inline Compact run under this lock.
    /// Release it before inline Analyze or dispatching any background maintenance.
    lock_guard: Option<SharedLockGuard>,
}

impl HookOperator {
    pub fn create(
        ctx: Arc<QueryContext>,
        catalog: String,
        database: String,
        table: String,
        mutation_kind: MutationKind,
        lock_opt: LockTableOption,
    ) -> Self {
        Self {
            ctx,
            catalog,
            database,
            table,
            mutation_kind,
            lock_opt,
            scope: TableHookScope::Full,
            lock_guard: None,
        }
    }

    /// Hand the main operation's table lock over to the write-finished callbacks.
    ///
    /// The caller must not also register the guard on the pipeline; these callbacks own its
    /// release point. Callers that hand over a lock should pass `LockTableOption::NoLock` as
    /// `lock_opt`, otherwise the compact hook would queue a second lock revision behind the
    /// one it already holds.
    pub fn with_lock_guard(mut self, lock_guard: Option<SharedLockGuard>) -> Self {
        self.lock_guard = lock_guard;
        self
    }

    pub(crate) fn with_scope(mut self, scope: TableHookScope) -> Self {
        self.scope = scope;
        self
    }

    /// Register write-finished callbacks for maintenance and lock release.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute(self, pipeline: &mut Pipeline) {
        if pipeline.is_empty() {
            return;
        }
        let Some((target, route)) = self.resolve_target().await else {
            self.release_lock_guard(pipeline);
            return;
        };
        let full = self.scope == TableHookScope::Full;
        if !full && !route.queues_background() {
            self.release_lock_guard(pipeline);
            return;
        }
        if self.ctx.txn_mgr().lock().is_active() {
            // Preserve the statement's lock lifetime, but never execute or enqueue automatic
            // maintenance until the user transaction has actually committed.
            self.release_lock_guard(pipeline);
            self.defer_until_commit(pipeline, target);
            return;
        }
        let capture_refresh = full && route.queues_background();
        if capture_refresh {
            self.ctx.expect_refresh_anchor(target.table_id);
        }
        let compact_after_write = full && compact_after_write_enabled(&self.ctx);
        let main_operation_start = Instant::now();
        let lock_release = self.lock_guard.clone();
        let Self {
            ctx,
            lock_opt,
            lock_guard,
            mutation_kind,
            ..
        } = self;
        register_write_hook(pipeline, target.table_id, move || {
            GlobalIORuntime::instance().block_on(async move {
                let write = ctx.take_recluster_write(target.table_id);
                // Take the original write's anchor before inline Compact can publish.
                let refresh = capture_refresh
                    .then(|| ctx.take_refresh_anchor(target.table_id))
                    .flatten();
                if full {
                    route
                        .run_inline(TableHookTask {
                            ctx: &ctx,
                            target: &target,
                            compact_after_write,
                            lock_opt,
                            lock_guard,
                            operation_name: mutation_kind.to_string(),
                            main_operation_start,
                        })
                        .await;
                } else if let Some(guard) = lock_guard {
                    drop(guard.try_take());
                }
                route.enqueue(&ctx, target, write, refresh).await;
                Ok(())
            })
        });
        // Fallback for an interrupted callback chain.
        if let Some(guard) = &lock_release {
            register_lock_release(pipeline, guard);
        }
    }

    fn defer_until_commit(&self, pipeline: &mut Pipeline, target: TableHookTarget) {
        let TableHookTarget {
            catalog,
            database,
            table,
            table_id,
        } = target;
        let before = self
            .ctx
            .txn_mgr()
            .lock()
            .buffered_snapshot_location(table_id);
        let ctx = self.ctx.clone();
        let scope = self.scope;
        let full = scope == TableHookScope::Full;
        let compact_after_write = full && compact_after_write_enabled(&ctx);
        pipeline.set_on_finished(move |info: &ExecutionInfo| {
            if info.res.is_ok() {
                let recluster_write = ctx.take_recluster_write(table_id);
                if !full && recluster_write.is_none() {
                    return Ok(());
                }
                let hook = TxnTableHook {
                    catalog,
                    database,
                    scope,
                    compaction_num_block_hint: if compact_after_write {
                        ctx.get_compaction_num_block_hint(&table)
                    } else {
                        0
                    },
                    table,
                    table_id,
                    enable_auto_analyze: full && ctx.get_enable_auto_analyze(),
                    recluster_write,
                };
                ctx.txn_mgr()
                    .lock()
                    .defer_table_hook(hook, before.as_deref());
            }
            Ok(())
        });
    }

    fn release_lock_guard(&self, pipeline: &mut Pipeline) {
        if let Some(lock_guard) = &self.lock_guard {
            register_lock_release(pipeline, lock_guard);
        }
    }

    async fn resolve_target(&self) -> Option<(TableHookTarget, HookRoute)> {
        let table = match self
            .ctx
            .get_table(&self.catalog, &self.database, &self.table)
            .await
        {
            Ok(table) => table,
            Err(e) => {
                warn!(
                    "Failed to resolve table hook target {}.{}.{}: {e}",
                    self.catalog, self.database, self.table
                );
                return None;
            }
        };
        let target = TableHookTarget {
            catalog: self.catalog.clone(),
            database: self.database.clone(),
            table: self.table.clone(),
            table_id: table.get_id(),
        };
        let route = HookRoute::for_table(&target, table.as_ref());
        Some((target, route))
    }
}

/// Compact always runs inline when triggered. Other work depends on the table type.
pub(super) enum HookRoute {
    /// Non-Fuse or read-only for maintenance: Compact only.
    CompactOnly,
    /// TEMP, TRANSIENT and `default.system_history`: inline Compact and Analyze only.
    ForegroundOnly,
    /// Ordinary persistent Fuse: also queue index Refresh and eligible FINAL rounds.
    Background,
}

impl HookRoute {
    /// COMMIT uses the same routing rule with the table's current name.
    pub(super) fn for_table(target: &TableHookTarget, table: &dyn Table) -> Self {
        let Some(fuse) = table.as_any().downcast_ref::<FuseTable>() else {
            return Self::CompactOnly;
        };
        if table.is_read_only_for_maintenance() {
            return Self::CompactOnly;
        }
        // A queued index publication on a transient table can vacuum objects still
        // needed by the next explicit transaction, even with a fixed-snapshot commit.
        if table.is_temp() || fuse.is_transient() || target.is_system_history() {
            return Self::ForegroundOnly;
        }
        Self::Background
    }

    pub(super) fn queues_background(&self) -> bool {
        matches!(self, Self::Background)
    }

    async fn run_inline(&self, task: TableHookTask<'_>) {
        task.execute_compact().await;
        if let Some(guard) = &task.lock_guard {
            drop(guard.try_take());
        }
        // Statistics still publish through the table version, so Analyze remains inline.
        if !matches!(self, Self::CompactOnly) {
            task.execute_analyze().await;
        }
    }

    async fn enqueue(
        &self,
        ctx: &Arc<QueryContext>,
        target: TableHookTarget,
        write: Option<ReclusterWriteHint>,
        refresh: Option<RefreshAnchor>,
    ) {
        // Only the worker checks index definitions: the writer may predate CREATE INDEX.
        if self.queues_background() && (write.is_some() || refresh.is_some()) {
            enqueue_table_hook(ctx, target, write, refresh).await;
        }
    }
}

/// All routes run after the write has succeeded. Isolate only maintenance, including
/// `Runtime::block_on` setup, which allocates before polling the dispatch future.
fn register_write_hook(
    pipeline: &mut Pipeline,
    table_id: u64,
    dispatch: impl FnOnce() -> Result<()> + Send + Sync + 'static,
) {
    pipeline.set_on_finished(move |info: &ExecutionInfo| {
        if info.res.is_err() {
            return Ok(());
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "defer panic diagnostics until outside the memory limit"
        )]
        match std::panic::catch_unwind(AssertUnwindSafe(dispatch)) {
            Ok(Ok(())) => (),
            Ok(Err(error)) => {
                let _guard = LimitMemGuard::enter_unlimited();
                warn!(table_id, error :? = error; "Post-write table hook failed");
            }
            Err(panic) => {
                let _guard = LimitMemGuard::enter_unlimited();
                take_alloc_error_panic();
                record_rejected("dispatch_panic");
                let message = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                warn!(table_id, error = message; "Post-write table hook panicked");
            }
        }
        Ok(())
    });
}

async fn enqueue_table_hook(
    ctx: &Arc<QueryContext>,
    target: TableHookTarget,
    recluster: Option<ReclusterWriteHint>,
    refresh: Option<RefreshAnchor>,
) {
    let table_id = target.table_id;
    // Capture allocates identity/settings and independent notifications before enqueue's
    // own boundary. It must not turn an already committed write into a client error.
    let result = CatchUnwindFuture::create(async {
        match MaintenanceTask::from_write(ctx, target, recluster, refresh).await {
            Ok(tasks) => match TableMaintenanceService::try_instance() {
                Some(service) => {
                    for task in tasks {
                        service.enqueue(task);
                    }
                }
                None => {
                    let _guard = LimitMemGuard::enter_unlimited();
                    warn!("Table maintenance service is not initialized, table_id={table_id}");
                }
            },
            Err(e) => {
                let _guard = LimitMemGuard::enter_unlimited();
                warn!("Failed to capture table maintenance request, table_id={table_id}: {e}");
            }
        }
    })
    .await;
    if let Err(error) = result {
        let _guard = LimitMemGuard::enter_unlimited();
        record_rejected("capture_panic");
        warn!(table_id, error :? = error; "Discard best-effort maintenance notification after capture panic");
    }
}

/// Called only after the committed transaction has been cleared. Synchronous maintenance
/// runs before dispatching background tasks, which capture identity/settings, not the
/// caller's transaction or query context.
pub(crate) async fn execute_deferred_table_hooks(
    ctx: Arc<QueryContext>,
    hooks: Vec<(TxnTableHook, Option<RefreshAnchor>)>,
    txn_id: &str,
) {
    for (hook, refresh) in hooks {
        let table_id = hook.table_id;
        // Isolate each table after the user commit, including context preparation,
        // synchronous Compact and notification capture. Never catch the commit itself here.
        let result = CatchUnwindFuture::create(async {
            log::info!("Dispatch post-commit table hook, txn_id={txn_id}, table_id={table_id}");
            let maintenance = match MaintenanceContext::create(&ctx).await {
                Ok(maintenance) => maintenance,
                Err(e) => {
                    let _guard = LimitMemGuard::enter_unlimited();
                    warn!(
                        "Failed to initialize post-commit table hook, txn_id={txn_id}, table_id={table_id}: {e}"
                    );
                    return;
                }
            };
            if let Err(error) = maintenance.tracking.tracking(dispatch_deferred_table_hook(maintenance.ctx, hook, refresh)).await {
                let _guard = LimitMemGuard::enter_unlimited();
                warn!(txn_id, table_id, error :? = error; "Post-commit maintenance preparation failed");
            }
        })
        .await;
        if let Err(error) = result {
            let _guard = LimitMemGuard::enter_unlimited();
            record_rejected("post_commit_panic");
            warn!(txn_id, table_id, error :? = error; "Post-commit table maintenance panicked");
        }
    }
}

async fn dispatch_deferred_table_hook(
    ctx: Arc<QueryContext>,
    hook: TxnTableHook,
    refresh: Option<RefreshAnchor>,
) -> Result<()> {
    let recorded = TableHookTarget {
        catalog: hook.catalog,
        database: hook.database,
        table: hook.table,
        table_id: hook.table_id,
    };
    // Resolve by ID again: a rename may change the route; a replacement is not our target.
    let Some(target) = recorded.resolve(&ctx, "post-commit").await? else {
        return Ok(());
    };
    let table = ctx
        .get_table(&target.catalog, &target.database, &target.table)
        .await?;
    if !target.matches_id("post-commit", table.get_id()) {
        return Ok(());
    }
    let route = HookRoute::for_table(&target, table.as_ref());
    if hook.scope == TableHookScope::Full {
        ctx.set_compaction_num_block_hint(&target.table, hook.compaction_num_block_hint);
        ctx.set_enable_auto_analyze(hook.enable_auto_analyze);
        route
            .run_inline(TableHookTask {
                ctx: &ctx,
                target: &target,
                compact_after_write: hook.compaction_num_block_hint > 0,
                lock_opt: LockTableOption::LockWithRetry,
                lock_guard: None,
                operation_name: "transaction".to_owned(),
                main_operation_start: Instant::now(),
            })
            .await;
    }
    route
        .enqueue(&ctx, target, hook.recluster_write, refresh)
        .await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use databend_common_pipeline::core::LockGuard;
    use databend_common_pipeline::core::UnlockApi;
    use parking_lot::Mutex;

    use super::*;

    #[derive(Default)]
    struct MockUnlock {
        unlocked: AtomicBool,
    }

    impl MockUnlock {
        fn unlocked(&self) -> bool {
            self.unlocked.load(Ordering::SeqCst)
        }
    }

    impl UnlockApi for MockUnlock {
        fn unlock(&self, _revision: u64) {
            self.unlocked.store(true, Ordering::SeqCst);
        }
    }

    struct LockedPipeline {
        pipeline: Pipeline,
        guard: SharedLockGuard,
        unlock: Arc<MockUnlock>,
        /// Lock state seen by each observing callback, in execution order. Callback panics
        /// are caught by the finished chain, so the states are asserted after it ran.
        observed: Arc<Mutex<Vec<bool>>>,
    }

    impl LockedPipeline {
        fn create() -> Self {
            let unlock = Arc::new(MockUnlock::default());
            let guard = SharedLockGuard::new(Arc::new(LockGuard::new(unlock.clone(), 1)));
            LockedPipeline {
                pipeline: Pipeline::create(),
                guard,
                unlock,
                observed: Arc::new(Mutex::new(vec![])),
            }
        }

        fn observe_lock(&mut self) {
            let unlock = self.unlock.clone();
            let observed = self.observed.clone();
            self.pipeline.set_on_finished(move |_info: &ExecutionInfo| {
                observed.lock().push(unlock.unlocked());
                Ok(())
            });
        }

        fn finish(&mut self) -> Result<()> {
            self.pipeline
                .take_on_finished()
                .apply(ExecutionInfo::create(Ok(()), HashMap::new()))
        }
    }

    #[test]
    fn test_lock_released_between_callbacks_in_order() -> Result<()> {
        let mut locked = LockedPipeline::create();
        // Synchronous write callbacks registered before release run under the lock.
        locked.observe_lock();
        register_lock_release(&mut locked.pipeline, &locked.guard);
        // Callbacks registered after release run without the lock.
        locked.observe_lock();

        locked.finish()?;
        assert_eq!(*locked.observed.lock(), vec![false, true]);
        assert!(locked.unlock.unlocked());
        assert!(locked.guard.try_take().is_none());
        Ok(())
    }

    #[test]
    fn test_lock_released_after_earlier_callback_fails() {
        let mut locked = LockedPipeline::create();
        locked
            .pipeline
            .set_on_finished(|_info: &ExecutionInfo| Err(ErrorCode::Internal("hook failed")));
        register_lock_release(&mut locked.pipeline, &locked.guard);
        locked.observe_lock();

        assert!(locked.finish().is_err());
        // The failure interrupts the normal chain, so neither the normal release nor the
        // later callback runs; the always callback still releases the lock.
        assert!(locked.observed.lock().is_empty());
        assert!(locked.unlock.unlocked());
        assert!(locked.guard.try_take().is_none());
    }
}
