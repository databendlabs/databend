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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SharedLockGuard;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_session::TxnTableHook;
use log::warn;

use crate::interpreters::hook::compact_hook::CompactTargetTableDescription;
use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::table_hook_scheduler::TableHookScheduler;
use crate::interpreters::hook::table_hook_scheduler::TableHookTask;
use crate::interpreters::hook::table_hook_scheduler::TableHookTaskSettings;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextQueryInfo;
use crate::sessions::TableContextSession;
use crate::sessions::TableContextTableAccess;

/// Register the release point of a handed-over table lock on the finished-callback chain.
///
/// The release is a normal callback so it runs in chain order: callbacks registered before it
/// still run under the lock, callbacks registered after it run without the lock. An always
/// callback is added as a safety net in case an earlier callback failed and interrupted the
/// normal chain. Both are no-ops once the guard has been taken.
pub(crate) fn register_lock_release(pipeline: &mut Pipeline, lock_guard: &SharedLockGuard) {
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
    /// The table lock acquired by the main operation, if any.
    ///
    /// The main pipeline and the compact/refresh hooks run under this lock. It is released
    /// before the analyze hook, which only reads snapshots and commits statistics through a
    /// sequence CAS, so it must not extend the lock hold time.
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
            lock_guard: None,
        }
    }

    /// Hand the main operation's table lock over to the hook chain.
    ///
    /// The caller must not also register the guard on the pipeline; the hook chain owns its
    /// release point. Callers that hand over a lock should pass `LockTableOption::NoLock` as
    /// `lock_opt`, otherwise the compact hook would queue a second lock revision behind the
    /// one it already holds.
    pub fn with_lock_guard(mut self, lock_guard: Option<SharedLockGuard>) -> Self {
        self.lock_guard = lock_guard;
        self
    }

    /// Execute the hook operator.
    /// The hook operator will:
    /// 1. Compact/recluster if needed.
    /// 2. Refresh indexes if needed.
    /// 3. Analyze if needed.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute(&self, pipeline: &mut Pipeline) {
        if pipeline.is_empty() {
            return;
        }
        if self.ctx.txn_mgr().lock().is_active() {
            // Preserve the statement's lock lifetime, but never execute or enqueue automatic
            // maintenance until the user transaction has actually committed.
            self.release_lock_guard(pipeline);
            self.defer_until_commit(pipeline).await;
            return;
        }
        if TableHookScheduler::is_async_enabled() {
            self.execute_async(pipeline).await;
            return;
        }

        if let Some(mut task) = self.create_task(self.lock_opt.clone()).await {
            task.lock_guard = self.lock_guard.clone();
            pipeline.set_on_finished(move |info: &ExecutionInfo| {
                if info.res.is_ok() {
                    GlobalIORuntime::instance()
                        .block_on(async move {
                            task.execute().await;
                            Ok::<_, databend_common_exception::ErrorCode>(())
                        })
                        .unwrap_or_else(|e| warn!("Table hook execution failed: {e}"));
                }
                Ok(())
            });
        }
    }

    async fn defer_until_commit(&self, pipeline: &mut Pipeline) {
        let table_id = match self
            .ctx
            .get_table(&self.catalog, &self.database, &self.table)
            .await
        {
            Ok(table) => table.get_id(),
            Err(e) => {
                warn!("Failed to resolve deferred table hook target: {e}");
                return;
            }
        };
        let before = self
            .ctx
            .txn_mgr()
            .lock()
            .buffered_snapshot_location(table_id);
        let ctx = self.ctx.clone();
        let catalog = self.catalog.clone();
        let database = self.database.clone();
        let table = self.table.clone();
        let compact_after_write = compact_after_write_enabled(&ctx);
        pipeline.set_on_finished(move |info: &ExecutionInfo| {
            if info.res.is_ok() {
                let hook = TxnTableHook {
                    catalog,
                    database,
                    compaction_num_block_hint: if compact_after_write {
                        ctx.get_compaction_num_block_hint(&table)
                    } else {
                        0
                    },
                    table,
                    table_id,
                    written_segments: ctx.written_segment_locations().list().into_iter().collect(),
                    enable_auto_analyze: ctx.get_enable_auto_analyze(),
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

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute_async(&self, pipeline: &mut Pipeline) {
        // Async hooks acquire their own lock with retry, so the main operation's lock is
        // released as soon as the main pipeline finishes.
        self.release_lock_guard(pipeline);

        // Keep the existing autocommit async context; only its execution chain is shared.
        if let Some(task) = self.create_task(LockTableOption::LockWithRetry).await {
            pipeline.set_on_finished(move |info: &ExecutionInfo| {
                if info.res.is_ok() {
                    enqueue_table_hook(task);
                }
                Ok(())
            });
        }
    }

    async fn create_task(&self, lock_opt: LockTableOption) -> Option<TableHookTask> {
        let table_id = match self
            .ctx
            .get_table(&self.catalog, &self.database, &self.table)
            .await
        {
            Ok(table) => table.get_id(),
            Err(e) => {
                warn!(
                    "Failed to resolve table hook target {}.{}.{}: {e}",
                    self.catalog, self.database, self.table
                );
                return None;
            }
        };
        Some(TableHookTask {
            ctx: self.ctx.clone(),
            table_id,
            compact_target: CompactTargetTableDescription {
                catalog: self.catalog.clone(),
                database: self.database.clone(),
                table: self.table.clone(),
                table_id: Some(table_id),
            },
            hook_settings: TableHookTaskSettings::create(&self.ctx),
            lock_opt,
            lock_guard: None,
            operation_name: self.mutation_kind.to_string(),
            main_operation_start: Instant::now(),
        })
    }
}

fn enqueue_table_hook(task: TableHookTask) {
    match TableHookScheduler::try_instance() {
        Some(scheduler) => scheduler.enqueue(task),
        None => warn!(
            "Async table hook scheduler is not initialized, table_id={}",
            task.table_id
        ),
    }
}

/// Called only after the committed transaction has been cleared. Each task owns a separate
/// session, so a later BEGIN on the caller's session cannot capture background maintenance.
pub(crate) async fn execute_deferred_table_hooks(
    ctx: Arc<QueryContext>,
    hooks: Vec<TxnTableHook>,
    txn_id: &str,
) {
    let is_async = TableHookScheduler::is_async_enabled();
    for hook in hooks {
        let table_id = hook.table_id;
        log::info!("Dispatch post-commit table hook, txn_id={txn_id}, table_id={table_id}");
        let task = match create_deferred_table_hook_task(&ctx, hook).await {
            Ok(task) => task,
            Err(e) => {
                warn!(
                    "Failed to initialize post-commit table hook, txn_id={txn_id}, table_id={table_id}: {e}"
                );
                continue;
            }
        };
        if is_async {
            enqueue_table_hook(task);
        } else {
            task.execute().await;
        }
    }
}

async fn create_deferred_table_hook_task(
    parent: &Arc<QueryContext>,
    hook: TxnTableHook,
) -> Result<TableHookTask> {
    let session = parent
        .get_current_session()
        .create_internal_session()
        .await?;
    let ctx = session.create_query_context_with_cluster(
        parent.get_cluster(),
        parent.get_version(),
        None,
    )?;
    ctx.set_compaction_num_block_hint(&hook.table, hook.compaction_num_block_hint);
    ctx.written_segment_locations()
        .extend(hook.written_segments);
    ctx.set_enable_auto_analyze(hook.enable_auto_analyze);
    Ok(TableHookTask {
        ctx,
        table_id: hook.table_id,
        compact_target: CompactTargetTableDescription {
            catalog: hook.catalog,
            database: hook.database,
            table: hook.table,
            table_id: Some(hook.table_id),
        },
        hook_settings: TableHookTaskSettings {
            compact_after_write: hook.compaction_num_block_hint > 0,
        },
        // The write transaction and any enclosing lifecycle lock have ended. Maintenance
        // acquires its own locks, regardless of whether the caller waits for it.
        lock_opt: LockTableOption::LockWithRetry,
        lock_guard: None,
        operation_name: "transaction".to_string(),
        main_operation_start: Instant::now(),
    })
}
