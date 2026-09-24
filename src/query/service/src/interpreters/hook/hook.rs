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

use databend_common_catalog::lock::LockTableOption;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SharedLockGuard;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::executor::physical_plans::MutationKind;
use log::warn;

use crate::interpreters::hook::analyze_hook::AnalyzeDesc;
use crate::interpreters::hook::analyze_hook::hook_analyze;
use crate::interpreters::hook::compact_hook::CompactHookTraceCtx;
use crate::interpreters::hook::compact_hook::CompactTargetTableDescription;
use crate::interpreters::hook::compact_hook::compact_after_write_enabled;
use crate::interpreters::hook::compact_hook::hook_compact;
use crate::interpreters::hook::refresh_hook::RefreshDesc;
use crate::interpreters::hook::refresh_hook::hook_refresh;
use crate::interpreters::hook::table_hook_scheduler::TableHookScheduler;
use crate::interpreters::hook::table_hook_scheduler::TableHookTask;
use crate::interpreters::hook::table_hook_scheduler::TableHookTaskSettings;
use crate::sessions::QueryContext;
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
    /// 1. Compact if needed.
    /// 2. Refresh aggregating index if needed.
    /// 3. Refresh virtual columns if needed.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute(&self, pipeline: &mut Pipeline) {
        if TableHookScheduler::is_async_enabled() {
            self.execute_async(pipeline).await;
            return;
        }

        self.execute_compact(pipeline).await;
        self.execute_refresh(pipeline).await;
        // Compaction and reclustering mutate the table and rely on the main operation's lock.
        // Analyze only reads snapshots and commits statistics with a sequence CAS, so the lock
        // is released here to keep other maintenance jobs from waiting on it.
        self.release_lock_guard(pipeline);
        self.execute_analyze(pipeline).await;
    }

    fn release_lock_guard(&self, pipeline: &mut Pipeline) {
        if let Some(lock_guard) = &self.lock_guard {
            register_lock_release(pipeline, lock_guard);
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_async(&self, pipeline: &mut Pipeline) {
        // Async hooks acquire their own lock with retry, so the main operation's lock is
        // released as soon as the main pipeline finishes.
        self.release_lock_guard(pipeline);
        if pipeline.is_empty() {
            return;
        }

        let table_id = match self
            .ctx
            .get_table(&self.catalog, &self.database, &self.table)
            .await
        {
            Ok(table) => table.get_id(),
            Err(e) => {
                warn!(
                    "Failed to resolve table id for async table hook {}.{}.{}: {}",
                    self.catalog, self.database, self.table, e
                );
                return;
            }
        };

        let task = TableHookTask {
            ctx: self.ctx.clone(),
            table_id,
            compact_target: CompactTargetTableDescription {
                catalog: self.catalog.clone(),
                database: self.database.clone(),
                table: self.table.clone(),
                table_id: Some(table_id),
            },
            hook_settings: TableHookTaskSettings::create(&self.ctx),
            // The async hook may start from on_finished before the parent lock guard
            // has been released, so it must acquire its own lock with retry.
            lock_opt: LockTableOption::LockWithRetry,
            operation_name: self.mutation_kind.to_string(),
            main_operation_start: Instant::now(),
        };

        pipeline.set_on_finished(move |info: &ExecutionInfo| {
            if info.res.is_ok() {
                match TableHookScheduler::try_instance() {
                    Some(scheduler) => scheduler.enqueue(task),
                    None => warn!("Async table hook scheduler is not initialized"),
                }
            }

            Ok(())
        });
    }

    /// Execute the compact hook operator.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_compact(&self, pipeline: &mut Pipeline) {
        if !compact_after_write_enabled(&self.ctx) {
            return;
        }

        let compact_target = CompactTargetTableDescription {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
            table_id: None,
        };

        let trace_ctx = CompactHookTraceCtx {
            start: Instant::now(),
            operation_name: self.mutation_kind.to_string(),
        };

        hook_compact(
            self.ctx.clone(),
            pipeline,
            compact_target,
            trace_ctx,
            self.lock_opt.clone(),
        )
        .await;
    }

    /// Execute the table-index refresh hook operator.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_refresh(&self, pipeline: &mut Pipeline) {
        let refresh_desc = RefreshDesc {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
            table_id: None,
        };

        hook_refresh(self.ctx.clone(), pipeline, refresh_desc).await;
    }

    /// Execute the analyze hook operator.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_analyze(&self, pipeline: &mut Pipeline) {
        let desc = AnalyzeDesc {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
            table_id: None,
        };

        hook_analyze(self.ctx.clone(), pipeline, desc).await;
    }
}
