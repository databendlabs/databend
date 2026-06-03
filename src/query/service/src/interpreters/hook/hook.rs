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

/// Hook operator.
pub struct HookOperator {
    ctx: Arc<QueryContext>,
    catalog: String,
    database: String,
    table: String,
    mutation_kind: MutationKind,
    lock_opt: LockTableOption,
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
        }
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
        self.execute_analyze(pipeline).await;
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_async(&self, pipeline: &mut Pipeline) {
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
            lock_opt: self.lock_opt.clone(),
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

    /// Execute the refresh hook operator.
    // 1. Refresh aggregating index.
    // 2. Refresh virtual columns.
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_refresh(&self, pipeline: &mut Pipeline) {
        let refresh_desc = RefreshDesc {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
            table_id: None,
            enable_refresh_aggregating_index_after_write: None,
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
