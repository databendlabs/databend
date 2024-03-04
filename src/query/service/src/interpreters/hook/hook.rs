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

use std::sync::Arc;
use std::time::Instant;

use databend_common_pipeline_core::Pipeline;

use crate::interpreters::hook::compact_hook::hook_compact;
use crate::interpreters::hook::compact_hook::CompactHookTraceCtx;
use crate::interpreters::hook::compact_hook::CompactTargetTableDescription;
use crate::interpreters::hook::refresh_hook::hook_refresh;
use crate::interpreters::hook::refresh_hook::RefreshDesc;
use crate::sessions::QueryContext;

/// Hook operator.
pub struct HookOperator {
    ctx: Arc<QueryContext>,
    catalog: String,
    database: String,
    table: String,
    operation_name: String,
    need_lock: bool,
}

impl HookOperator {
    pub fn create(
        ctx: Arc<QueryContext>,
        catalog: String,
        database: String,
        table: String,
        operation_name: String,
        need_lock: bool,
    ) -> Self {
        Self {
            ctx,
            catalog,
            database,
            table,
            operation_name,
            need_lock,
        }
    }

    /// Execute the hook operator.
    /// The hook operator will:
    /// 1. Compact if needed.
    /// 2. Refresh aggregating index if needed.
    /// 3. Refresh virtual columns if needed.
    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn execute(&self, pipeline: &mut Pipeline) {
        self.execute_compact(pipeline).await;
        self.execute_refresh(pipeline).await;
    }

    /// Execute the compact hook operator.
    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_compact(&self, pipeline: &mut Pipeline) {
        let compact_target = CompactTargetTableDescription {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
        };

        let trace_ctx = CompactHookTraceCtx {
            start: Instant::now(),
            operation_name: self.operation_name.to_owned(),
        };

        hook_compact(
            self.ctx.clone(),
            pipeline,
            compact_target,
            trace_ctx,
            self.need_lock,
        )
        .await;
    }

    /// Execute the refresh hook operator.
    // 1. Refresh aggregating index.
    // 2. Refresh virtual columns.
    #[minitrace::trace]
    #[async_backtrace::framed]
    pub async fn execute_refresh(&self, pipeline: &mut Pipeline) {
        let refresh_desc = RefreshDesc {
            catalog: self.catalog.to_owned(),
            database: self.database.to_owned(),
            table: self.table.to_owned(),
        };

        let trace_ctx = CompactHookTraceCtx {
            start: Instant::now(),
            operation_name: self.operation_name.to_owned(),
        };

        hook_refresh(self.ctx.clone(), pipeline, refresh_desc, trace_ctx).await;
    }
}
