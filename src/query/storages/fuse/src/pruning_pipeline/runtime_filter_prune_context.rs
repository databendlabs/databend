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
use std::sync::OnceLock;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::IndexType;

use crate::operations::read::runtime_filter_wait::wait_runtime_filters_for_pruning;
use crate::pruning::RuntimeMinMaxPruner;

#[derive(Clone)]
pub struct RuntimeFilterPruneContext {
    ctx: Arc<dyn TableContext>,
    scan_id: IndexType,
    func_ctx: FunctionContext,
    table_schema: TableSchemaRef,
    runtime_min_max_pruner: Arc<OnceLock<Option<Arc<RuntimeMinMaxPruner>>>>,
}

impl RuntimeFilterPruneContext {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        scan_id: IndexType,
        table_schema: TableSchemaRef,
    ) -> Result<Option<Self>> {
        if ctx.get_runtime_filter_ready(scan_id).is_empty() {
            return Ok(None);
        }

        Ok(Some(Self {
            func_ctx: ctx.get_function_context()?,
            ctx,
            scan_id,
            table_schema,
            runtime_min_max_pruner: Arc::new(OnceLock::new()),
        }))
    }

    pub async fn runtime_min_max_pruner(&self) -> Result<Option<Arc<RuntimeMinMaxPruner>>> {
        if let Some(pruner) = self.runtime_min_max_pruner.get() {
            return Ok(pruner.clone());
        }

        let ready = self.ctx.get_runtime_filter_ready(self.scan_id);
        if !ready.is_empty() {
            wait_runtime_filters_for_pruning(self.scan_id, self.ctx.get_abort_notify(), &ready)
                .await?;
        }

        let runtime_filters = self.ctx.get_runtime_filters(self.scan_id);
        let pruner = RuntimeMinMaxPruner::try_create(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            &runtime_filters,
        );
        let _ = self.runtime_min_max_pruner.set(pruner.clone());

        Ok(self.runtime_min_max_pruner.get().cloned().unwrap_or(pruner))
    }
}
