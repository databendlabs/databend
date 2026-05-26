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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
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
    runtime_filter_ready: Vec<Arc<RuntimeFilterReady>>,
    min_max_column_ids: Vec<ColumnId>,
    runtime_min_max_pruner: Arc<OnceLock<Option<Arc<RuntimeMinMaxPruner>>>>,
}

impl RuntimeFilterPruneContext {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        scan_id: IndexType,
        table_schema: TableSchemaRef,
    ) -> Result<Option<Self>> {
        let runtime_filter_ready = Self::min_max_ready(&ctx.get_runtime_filter_ready(scan_id));
        if runtime_filter_ready.is_empty() {
            return Ok(None);
        }
        let min_max_column_ids =
            Self::min_max_column_ids_from_ready(&table_schema, &runtime_filter_ready)?;

        Ok(Some(Self {
            func_ctx: ctx.get_function_context()?,
            ctx,
            scan_id,
            table_schema,
            runtime_filter_ready,
            min_max_column_ids,
            runtime_min_max_pruner: Arc::new(OnceLock::new()),
        }))
    }

    pub fn min_max_column_ids(&self) -> &[ColumnId] {
        &self.min_max_column_ids
    }

    pub async fn runtime_min_max_pruner(&self) -> Result<Option<Arc<RuntimeMinMaxPruner>>> {
        if let Some(pruner) = self.runtime_min_max_pruner.get() {
            return Ok(pruner.clone());
        }

        wait_runtime_filters_for_pruning(
            self.scan_id,
            self.ctx.get_abort_notify(),
            &self.runtime_filter_ready,
        )
        .await?;

        let runtime_filters = self.ctx.get_runtime_filters(self.scan_id);
        let pruner = RuntimeMinMaxPruner::try_create(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            &runtime_filters,
        );
        let _ = self.runtime_min_max_pruner.set(pruner.clone());

        Ok(self.runtime_min_max_pruner.get().cloned().unwrap_or(pruner))
    }

    fn min_max_ready(
        runtime_filter_ready: &[Arc<RuntimeFilterReady>],
    ) -> Vec<Arc<RuntimeFilterReady>> {
        runtime_filter_ready
            .iter()
            .filter(|ready| ready.has_min_max())
            .cloned()
            .collect()
    }

    fn min_max_column_ids_from_ready(
        table_schema: &TableSchemaRef,
        runtime_filter_ready: &[Arc<RuntimeFilterReady>],
    ) -> Result<Vec<ColumnId>> {
        let mut column_ids = BTreeSet::new();
        for ready in runtime_filter_ready {
            for column_name in ready.min_max_column_names() {
                let field = table_schema.field_with_name(column_name)?;
                column_ids.extend(field.leaf_column_ids());
            }
        }

        Ok(column_ids.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    #[test]
    fn min_max_ready_ignores_filters_without_min_max_metadata() {
        let ready = vec![Arc::new(RuntimeFilterReady::default())];

        assert!(RuntimeFilterPruneContext::min_max_ready(&ready).is_empty());
    }

    #[test]
    fn min_max_column_ids_only_include_runtime_min_max_probe_columns() -> Result<()> {
        let table_schema = Arc::new(TableSchema::new(vec![
            TableField::new(
                "projected_col",
                TableDataType::Number(NumberDataType::Int32),
            ),
            TableField::new("runtime_col", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("unused_col", TableDataType::Number(NumberDataType::Int32)),
        ]));
        let ready = vec![
            Arc::new(RuntimeFilterReady::default()),
            Arc::new(RuntimeFilterReady::with_min_max_column_names([
                "runtime_col".to_string(),
            ])),
        ];

        assert_eq!(
            RuntimeFilterPruneContext::min_max_column_ids_from_ready(&table_schema, &ready)?,
            vec![table_schema.column_id_of("runtime_col")?]
        );

        Ok(())
    }
}
