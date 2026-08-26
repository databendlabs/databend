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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::IndexType;

use crate::operations::read::runtime_filter_wait::wait_runtime_filters_for_pruning;
use crate::pruning::RuntimeStatsPruner;

#[derive(Clone)]
pub struct RuntimeFilterPruneContext {
    ctx: Arc<dyn TableContext>,
    scan_id: IndexType,
    func_ctx: FunctionContext,
    table_schema: TableSchemaRef,
    runtime_filter_ready: Vec<Arc<RuntimeFilterReady>>,
    statistics_column_ids: Vec<ColumnId>,
    runtime_filter_wait_finished: Arc<AtomicBool>,
    runtime_stats_pruner: Arc<OnceLock<Option<Arc<RuntimeStatsPruner>>>>,
}

impl RuntimeFilterPruneContext {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        scan_id: IndexType,
        table_schema: TableSchemaRef,
    ) -> Result<Option<Self>> {
        let runtime_filter_ready = Self::statistics_ready(&ctx.get_runtime_filter_ready(scan_id));
        if runtime_filter_ready.is_empty() {
            return Ok(None);
        }
        let statistics_column_ids =
            Self::statistics_column_ids_from_ready(&table_schema, &runtime_filter_ready)?;

        Ok(Some(Self {
            func_ctx: ctx.get_function_context()?,
            ctx,
            scan_id,
            table_schema,
            runtime_filter_ready,
            statistics_column_ids,
            runtime_filter_wait_finished: Arc::new(AtomicBool::new(false)),
            runtime_stats_pruner: Arc::new(OnceLock::new()),
        }))
    }

    pub fn statistics_column_ids(&self) -> &[ColumnId] {
        &self.statistics_column_ids
    }

    pub async fn runtime_stats_pruner(&self) -> Result<Option<Arc<RuntimeStatsPruner>>> {
        if let Some(pruner) = self.runtime_stats_pruner.get() {
            return Ok(pruner.clone());
        }

        if !self.runtime_filter_wait_finished.load(Ordering::Acquire)
            && !Self::all_runtime_filters_ready(&self.runtime_filter_ready)
        {
            wait_runtime_filters_for_pruning(
                self.scan_id,
                self.ctx.get_abort_notify(),
                &self.runtime_filter_ready,
            )
            .await?;
            self.runtime_filter_wait_finished
                .store(true, Ordering::Release);
        }

        let runtime_filters = self.ctx.get_runtime_filters(self.scan_id);
        let pruner = RuntimeStatsPruner::try_create(
            self.func_ctx.clone(),
            self.table_schema.clone(),
            &runtime_filters,
        );

        Ok(Self::cache_runtime_stats_pruner(
            &self.runtime_stats_pruner,
            pruner,
            Self::all_runtime_filters_ready(&self.runtime_filter_ready),
        ))
    }

    fn statistics_ready(
        runtime_filter_ready: &[Arc<RuntimeFilterReady>],
    ) -> Vec<Arc<RuntimeFilterReady>> {
        runtime_filter_ready
            .iter()
            .filter(|ready| ready.has_statistics_pruning())
            .cloned()
            .collect()
    }

    fn statistics_column_ids_from_ready(
        table_schema: &TableSchemaRef,
        runtime_filter_ready: &[Arc<RuntimeFilterReady>],
    ) -> Result<Vec<ColumnId>> {
        let mut column_ids = BTreeSet::new();
        for ready in runtime_filter_ready {
            for column_name in ready.statistics_column_names() {
                column_ids.extend(table_schema.leaf_columns_of(column_name));
            }
        }

        Ok(column_ids.into_iter().collect())
    }

    fn all_runtime_filters_ready(runtime_filter_ready: &[Arc<RuntimeFilterReady>]) -> bool {
        runtime_filter_ready
            .iter()
            .all(|ready| ready.runtime_filter_watcher.subscribe().borrow().is_some())
    }

    fn cache_runtime_stats_pruner(
        cache: &OnceLock<Option<Arc<RuntimeStatsPruner>>>,
        pruner: Option<Arc<RuntimeStatsPruner>>,
        runtime_filter_ready: bool,
    ) -> Option<Arc<RuntimeStatsPruner>> {
        if runtime_filter_ready {
            let _ = cache.set(pruner.clone());
            return cache.get().cloned().unwrap_or(pruner);
        }

        pruner
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
    fn statistics_ready_ignores_filters_without_statistics_metadata() {
        let ready = vec![Arc::new(RuntimeFilterReady::default())];

        assert!(RuntimeFilterPruneContext::statistics_ready(&ready).is_empty());
    }

    #[test]
    fn statistics_column_ids_only_include_runtime_filter_probe_columns() -> Result<()> {
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
            Arc::new(RuntimeFilterReady::with_statistics_column_names([
                "runtime_col".to_string(),
            ])),
        ];

        assert_eq!(
            RuntimeFilterPruneContext::statistics_column_ids_from_ready(&table_schema, &ready)?,
            vec![table_schema.column_id_of("runtime_col")?]
        );

        Ok(())
    }

    #[test]
    fn statistics_column_ids_ignore_probe_names_not_in_table_schema() -> Result<()> {
        let table_schema = Arc::new(TableSchema::new(vec![TableField::new(
            "number",
            TableDataType::Number(NumberDataType::UInt64),
        )]));
        let ready = vec![Arc::new(RuntimeFilterReady::with_statistics_column_names(
            ["subquery_2".to_string(), "number".to_string()],
        ))];

        assert_eq!(
            RuntimeFilterPruneContext::statistics_column_ids_from_ready(&table_schema, &ready)?,
            vec![table_schema.column_id_of("number")?]
        );

        Ok(())
    }

    #[test]
    fn runtime_stats_pruner_does_not_cache_none_before_filters_ready() {
        let cache = OnceLock::new();

        assert!(
            RuntimeFilterPruneContext::cache_runtime_stats_pruner(&cache, None, false).is_none()
        );
        assert!(cache.get().is_none());
    }

    #[test]
    fn runtime_stats_pruner_does_not_cache_partial_pruner_before_filters_ready() {
        let cache = OnceLock::new();
        let schema = Arc::new(TableSchema::new(vec![]));
        let pruner = Some(Arc::new(RuntimeStatsPruner::new(
            FunctionContext::default(),
            schema,
            vec![],
        )));

        assert!(
            RuntimeFilterPruneContext::cache_runtime_stats_pruner(&cache, pruner, false).is_some()
        );
        assert!(cache.get().is_none());
    }

    #[test]
    fn runtime_stats_pruner_caches_none_after_filters_ready() {
        let cache = OnceLock::new();

        assert!(
            RuntimeFilterPruneContext::cache_runtime_stats_pruner(&cache, None, true).is_none()
        );
        assert!(cache.get().is_some_and(|cached| cached.is_none()));
    }
}
