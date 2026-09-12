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

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogType;
use databend_common_meta_app::schema::DYNAMIC_TABLE_ENGINE;
use databend_common_sql::Planner;
use databend_common_sql::plans::Insert;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::Plan;
use databend_storages_common_table_meta::table::OPT_KEY_AS_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_TABLE_IDS;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::QueryFinishHooks;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct RefreshDynamicTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: databend_common_sql::plans::RefreshDynamicTablePlan,
}

impl RefreshDynamicTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: databend_common_sql::plans::RefreshDynamicTablePlan,
    ) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshDynamicTableInterpreter {
    fn name(&self) -> &str {
        "RefreshDynamicTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let _lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                &LockTableOption::LockWithRetry,
            )
            .await?;
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.table,
        )?;
        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        if table.engine() != DYNAMIC_TABLE_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not a dynamic table",
                self.plan.database, self.plan.table
            )));
        }
        let query = table
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_AS_QUERY)
            .cloned()
            .ok_or_else(|| ErrorCode::InvalidOperation("dynamic table definition is missing"))?;

        // A full overwrite is the only refresh mode. It lands as a single Fuse commit, so readers
        // observe either the previous contents or the new ones, never a mixture.
        //
        // The plan is built structurally rather than by formatting an `INSERT OVERWRITE` string.
        // A backtick is legal inside a quoted identifier (it is escaped by doubling), so
        // interpolating the catalog/database/table names into SQL and re-parsing would let a
        // crafted name break out of its quoting -- in a write path. Only the stored definition,
        // which the system itself serialized, is parsed here.
        let mut planner = Planner::new(self.ctx.clone());
        let (select_plan, _) = planner.plan_sql(&query).await?;
        let Plan::Query { metadata, .. } = &select_plan else {
            return Err(ErrorCode::InvalidOperation(
                "dynamic table definition must be a query",
            ));
        };
        let current_source_table_ids = metadata
            .read()
            .tables()
            .iter()
            .map(|entry| entry.table().get_id())
            .collect::<BTreeSet<_>>();
        let expected_source_table_ids = table
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_SOURCE_TABLE_IDS)
            .ok_or_else(|| {
                ErrorCode::InvalidOperation("dynamic table source table IDs are missing")
            })?
            .split(',')
            .map(|id| {
                id.parse::<u64>().map_err(|error| {
                    ErrorCode::InvalidOperation(format!(
                        "invalid dynamic table source table ID '{id}': {error}"
                    ))
                })
            })
            .collect::<Result<BTreeSet<_>>>()?;
        if expected_source_table_ids != current_source_table_ids {
            return Err(ErrorCode::InvalidOperation(format!(
                "dynamic table source tables changed: expected {:?}, got {:?}",
                expected_source_table_ids, current_source_table_ids
            )));
        }

        let insert = Insert {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
            branch: None,
            schema: table.schema(),
            overwrite: true,
            source: InsertInputSource::SelectPlan(Box::new(select_plan)),
            table_info: Some(table.get_table_info().clone()),
            lineage_target_table_id: None,
            lineage_target_catalog_type: CatalogType::Default,
        };
        let interpreter =
            InsertInterpreter::try_create_refresh(self.ctx.clone(), insert, table.get_id())?;
        let stream = interpreter
            .execute_with_hooks(self.ctx.clone(), QueryFinishHooks::nested_with_hooks())
            .await?;
        futures::pin_mut!(stream);
        use futures::TryStreamExt;
        while stream.try_next().await?.is_some() {}

        Ok(PipelineBuildResult::create())
    }
}
