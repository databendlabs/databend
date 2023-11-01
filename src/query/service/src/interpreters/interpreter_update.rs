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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::lock::Lock;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::ROW_ID_COL_NAME;
use common_license::license::Feature::ComputedColumn;
use common_license::license_manager::get_license_manager;
use common_sql::binder::ColumnBindingBuilder;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::Visibility;
use log::debug;
use storages_common_locks::LockManager;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::hook_refresh_agg_index;
use crate::interpreters::common::RefreshAggIndexDesc;
use crate::interpreters::interpreter_delete::replace_subquery;
use crate::interpreters::interpreter_delete::subquery_filter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::UpdatePlan;

/// interprets UpdatePlan
pub struct UpdateInterpreter {
    ctx: Arc<QueryContext>,
    plan: UpdatePlan,
}

impl UpdateInterpreter {
    /// Create the UpdateInterpreter from UpdatePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: UpdatePlan) -> Result<Self> {
        Ok(UpdateInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UpdateInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "UpdateInterpreter"
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "update_interpreter_execute");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        // refresh table.
        let tbl = catalog
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await?;

        // check mutability
        tbl.check_mutable()?;

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;

        let selection = if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.support_row_id_column();
            if !support_row_id {
                return Err(ErrorCode::from_string(
                    "table doesn't support row_id, so it can't use delete with subquery"
                        .to_string(),
                ));
            }
            let table_index = self
                .plan
                .metadata
                .read()
                .get_table_index(Some(self.plan.database.as_str()), self.plan.table.as_str());
            let row_id_column_binding = ColumnBindingBuilder::new(
                ROW_ID_COL_NAME.to_string(),
                self.plan.subquery_desc[0].index,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::InVisible,
            )
            .database_name(Some(self.plan.database.clone()))
            .table_name(Some(self.plan.table.clone()))
            .table_index(table_index)
            .build();
            let mut filters = VecDeque::new();
            for subquery_desc in &self.plan.subquery_desc {
                let filter = subquery_filter(
                    self.ctx.clone(),
                    self.plan.metadata.clone(),
                    &row_id_column_binding,
                    subquery_desc,
                )
                .await?;
                filters.push_front(filter);
            }
            // Traverse `selection` and put `filters` into `selection`.
            let mut selection = self.plan.selection.clone().unwrap();
            replace_subquery(&mut filters, &mut selection)?;
            Some(selection)
        } else {
            self.plan.selection.clone()
        };

        let (filter, col_indices) = if let Some(scalar) = selection {
            let filter = cast_expr_to_non_null_boolean(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone()),
            )?
            .as_remote_expr();
            let col_indices: Vec<usize> = if !self.plan.subquery_desc.is_empty() {
                let mut col_indices = HashSet::new();
                for subquery_desc in &self.plan.subquery_desc {
                    col_indices.extend(subquery_desc.outer_columns.iter());
                }
                col_indices.into_iter().collect()
            } else {
                scalar.used_columns().into_iter().collect()
            };
            (Some(filter), col_indices)
        } else {
            (None, vec![])
        };

        let update_list = self.plan.generate_update_list(
            self.ctx.clone(),
            tbl.schema().into(),
            col_indices.clone(),
            None,
            false,
        )?;

        let computed_list = self
            .plan
            .generate_stored_computed_list(self.ctx.clone(), Arc::new(tbl.schema().into()))?;

        if !computed_list.is_empty() {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let mut build_res = PipelineBuildResult::create();
        tbl.update(
            self.ctx.clone(),
            filter,
            col_indices,
            update_list,
            computed_list,
            !self.plan.subquery_desc.is_empty(),
            &mut build_res.main_pipeline,
        )
        .await?;

        // generate sync aggregating indexes if `enable_refresh_aggregating_index_after_write` on.
        {
            let refresh_agg_index_desc = RefreshAggIndexDesc {
                catalog: catalog_name.to_string(),
                database: db_name.to_string(),
                table: tbl_name.to_string(),
            };

            hook_refresh_agg_index(
                self.ctx.clone(),
                &mut build_res.main_pipeline,
                refresh_agg_index_desc,
            )
            .await?;
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        Ok(build_res)
    }
}
