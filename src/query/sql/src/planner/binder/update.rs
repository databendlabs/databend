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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UpdateStmt;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::ROW_VERSION_COL_NAME;

use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::normalize_identifier;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::Plan;
use crate::plans::UpdatePlan;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ScalarExpr;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_update(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &UpdateStmt,
    ) -> Result<Plan> {
        let UpdateStmt {
            table,
            update_list,
            selection,
            with,
            ..
        } = stmt;

        self.init_cte(bind_context, with)?;

        let fully_table = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table
        {
            self.fully_table_identifier(catalog, database, table)
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };
        let (catalog_name, database_name, table_name) = (
            fully_table.catalog_name(),
            fully_table.database_name(),
            fully_table.table_name(),
        );

        // Add table lock.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &catalog_name,
                &database_name,
                &table_name,
                &LockTableOption::LockWithRetry,
            )
            .await
            .map_err(|err| fully_table.not_found_suggest_error(err))?;

        let (table_expr, mut context) = self.bind_table_reference(bind_context, table)?;

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        context.allow_internal_columns(false);
        let mut scalar_binder = ScalarBinder::new(
            &mut context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let schema = table.schema();
        let mut update_columns = HashMap::with_capacity(update_list.len());
        for update_expr in update_list {
            let col_name = normalize_identifier(&update_expr.name, &self.name_resolution_ctx).name;
            let index = schema.index_of(&col_name)?;
            if update_columns.contains_key(&index) {
                return Err(ErrorCode::BadArguments(format!(
                    "Multiple assignments in the single statement to column `{}`",
                    col_name
                )));
            }
            let field = schema.field(index);
            if field.computed_expr().is_some() {
                return Err(ErrorCode::BadArguments(format!(
                    "The value specified for computed column '{}' is not allowed",
                    field.name()
                )));
            }

            // TODO(zhyass): update_list support subquery.
            let (scalar, _) = scalar_binder.bind(&update_expr.expr)?;
            if !self.check_allowed_scalar_expr(&scalar)? {
                return Err(ErrorCode::SemanticError(
                    "update_list in update statement can't contain subquery|window|aggregate|udf functions|async functions".to_string(),
                )
                .set_span(scalar.span()));
            }

            update_columns.insert(index, scalar);
        }

        let (selection, subquery_desc) = self
            .process_selection(selection, table_expr, &mut scalar_binder)
            .await?;

        if let Some(selection) = &selection {
            if !self.check_allowed_scalar_expr_with_subquery(selection)? {
                return Err(ErrorCode::SemanticError(
                    "selection in update statement can't contain window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(selection.span()));
            }
        }

        if table.change_tracking_enabled() {
            let (index, row_version) = Self::update_row_version(
                table.schema_with_stream(),
                &context.columns,
                Some(&database_name),
                Some(&table_name),
            )?;
            update_columns.insert(index, row_version);
        }

        let plan = UpdatePlan {
            catalog: catalog_name,
            database: database_name,
            table: table_name,
            update_list: update_columns,
            selection,
            bind_context: Box::new(context),
            metadata: self.metadata.clone(),
            subquery_desc,
            lock_guard,
        };
        Ok(Plan::Update(Box::new(plan)))
    }

    pub fn update_row_version(
        schema: Arc<TableSchema>,
        columns: &[ColumnBinding],
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<(FieldIndex, ScalarExpr)> {
        let col_name = ROW_VERSION_COL_NAME;
        let index = schema.index_of(col_name)?;
        let mut row_version = None;
        for column_binding in columns.iter() {
            if BindContext::match_column_binding(database, table, col_name, column_binding) {
                row_version = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding.clone(),
                }));
                break;
            }
        }
        let col = row_version.ok_or_else(|| ErrorCode::Internal("row_version It's a bug"))?;
        let scalar = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![
                col,
                ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt64(1)),
                }
                .into(),
            ],
        });
        Ok((index, scalar))
    }
}
