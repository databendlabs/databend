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

use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::ReplaceStmt;
use databend_common_ast::ast::Statement;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::CopyIntoTableMode;
use crate::plans::InsertInputSource;
use crate::plans::InsertValue;
use crate::plans::Plan;
use crate::plans::Replace;
use crate::BindContext;
impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_replace(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ReplaceStmt,
    ) -> Result<Plan> {
        let ReplaceStmt {
            catalog,
            database,
            table,
            on_conflict_columns,
            columns,
            source,
            delete_when,
            hints: _,
        } = stmt;

        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table);

        // Add table lock before execution.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &catalog_name,
                &database_name,
                &table_name,
                &LockTableOption::LockWithRetry,
            )
            .await?;

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema = if columns.is_empty() {
            table.schema()
        } else {
            let schema = table.schema();
            let field_indexes = columns
                .iter()
                .map(|ident| {
                    schema.index_of(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(schema.project(&field_indexes))
        };

        let on_conflict_fields = on_conflict_columns
            .iter()
            .map(|ident| {
                schema
                    .field_with_name(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;

        let input_source: Result<InsertInputSource> = match source.clone() {
            InsertSource::Values { rows } => {
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let new_row = bind_context
                        .exprs_to_scalar(
                            &row,
                            &Arc::new(schema.clone().into()),
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                        )
                        .await?;
                    new_rows.push(new_row);
                }
                Ok(InsertInputSource::Values(InsertValue::Values {
                    rows: new_rows,
                }))
            }
            InsertSource::RawValues { rest_str, start } => {
                let values_str = rest_str.trim_end_matches(';').trim_start().to_owned();
                match self.ctx.get_stage_attachment() {
                    Some(attachment) => {
                        let plan = self
                            .bind_copy_from_attachment(
                                bind_context,
                                attachment,
                                catalog_name.clone(),
                                database_name.clone(),
                                table_name.clone(),
                                Arc::new(schema.clone().into()),
                                &values_str,
                                CopyIntoTableMode::Replace,
                            )
                            .await?;
                        Ok(InsertInputSource::Stage(Box::new(plan)))
                    }
                    None => Ok(InsertInputSource::Values(InsertValue::RawValues {
                        data: values_str,
                        start,
                    })),
                }
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                Ok(InsertInputSource::SelectPlan(Box::new(select_plan)))
            }
            InsertSource::StreamingLoad { .. } => Err(ErrorCode::Unimplemented(
                "Replace with streaming load not supported yet.",
            )),
        };

        let plan = Replace {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            table_id,
            on_conflict_fields,
            schema,
            source: input_source?,
            delete_when: delete_when.clone(),
            lock_guard,
        };

        Ok(Plan::Replace(Box::new(plan)))
    }
}
