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

use databend_common_ast::ast::InsertMultiTableStmt;
use databend_common_ast::ast::IntoClause;
use databend_common_ast::ast::SourceExpr;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;

use crate::binder::ScalarBinder;
use crate::plans::Else;
use crate::plans::InsertMultiTable;
use crate::plans::Into;
use crate::plans::Plan;
use crate::plans::When;
use crate::BindContext;
use crate::Binder;
impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_insert_multi_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &InsertMultiTableStmt,
    ) -> Result<Plan> {
        let InsertMultiTableStmt {
            when_clauses,
            else_clause,
            source,
            overwrite,
            is_first,
            into_clauses,
        } = stmt;

        let mut target_tables = HashMap::new();

        let (input_source, bind_context) = {
            let table_ref = TableReference::Subquery {
                subquery: Box::new(source.clone()),
                span: None,
                lateral: false,
                alias: None,
            };

            let (s_expr, bind_context) = self.bind_table_reference(bind_context, &table_ref)?;

            let select_plan = Plan::Query {
                s_expr: Box::new(s_expr),
                metadata: self.metadata.clone(),
                bind_context: Box::new(bind_context.clone()),
                rewrite_kind: None,
                formatted_ast: None,
                ignore_result: false,
            };

            (select_plan, bind_context)
        };

        let source_schema = input_source.schema();
        for field in source_schema.fields.iter() {
            if field.name().to_lowercase() == "default" {
                return Err(ErrorCode::BadArguments(
                    "The column name in source of multi-table insert can't be 'default'"
                        .to_string(),
                ));
            }
        }

        let mut source_bind_context = bind_context.clone();
        let mut whens = vec![];
        for when_clause in when_clauses {
            let mut scalar_binder = ScalarBinder::new(
                &mut source_bind_context,
                self.ctx.clone(),
                self.metadata.clone(),
                &[],
                self.m_cte_bound_ctx.clone(),
                self.ctes_map.clone(),
            );
            let (condition, _) = scalar_binder.bind(&when_clause.condition)?;
            if !matches!(condition.data_type()?.remove_nullable(), DataType::Boolean) {
                return Err(ErrorCode::IllegalDataType(
                    "The condition in WHEN clause must be a boolean expression".to_string(),
                ));
            }
            let intos = self
                .bind_into_clauses(
                    &when_clause.into_clauses,
                    source_schema.clone(),
                    &mut source_bind_context,
                    &mut target_tables,
                )
                .await?;
            whens.push(When { condition, intos });
        }

        let opt_else = match else_clause {
            Some(else_clause) => {
                let intos = self
                    .bind_into_clauses(
                        &else_clause.into_clauses,
                        source_schema.clone(),
                        &mut source_bind_context,
                        &mut target_tables,
                    )
                    .await?;
                Some(Else { intos })
            }
            None => None,
        };

        let intos = self
            .bind_into_clauses(
                into_clauses,
                source_schema.clone(),
                &mut source_bind_context,
                &mut target_tables,
            )
            .await?;

        let mut ordered_target_tables = target_tables.into_iter().collect::<Vec<_>>();
        // convenient for testing
        ordered_target_tables.sort_by(|(_, l), (_, r)| l.0.cmp(&r.0).then_with(|| l.1.cmp(&r.1)));

        let plan = InsertMultiTable {
            input_source,
            whens,
            opt_else,
            overwrite: *overwrite,
            is_first: *is_first,
            intos,
            target_tables: ordered_target_tables,
            meta_data: self.metadata.clone(),
        };
        Ok(Plan::InsertMultiTable(Box::new(plan)))
    }
}

impl Binder {
    async fn bind_into_clauses(
        &mut self,
        into_clauses: &[IntoClause],
        source_schema: DataSchemaRef,
        source_bind_context: &mut BindContext,
        target_tables: &mut HashMap<u64, (String, String)>,
    ) -> Result<Vec<Into>> {
        let mut intos = vec![];
        for into_clause in into_clauses {
            let IntoClause {
                database,
                table,
                target_columns,
                source_columns,
                catalog,
            } = into_clause;
            let (catalog_name, database_name, table_name) =
                self.normalize_object_identifier_triple(catalog, database, table);

            let target_table = self
                .ctx
                .get_table(&catalog_name, &database_name, &table_name)
                .await?;
            target_tables.insert(
                target_table.get_id(),
                (database_name.clone(), table_name.clone()),
            );

            let n_target_col = if target_columns.is_empty() {
                target_table.schema().fields().len()
            } else {
                target_columns.len()
            };
            let n_source_col = if source_columns.is_empty() {
                source_schema.fields().len()
            } else {
                source_columns.len()
            };
            if n_target_col != n_source_col {
                return Err(ErrorCode::BadArguments(
                    "The number of columns in the target and the source must be the same"
                        .to_string(),
                ));
            }

            let mut casted_schema = if target_columns.is_empty() {
                target_table.schema()
            } else {
                self.schema_project(&target_table.schema(), target_columns.as_ref())?
            };

            let default_indices = source_columns
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if matches!(col, SourceExpr::Default) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            if !default_indices.is_empty() {
                let mut casted_schema_fields = vec![];
                for (i, field) in casted_schema.fields().iter().enumerate() {
                    if default_indices.contains(&i) {
                        continue;
                    }
                    casted_schema_fields.push(field.clone());
                }
                casted_schema = Arc::new(TableSchema {
                    fields: casted_schema_fields,
                    metadata: casted_schema.metadata.clone(),
                    next_column_id: casted_schema.next_column_id(),
                });
            }

            let source_scalar_exprs = if source_columns.is_empty() {
                None
            } else {
                let mut scalar_binder = ScalarBinder::new(
                    source_bind_context,
                    self.ctx.clone(),
                    self.metadata.clone(),
                    &[],
                    self.m_cte_bound_ctx.clone(),
                    self.ctes_map.clone(),
                );
                let mut source_scalar_exprs = vec![];
                for source_column in source_columns {
                    match source_column {
                        SourceExpr::Expr(expr) => {
                            let (scalar_expr, _) = scalar_binder.bind(expr)?;
                            source_scalar_exprs.push(scalar_expr);
                        }
                        SourceExpr::Default => {
                            continue;
                        }
                    }
                }
                Some(source_scalar_exprs)
            };

            intos.push(Into {
                catalog: catalog_name,
                database: database_name,
                table: table_name,
                source_scalar_exprs,
                casted_schema: Arc::new(casted_schema.into()),
            });
        }
        Ok(intos)
    }
}
