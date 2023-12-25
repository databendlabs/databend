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

use std::str::FromStr;
use std::sync::Arc;

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::InsertStmt;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::FileFormatOptionsAst;
use databend_common_meta_app::principal::OnErrorMode;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::plans::CopyIntoTableMode;
use crate::plans::Insert;
use crate::plans::InsertInputSource;
use crate::plans::Plan;
use crate::BindContext;
impl Binder {
    pub fn schema_project(
        &self,
        schema: &Arc<TableSchema>,
        columns: &[Identifier],
    ) -> Result<Arc<TableSchema>> {
        let fields = if columns.is_empty() {
            schema
                .fields()
                .iter()
                .filter(|f| f.computed_expr().is_none())
                .cloned()
                .collect::<Vec<_>>()
        } else {
            columns
                .iter()
                .map(|ident| {
                    let field = schema.field_with_name(
                        &normalize_identifier(ident, &self.name_resolution_ctx).name,
                    )?;
                    if field.computed_expr().is_some() {
                        Err(ErrorCode::BadArguments(format!(
                            "Computed column error: '{}' cannot accept specified values as it's a computed column",
                            field.name()
                        )))

                    } else {
                        Ok(field.clone())
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        Ok(TableSchemaRefExt::create(fields))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_insert(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &InsertStmt,
    ) -> Result<Plan> {
        let InsertStmt {
            catalog,
            database,
            table,
            columns,
            source,
            overwrite,
            ..
        } = stmt;
        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();
        let schema = self.schema_project(&table.schema(), columns)?;

        let input_source: Result<InsertInputSource> = match source.clone() {
            InsertSource::Streaming {
                format,
                rest_str,
                start,
            } => {
                if format.to_uppercase() == "VALUES" {
                    let data = rest_str.trim_end_matches(';').trim_start().to_owned();
                    Ok(InsertInputSource::Values { data, start })
                } else {
                    Ok(InsertInputSource::StreamingWithFormat(format, start, None))
                }
            }
            InsertSource::StreamingV2 {
                settings,
                on_error_mode,
                start,
            } => {
                let params = FileFormatOptionsAst { options: settings }.try_into()?;
                Ok(InsertInputSource::StreamingWithFileFormat {
                    format: params,
                    start,
                    on_error_mode: OnErrorMode::from_str(
                        &on_error_mode.unwrap_or("abort".to_string()),
                    )?,
                    input_context_option: None,
                })
            }
            InsertSource::Values { rest_str, start } => {
                let values_str = rest_str.trim_end_matches(';').trim_start().to_owned();
                match self.ctx.get_stage_attachment() {
                    Some(attachment) => {
                        return self
                            .bind_copy_from_attachment(
                                bind_context,
                                attachment,
                                catalog_name,
                                database_name,
                                table_name,
                                Arc::new(schema.into()),
                                &values_str,
                                CopyIntoTableMode::Insert {
                                    overwrite: *overwrite,
                                },
                            )
                            .await;
                    }
                    None => Ok(InsertInputSource::Values {
                        data: rest_str,
                        start,
                    }),
                }
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                let opt_ctx = OptimizerContext::new(self.ctx.clone(), self.metadata.clone())
                    .with_enable_distributed_optimization(!self.ctx.get_cluster().is_empty());

                if let Plan::Query { s_expr, .. } = &select_plan {
                    if !self.check_sexpr_top(s_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "Insert source error: UDF (User-Defined Functions) are not permitted in insert source queries",
                        ));
                    }
                }

                let optimized_plan = optimize(opt_ctx, select_plan)?;
                Ok(InsertInputSource::SelectPlan(Box::new(optimized_plan)))
            }
        };

        let plan = Insert {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            table_id,
            schema,
            overwrite: *overwrite,
            source: input_source?,
        };

        Ok(Plan::Insert(Box::new(plan)))
    }
}
