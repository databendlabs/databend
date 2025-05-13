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
use databend_common_ast::ast::OnErrorMode;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;

use super::util::TableIdentifier;
use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::CopyIntoTableMode;
use crate::plans::Insert;
use crate::plans::InsertInputSource;
use crate::plans::InsertValue;
use crate::plans::Plan;
use crate::BindContext;
use crate::DefaultExprBinder;

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
                            "The value specified for computed column '{}' is not allowed",
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
            with,
            catalog,
            database,
            table,
            columns,
            source,
            overwrite,
            ..
        } = stmt;

        self.init_cte(bind_context, with)?;

        let table_identifier = TableIdentifier::new(self, catalog, database, table, &None);
        let (catalog_name, database_name, table_name) = (
            table_identifier.catalog_name(),
            table_identifier.database_name(),
            table_identifier.table_name(),
        );

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await
            .map_err(|err| table_identifier.not_found_suggest_error(err))?;

        let schema = self.schema_project(&table.schema(), columns)?;

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
                    None => Ok(InsertInputSource::Values(InsertValue::RawValues {
                        data: rest_str,
                        start,
                    })),
                }
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                Ok(InsertInputSource::SelectPlan(Box::new(select_plan)))
            }
            InsertSource::StreamingLoad {
                format_options,
                on_error_mode,
            } => {
                let file_format_params = FileFormatParams::try_from_reader(
                    FileFormatOptionsReader::from_ast(&format_options),
                    false,
                )?;
                let required_values_schema: DataSchemaRef = Arc::new(schema.clone().into());

                let default_exprs = if file_format_params.need_field_default() {
                    Some(
                        DefaultExprBinder::try_new(self.ctx.clone())?
                            .prepare_default_values(&required_values_schema)?,
                    )
                } else {
                    None
                };
                Ok(InsertInputSource::StreamingLoad {
                    file_format: Box::new(file_format_params),
                    schema: schema.clone(),
                    on_error_mode: OnErrorMode::from_str(
                        &on_error_mode.unwrap_or("abort".to_string()),
                    )?,
                    block_thresholds: table.get_block_thresholds(),
                    default_exprs,
                    // fill it in HTTP handler
                    receiver: Default::default(),
                })
            }
        };

        let plan = Insert {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            schema,
            overwrite: *overwrite,
            source: input_source?,
            table_info: None,
        };

        Ok(Plan::Insert(Box::new(plan)))
    }
}
