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

use databend_common_ast::ast::CopyIntoLocationSource;
use databend_common_ast::ast::CopyIntoLocationStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;

use crate::binder::copy_into_table::resolve_file_location;
use crate::binder::Binder;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Plan;
use crate::BindContext;

impl<'a> Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_copy_into_location(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyIntoLocationStmt,
    ) -> Result<Plan> {
        let query = match &stmt.src {
            CopyIntoLocationSource::Table(table) => {
                let (catalog_name, database_name, table_name) = self
                    .normalize_object_identifier_triple(
                        &table.catalog,
                        &table.database,
                        &table.table,
                    );
                let subquery = format!("SELECT * FROM {catalog_name}.{database_name}.{table_name}");
                let tokens = tokenize_sql(&subquery)?;
                let sub_stmt_msg = parse_sql(&tokens, self.dialect)?;
                let sub_stmt = sub_stmt_msg.0;
                match &sub_stmt {
                    Statement::Query(query) => {
                        self.bind_statement(bind_context, &Statement::Query(query.clone()))
                            .await
                    }
                    _ => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <location> FROM <non-query> is invalid",
                        ));
                    }
                }
            }
            CopyIntoLocationSource::Query(query) => {
                if let Some(with) = &stmt.with {
                    self.add_cte(with, bind_context)?;
                }
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await
            }
        }?;

        let (mut stage_info, path) = resolve_file_location(self.ctx.as_ref(), &stmt.dst).await?;
        self.apply_copy_into_location_options(stmt, &mut stage_info)
            .await?;

        Ok(Plan::CopyIntoLocation(CopyIntoLocationPlan {
            stage: Box::new(stage_info),
            path,
            from: Box::new(query),
        }))
    }

    #[async_backtrace::framed]
    pub async fn apply_copy_into_location_options(
        &mut self,
        stmt: &CopyIntoLocationStmt,
        stage: &mut StageInfo,
    ) -> Result<()> {
        if !stmt.file_format.is_empty() {
            stage.file_format_params = self.try_resolve_file_format(&stmt.file_format).await?;
        }

        // Copy options.
        {
            // max_file_size.
            if stmt.max_file_size != 0 {
                stage.copy_options.max_file_size = stmt.max_file_size;
            }
            stage.copy_options.single = stmt.single;
            stage.copy_options.detailed_output = stmt.detailed_output;
        }

        Ok(())
    }
}
