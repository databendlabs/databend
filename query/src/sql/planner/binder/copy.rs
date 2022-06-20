// Copyright 2022 Datafuse Labs.
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

use std::assert_matches::debug_assert_matches;
use std::collections::BTreeMap;
use std::str::FromStr;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyTarget;
use common_ast::ast::CreateStageStmt;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::OnErrorMode;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_planners::CopyMode;
use common_planners::CopyPlan;
use common_planners::CreateUserStagePlan;
use common_planners::ListPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::RemoveUserStagePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;

use crate::interpreters::SelectInterpreterV2;
use crate::sql::binder::Binder;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::plans::ValidationMode;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location;
use crate::sql::statements::parse_uri_location;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_copy(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
    ) -> Result<Plan> {
        match &stmt.dst {
            CopyTarget::Table(catalog, database, table) => {
                let (stage_info, path) = match &stmt.src {
                    CopyTarget::Table(_, _, _) => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <table> FROM <table> is invalid",
                        ))
                    }
                    CopyTarget::Query(_) => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <table> FROM <query> is invalid",
                        ))
                    }
                    // TODO(xuanwo): we need to parse credential and encryption.
                    CopyTarget::Location(location) => self.bind_stage(stmt, location).await?,
                };

                let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
                    .map_err(ErrorCode::SyntaxException)?;

                let catalog_name = catalog
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or(self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or(self.ctx.get_current_database());
                let table_name = table.to_string();
                let table = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await?;
                let schema = table.schema();
                let table_id = table.get_id();

                // TODO(xuanwo): we need to support columns in COPY.

                // Read Source plan.
                let from = ReadDataSourcePlan {
                    catalog: catalog_name.clone(),
                    source_info: SourceInfo::StageSource(StageTableInfo {
                        schema: schema.clone(),
                        stage_info,
                        path,
                        files: vec![],
                    }),
                    scan_fields: None,
                    parts: vec![],
                    statistics: Default::default(),
                    description: "".to_string(),
                    tbl_args: None,
                    push_downs: None,
                };

                Ok(Plan::Copy(Box::new(CopyPlanV2::IntoTable {
                    catalog_name,
                    database_name,
                    table_name,
                    table_id,
                    schema,
                    from,
                    files: stmt.files.clone(),
                    pattern: stmt.pattern.clone(),
                    validation_mode,
                })))
            }
            CopyTarget::Location(location) => {
                let (stage_info, path) = self.bind_stage(stmt, location).await?;
                let query = match &stmt.src {
                    CopyTarget::Table(catalog, database, table) => {
                        let catalog_name = catalog
                            .as_ref()
                            .map(|v| v.to_string())
                            .unwrap_or(self.ctx.get_current_catalog());
                        let database_name = database
                            .as_ref()
                            .map(|v| v.to_string())
                            .unwrap_or(self.ctx.get_current_database());
                        let table_name = table.to_string();

                        let subquery = format!(
                            "SELECT * FROM {}.{}.{}",
                            catalog_name, database_name, table_name
                        );
                        let tokens = tokenize_sql(&subquery)?;
                        let backtrace = Backtrace::new();
                        let stmts = parse_sql(&tokens, &backtrace)?;
                        if stmts.len() > 1 {
                            return Err(ErrorCode::UnImplement("unsupported multiple statements"));
                        }
                        match &stmts[0] {
                            Statement::Query(query) => {
                                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                                    .await?
                            }
                            _ => {
                                return Err(ErrorCode::SyntaxException(
                                    "COPY INTO <location> FROM <non-query> is invalid",
                                ))
                            }
                        }
                    }
                    CopyTarget::Query(query) => {
                        self.bind_statement(bind_context, &Statement::Query(query.clone()))
                            .await?
                    }
                    CopyTarget::Location(_) => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <location> FROM <location> is invalid",
                        ))
                    }
                };

                debug_assert!(
                    matches!(query, Plan::Query { .. }),
                    "input sql must be Plan::Query, but it's not"
                );

                // Validation mode.
                let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
                    .map_err(ErrorCode::SyntaxException)?;

                Ok(Plan::Copy(Box::new(CopyPlanV2::IntoStage {
                    stage: stage_info,
                    path,
                    validation_mode,
                    from: Box::new(query),
                })))
            }
            CopyTarget::Query(_) => {
                return Err(ErrorCode::SyntaxException("COPY INTO <query> is invalid"))
            }
        }
    }

    async fn bind_stage(
        &mut self,
        stmt: &CopyStmt<'a>,
        location: &str,
    ) -> Result<(UserStageInfo, String)> {
        let (mut stage_info, path) = if location.starts_with('@') {
            parse_stage_location(&self.ctx, &location).await?
        } else {
            parse_uri_location(&location, &BTreeMap::new(), &BTreeMap::new())?
        };

        if !stmt.file_format.is_empty() {
            stage_info.file_format_options = parse_copy_file_format_options(&stmt.file_format)?;
        }

        // Copy options.
        {
            // TODO(xuanwo): COPY should handle on error.
            // on_error.
            // if !stmt.on_error.is_empty() {
            //     stage_info.copy_options.on_error =
            //         OnErrorMode::from_str(&self.on_error).map_err(ErrorCode::SyntaxException)?;
            // }

            // size_limit.
            if stmt.size_limit != 0 {
                stage_info.copy_options.size_limit = stmt.size_limit;
            }
        }

        Ok((stage_info, path))
    }
}
