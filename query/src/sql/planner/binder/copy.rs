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
use common_planners::ValidationMode;

use crate::sql::binder::Binder;
use crate::sql::plans::Plan;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location;
use crate::sql::statements::parse_uri_location;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_copy(
        &mut self,
        stmt: &CopyStmt<'a>,
    ) -> Result<Plan> {
        match &stmt.dst {
            CopyTarget::Table(catalog, database, table) => {
                let (mut stage_info, path) = match &stmt.src {
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

                let catalog_name = catalog
                    .map(|v| v.to_string())
                    .unwrap_or(self.ctx.get_current_catalog());
                let database_name = database
                    .map(|v| v.to_string())
                    .unwrap_or(self.ctx.get_current_database());
                let table_name = table.to_string();
                let table = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await?;
                let mut schema = table.schema();
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

                // Pattern.
                let pattern = self.pattern.clone();

                Ok(Plan::Copy(Box::new(CopyPlan {
                    validation_mode,
                    copy_mode: CopyMode::IntoTable {
                        catalog_name,
                        db_name,
                        tbl_name,
                        tbl_id,
                        schema,
                        from,
                        files: self.files.clone(),
                        pattern,
                    },
                })))
            }
            CopyTarget::Location(location) => {
                let (mut stage_info, path) = self.bind_stage(stmt, location).await?;
                let query = match &stmt.src {
                    CopyTarget::Table(catalog, database, table) => {
                        let catalog_name = catalog
                            .map(|v| v.to_string())
                            .unwrap_or(self.ctx.get_current_catalog());
                        let database_name = database
                            .map(|v| v.to_string())
                            .unwrap_or(self.ctx.get_current_database());
                        let table_name = table.to_string();
                        let subquery = format!(
                            "SELECT * FROM {}.{}.{}",
                            catalog_name, database_name, table_name
                        );
                        let tokens = tokenize_sql(sql)?;
                        let backtrace = Backtrace::new();
                        let stmts = parse_sql(&tokens, &backtrace)?;
                        if stmts.len() > 1 {
                            return Err(ErrorCode::UnImplement("unsupported multiple statements"));
                        }
                        match &stmts[0] {
                            Statement::Query(query) => query,
                            _ => {
                                return Err(ErrorCode::SyntaxException(
                                    "COPY INTO <location> FROM <non-query> is invalid",
                                ))
                            }
                        }
                    }
                    CopyTarget::Query(query) => query,
                    CopyTarget::Location(_) => {
                        return Err(ErrorCode::SyntaxException(
                            "COPY INTO <location> FROM <location> is invalid",
                        ))
                    }
                };

                Ok(Plan::Copy(Box::new(CopyPlan {
                    validation_mode,
                    copy_mode: CopyMode::IntoStage {
                        stage_table_info: StageTableInfo {
                            schema: query.schema(),
                            stage_info,
                            path,
                            files: vec![],
                        },
                        // TODO(xuanwo): we need to convert query to Plan.
                        query: Box::new(query),
                    },
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
            stage_info.file_format_options =
                parse_copy_file_format_options(&self.file_format_options)?;
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

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        Ok((stage_info, path))
    }
}
