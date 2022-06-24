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
use common_ast::ast::CopyUnit;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;

use crate::sql::binder::Binder;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::plans::ValidationMode;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location_v2;
use crate::sql::statements::parse_uri_location_v2;
use crate::sql::BindContext;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_copy(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
    ) -> Result<Plan> {
        match (&stmt.src, &stmt.dst) {
            (
                CopyUnit::StageLocation { name, path },
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
            ) => {
                let catalog_name = catalog
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = table.to_string();

                self.bind_copy_from_stage_into_table(
                    bind_context,
                    stmt,
                    name,
                    path,
                    &catalog_name,
                    &database_name,
                    &table,
                )
                .await
            }
            (
                CopyUnit::UriLocation {
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                },
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
            ) => {
                let catalog_name = catalog
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = table.to_string();

                self.bind_copy_from_uri_into_table(
                    bind_context,
                    stmt,
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                    &catalog_name,
                    &database_name,
                    &table,
                )
                .await
            }
            (
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
                CopyUnit::StageLocation { name, path },
            ) => {
                let catalog_name = catalog
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = table.to_string();

                self.bind_copy_from_table_into_stage(
                    bind_context,
                    stmt,
                    &catalog_name,
                    &database_name,
                    &table,
                    name,
                    path,
                )
                .await
            }
            (
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
                CopyUnit::UriLocation {
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                },
            ) => {
                let catalog_name = catalog
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = table.to_string();

                self.bind_copy_from_table_into_uri(
                    bind_context,
                    stmt,
                    &catalog_name,
                    &database_name,
                    &table,
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                )
                .await
            }
            (CopyUnit::Query(query), CopyUnit::StageLocation { name, path }) => {
                self.bind_copy_from_query_into_stage(bind_context, stmt, query, name, path)
                    .await
            }
            (
                CopyUnit::Query(query),
                CopyUnit::UriLocation {
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                },
            ) => {
                self.bind_copy_from_query_into_uri(
                    bind_context,
                    stmt,
                    query,
                    protocol,
                    name,
                    path,
                    credentials,
                    encryption,
                )
                .await
            }
            (src, dst) => Err(ErrorCode::SyntaxException(format!(
                "COPY INTO <{}> FROM <{}> is invalid",
                dst.target(),
                src.target()
            ))),
        }
    }

    /// Bind COPY INFO <table> FROM <stage_location>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_stage_into_table(
        &mut self,
        _: &BindContext,
        stmt: &CopyStmt<'a>,
        src_stage: &str,
        src_path: &str,
        dst_catalog_name: &str,
        dst_database_name: &str,
        dst_table_name: &str,
    ) -> Result<Plan> {
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let table = self
            .ctx
            .get_table(dst_catalog_name, dst_database_name, dst_table_name)
            .await?;

        let (mut stage_info, path) =
            parse_stage_location_v2(&self.ctx, src_stage, src_path).await?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        let from = ReadDataSourcePlan {
            catalog: dst_catalog_name.to_string(),
            source_info: SourceInfo::StageSource(StageTableInfo {
                schema: table.schema(),
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
            catalog_name: dst_catalog_name.to_string(),
            database_name: dst_database_name.to_string(),
            table_name: dst_table_name.to_string(),
            table_id: table.get_id(),
            schema: table.schema(),
            from: Box::new(from),
            files: stmt.files.clone(),
            pattern: stmt.pattern.clone(),
            validation_mode,
        })))
    }

    /// Bind COPY INFO <table> FROM <uri_location>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_uri_into_table(
        &mut self,
        _: &BindContext,
        stmt: &CopyStmt<'a>,
        src_protocol: &str,
        src_name: &str,
        src_path: &str,
        src_credentials: &BTreeMap<String, String>,
        src_encryption: &BTreeMap<String, String>,
        dst_catalog_name: &str,
        dst_database_name: &str,
        dst_table_name: &str,
    ) -> Result<Plan> {
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let table = self
            .ctx
            .get_table(dst_catalog_name, dst_database_name, dst_table_name)
            .await?;

        let (mut stage_info, path) = parse_uri_location_v2(
            src_protocol,
            src_name,
            src_path,
            src_credentials,
            src_encryption,
        )?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        let from = ReadDataSourcePlan {
            catalog: dst_catalog_name.to_string(),
            source_info: SourceInfo::StageSource(StageTableInfo {
                schema: table.schema(),
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
            catalog_name: dst_catalog_name.to_string(),
            database_name: dst_database_name.to_string(),
            table_name: dst_table_name.to_string(),
            table_id: table.get_id(),
            schema: table.schema(),
            from: Box::new(from),
            files: stmt.files.clone(),
            pattern: stmt.pattern.clone(),
            validation_mode,
        })))
    }

    /// Bind COPY INFO <stage_location> FROM <table>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_table_into_stage(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
        src_catalog_name: &str,
        src_database_name: &str,
        src_table_name: &str,
        dst_stage: &str,
        dst_path: &str,
    ) -> Result<Plan> {
        let subquery =
            format!("SELECT * FROM {src_catalog_name}.{src_database_name}.{src_table_name}");
        let tokens = tokenize_sql(&subquery)?;
        let backtrace = Backtrace::new();
        let sub_stmt = parse_sql(&tokens, &backtrace)?;

        let query = match &sub_stmt {
            Statement::Query(query) => {
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await?
            }
            _ => {
                return Err(ErrorCode::SyntaxException(
                    "COPY INTO <location> FROM <non-query> is invalid",
                ))
            }
        };

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) =
            parse_stage_location_v2(&self.ctx, dst_stage, dst_path).await?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        Ok(Plan::Copy(Box::new(CopyPlanV2::IntoStage {
            stage: Box::new(stage_info),
            path,
            validation_mode,
            from: Box::new(query),
        })))
    }

    /// Bind COPY INFO <uri_location> FROM <table>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_table_into_uri(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
        src_catalog_name: &str,
        src_database_name: &str,
        src_table_name: &str,
        dst_protocol: &str,
        dst_name: &str,
        dst_path: &str,
        dst_credentials: &BTreeMap<String, String>,
        dst_encryption: &BTreeMap<String, String>,
    ) -> Result<Plan> {
        let subquery =
            format!("SELECT * FROM {src_catalog_name}.{src_database_name}.{src_table_name}");
        let tokens = tokenize_sql(&subquery)?;
        let backtrace = Backtrace::new();
        let sub_stmt = parse_sql(&tokens, &backtrace)?;

        let query = match &sub_stmt {
            Statement::Query(query) => {
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await?
            }
            _ => {
                return Err(ErrorCode::SyntaxException(
                    "COPY INTO <location> FROM <non-query> is invalid",
                ))
            }
        };

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) = parse_uri_location_v2(
            dst_protocol,
            dst_name,
            dst_path,
            dst_credentials,
            dst_encryption,
        )?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        Ok(Plan::Copy(Box::new(CopyPlanV2::IntoStage {
            stage: Box::new(stage_info),
            path,
            validation_mode,
            from: Box::new(query),
        })))
    }

    /// Bind COPY INFO <stage_location> FROM <query>
    async fn bind_copy_from_query_into_stage(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
        src_query: &Query<'_>,
        dst_stage: &str,
        dst_path: &str,
    ) -> Result<Plan> {
        let query = self
            .bind_statement(bind_context, &Statement::Query(Box::new(src_query.clone())))
            .await?;

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) =
            parse_stage_location_v2(&self.ctx, dst_stage, dst_path).await?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        Ok(Plan::Copy(Box::new(CopyPlanV2::IntoStage {
            stage: Box::new(stage_info),
            path,
            validation_mode,
            from: Box::new(query),
        })))
    }

    /// Bind COPY INFO <uri_location> FROM <query>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_query_into_uri(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt<'a>,
        src_query: &Query<'_>,
        dst_protocol: &str,
        dst_name: &str,
        dst_path: &str,
        dst_credentials: &BTreeMap<String, String>,
        dst_encryption: &BTreeMap<String, String>,
    ) -> Result<Plan> {
        let query = self
            .bind_statement(bind_context, &Statement::Query(Box::new(src_query.clone())))
            .await?;

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) = parse_uri_location_v2(
            dst_protocol,
            dst_name,
            dst_path,
            dst_credentials,
            dst_encryption,
        )?;
        self.apply_stage_options(stmt, &mut stage_info)?;

        Ok(Plan::Copy(Box::new(CopyPlanV2::IntoStage {
            stage: Box::new(stage_info),
            path,
            validation_mode,
            from: Box::new(query),
        })))
    }

    fn apply_stage_options(
        &mut self,
        stmt: &CopyStmt<'a>,
        stage: &mut UserStageInfo,
    ) -> Result<()> {
        if !stmt.file_format.is_empty() {
            stage.file_format_options = parse_copy_file_format_options(&stmt.file_format)?;
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
                stage.copy_options.size_limit = stmt.size_limit;
            }
        }

        Ok(())
    }
}
