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

use std::str::FromStr;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyUnit;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;
use common_storage::parse_uri_location;
use common_storage::UriLocation;

use crate::sessions::query_ctx::QryCtx;
use crate::sql::binder::Binder;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::plans::ValidationMode;
use crate::sql::statements::parse_copy_file_format_options;
use crate::sql::statements::parse_stage_location_v2;
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
                CopyUnit::UriLocation(uri_location),
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

                let ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_uri_into_table(
                    bind_context,
                    stmt,
                    &ul,
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
                CopyUnit::UriLocation(uri_location),
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

                let ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_table_into_uri(
                    bind_context,
                    stmt,
                    &catalog_name,
                    &database_name,
                    &table,
                    &ul,
                )
                .await
            }
            (CopyUnit::Query(query), CopyUnit::StageLocation { name, path }) => {
                self.bind_copy_from_query_into_stage(bind_context, stmt, query, name, path)
                    .await
            }
            (CopyUnit::Query(query), CopyUnit::UriLocation(uri_location)) => {
                let ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_query_into_uri(bind_context, stmt, query, &ul)
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
        src_uri_location: &UriLocation,
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

        let (storage_params, path) = parse_uri_location(src_uri_location)?;
        if !storage_params.is_secure() && !self.ctx.get_config().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy from insecure storage is not allowed",
            ));
        }

        let mut stage_info = UserStageInfo::new_external_stage(storage_params, &path);
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
        let sub_stmt_msg = parse_sql(&tokens, &backtrace)?;
        let sub_stmt = sub_stmt_msg.0;
        let query = match &sub_stmt {
            Statement::Query(query) => {
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await?
            }
            _ => {
                return Err(ErrorCode::SyntaxException(
                    "COPY INTO <location> FROM <non-query> is invalid",
                ));
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
        dst_uri_location: &UriLocation,
    ) -> Result<Plan> {
        let subquery =
            format!("SELECT * FROM {src_catalog_name}.{src_database_name}.{src_table_name}");
        let tokens = tokenize_sql(&subquery)?;
        let backtrace = Backtrace::new();
        let sub_stmt_msg = parse_sql(&tokens, &backtrace)?;
        let sub_stmt = sub_stmt_msg.0;
        let query = match &sub_stmt {
            Statement::Query(query) => {
                self.bind_statement(bind_context, &Statement::Query(query.clone()))
                    .await?
            }
            _ => {
                return Err(ErrorCode::SyntaxException(
                    "COPY INTO <location> FROM <non-query> is invalid",
                ));
            }
        };

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (storage_params, path) = parse_uri_location(dst_uri_location)?;
        if !storage_params.is_secure() && !self.ctx.get_config().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy into insecure storage is not allowed",
            ));
        }

        let mut stage_info = UserStageInfo::new_external_stage(storage_params, &path);
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
        dst_uri_location: &UriLocation,
    ) -> Result<Plan> {
        let query = self
            .bind_statement(bind_context, &Statement::Query(Box::new(src_query.clone())))
            .await?;

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (storage_params, path) = parse_uri_location(dst_uri_location)?;
        if !storage_params.is_secure() && !self.ctx.get_config().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy into insecure storage is not allowed",
            ));
        }

        let mut stage_info = UserStageInfo::new_external_stage(storage_params, &path);
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
