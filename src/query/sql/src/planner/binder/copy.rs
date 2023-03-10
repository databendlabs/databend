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
use std::sync::Arc;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyUnit;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageInfo;
use common_users::UserApiProvider;
use tracing::debug;

use crate::binder::location::parse_uri_location;
use crate::binder::Binder;
use crate::plans::CopyPlan;
use crate::plans::Plan;
use crate::plans::ValidationMode;
use crate::BindContext;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn bind_copy(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt,
    ) -> Result<Plan> {
        match (&stmt.src, &stmt.dst) {
            (
                CopyUnit::StageLocation(stage_location),
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
            ) => {
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                self.bind_copy_from_stage_into_table(
                    bind_context,
                    stmt,
                    &stage_location.name,
                    &stage_location.path,
                    &catalog_name,
                    &database_name,
                    &table_name,
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
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);

                let mut ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    part_prefix: uri_location.part_prefix.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_uri_into_table(
                    bind_context,
                    stmt,
                    &mut ul,
                    &catalog_name,
                    &database_name,
                    &table_name,
                )
                .await
            }
            (
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                },
                CopyUnit::StageLocation(stage_location),
            ) => {
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);

                self.bind_copy_from_table_into_stage(
                    bind_context,
                    stmt,
                    &catalog_name,
                    &database_name,
                    &table_name,
                    &stage_location.name,
                    &stage_location.path,
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
                let (catalog_name, database_name, table) =
                    self.normalize_object_identifier_triple(catalog, database, table);

                let mut ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    part_prefix: uri_location.part_prefix.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_table_into_uri(
                    bind_context,
                    stmt,
                    &catalog_name,
                    &database_name,
                    &table,
                    &mut ul,
                )
                .await
            }
            (CopyUnit::Query(query), CopyUnit::StageLocation(stage_location)) => {
                self.bind_copy_from_query_into_stage(
                    bind_context,
                    stmt,
                    query,
                    &stage_location.name,
                    &stage_location.path,
                )
                .await
            }
            (CopyUnit::Query(query), CopyUnit::UriLocation(uri_location)) => {
                let mut ul = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    part_prefix: uri_location.part_prefix.clone(),
                    connection: uri_location.connection.clone(),
                };

                self.bind_copy_from_query_into_uri(bind_context, stmt, query, &mut ul)
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
        stmt: &CopyStmt,
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
        self.apply_stage_options(stmt, &mut stage_info).await?;

        let from = DataSourcePlan {
            catalog: dst_catalog_name.to_string(),
            source_info: DataSourceInfo::StageSource(StageTableInfo {
                schema: table.schema(),
                stage_info,
                path,
                files: stmt.files.clone(),
                pattern: stmt.pattern.clone(),
                files_to_copy: None,
            }),
            output_schema: table.schema(),
            parts: Partitions::default(),
            statistics: Default::default(),
            description: "".to_string(),
            tbl_args: None,
            push_downs: None,
        };

        Ok(Plan::Copy(Box::new(CopyPlan::IntoTable {
            catalog_name: dst_catalog_name.to_string(),
            database_name: dst_database_name.to_string(),
            table_name: dst_table_name.to_string(),
            table_id: table.get_id(),
            schema: table.schema(),
            from: Box::new(from),
            validation_mode,
            force: stmt.force,
        })))
    }

    /// Bind COPY INFO <table> FROM <uri_location>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_uri_into_table(
        &mut self,
        _: &BindContext,
        stmt: &CopyStmt,
        src_uri_location: &mut UriLocation,
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
        if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy from insecure storage is not allowed",
            ));
        }

        let mut stage_info = StageInfo::new_external_stage(storage_params, &path);
        self.apply_stage_options(stmt, &mut stage_info).await?;
        let from = DataSourcePlan {
            catalog: dst_catalog_name.to_string(),
            source_info: DataSourceInfo::StageSource(StageTableInfo {
                schema: table.schema(),
                stage_info,
                path,
                files: stmt.files.clone(),
                pattern: stmt.pattern.clone(),
                files_to_copy: None,
            }),
            output_schema: table.schema(),
            parts: Partitions::default(),
            statistics: Default::default(),
            description: "".to_string(),
            tbl_args: None,
            push_downs: None,
        };

        Ok(Plan::Copy(Box::new(CopyPlan::IntoTable {
            catalog_name: dst_catalog_name.to_string(),
            database_name: dst_database_name.to_string(),
            table_name: dst_table_name.to_string(),
            table_id: table.get_id(),
            schema: table.schema(),
            from: Box::new(from),
            validation_mode,
            force: stmt.force,
        })))
    }

    /// Bind COPY INFO <stage_location> FROM <table>
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_table_into_stage(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt,
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
        let sub_stmt_msg = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
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
        self.apply_stage_options(stmt, &mut stage_info).await?;

        Ok(Plan::Copy(Box::new(CopyPlan::IntoStage {
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
        stmt: &CopyStmt,
        src_catalog_name: &str,
        src_database_name: &str,
        src_table_name: &str,
        dst_uri_location: &mut UriLocation,
    ) -> Result<Plan> {
        let subquery =
            format!("SELECT * FROM {src_catalog_name}.{src_database_name}.{src_table_name}");
        let tokens = tokenize_sql(&subquery)?;
        let backtrace = Backtrace::new();
        let sub_stmt_msg = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
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
        if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy into insecure storage is not allowed",
            ));
        }

        let mut stage_info = StageInfo::new_external_stage(storage_params, &path);
        self.apply_stage_options(stmt, &mut stage_info).await?;

        Ok(Plan::Copy(Box::new(CopyPlan::IntoStage {
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
        stmt: &CopyStmt,
        src_query: &Query,
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
        self.apply_stage_options(stmt, &mut stage_info).await?;

        Ok(Plan::Copy(Box::new(CopyPlan::IntoStage {
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
        stmt: &CopyStmt,
        src_query: &Query,
        dst_uri_location: &mut UriLocation,
    ) -> Result<Plan> {
        let query = self
            .bind_statement(bind_context, &Statement::Query(Box::new(src_query.clone())))
            .await?;

        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (storage_params, path) = parse_uri_location(dst_uri_location)?;
        if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
            return Err(ErrorCode::StorageInsecure(
                "copy into insecure storage is not allowed",
            ));
        }

        let mut stage_info = StageInfo::new_external_stage(storage_params, &path);
        self.apply_stage_options(stmt, &mut stage_info).await?;

        Ok(Plan::Copy(Box::new(CopyPlan::IntoStage {
            stage: Box::new(stage_info),
            path,
            validation_mode,
            from: Box::new(query),
        })))
    }

    async fn apply_stage_options(&mut self, stmt: &CopyStmt, stage: &mut StageInfo) -> Result<()> {
        if !stmt.file_format.is_empty() {
            stage.file_format_options = self.try_resolve_file_format(&stmt.file_format).await?;
        }

        // Copy options.
        {
            // on_error.
            stage.copy_options.on_error =
                OnErrorMode::from_str(&stmt.on_error).map_err(ErrorCode::SyntaxException)?;

            // size_limit.
            if stmt.size_limit != 0 {
                stage.copy_options.size_limit = stmt.size_limit;
            }
            // max_file_size.
            if stmt.max_file_size != 0 {
                stage.copy_options.max_file_size = stmt.max_file_size;
            }
            stage.copy_options.split_size = stmt.split_size;

            stage.copy_options.single = stmt.single;
            stage.copy_options.purge = stmt.purge;
        }

        Ok(())
    }
}

/// Named stage(start with `@`):
///
/// ```sql
/// copy into mytable from @my_ext_stage
///     file_format = (type = csv);
/// ```
///
/// Returns user's stage info and relative path towards the stage's root.
///
/// If input location is empty we will convert it to `/` means the root of stage
///
/// - @mystage => (mystage, "/")
///
/// If input location is endswith `/`, it's a folder.
///
/// - @mystage/ => (mystage, "/")
///
/// Otherwise, it's a file
///
/// - @mystage/abc => (mystage, "abc")
///
/// For internal stage, we will also add prefix `/stage/<stage>/`
///
/// - @internal/abc => (internal, "/stage/internal/abc")
pub async fn parse_stage_location(
    ctx: &Arc<dyn TableContext>,
    location: &str,
) -> Result<(StageInfo, String)> {
    let s: Vec<&str> = location.split('@').collect();
    // @my_ext_stage/abc/
    let names: Vec<&str> = s[1].splitn(2, '/').filter(|v| !v.is_empty()).collect();

    let stage = if names[0] == "~" {
        StageInfo::new_user_stage(&ctx.get_current_user()?.name)
    } else {
        UserApiProvider::instance()
            .get_stage(&ctx.get_tenant(), names[0])
            .await?
    };

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');
    let path = if path.is_empty() { "/" } else { path };

    debug!("parsed stage: {stage:?}, path: {path}");
    Ok((stage, path.to_string()))
}

/// parse_stage_location_v2 work similar to parse_stage_location.
///
/// Difference is input location has already been parsed by parser.
///
/// # NOTE:
/// `path` MUST starts with '/'
pub async fn parse_stage_location_v2(
    ctx: &Arc<dyn TableContext>,
    name: &str,
    path: &str,
) -> Result<(StageInfo, String)> {
    let stage = if name == "~" {
        StageInfo::new_user_stage(&ctx.get_current_user()?.name)
    } else {
        UserApiProvider::instance()
            .get_stage(&ctx.get_tenant(), name)
            .await?
    };

    // prefix must be endswith `/`, so we should trim path here.
    let relative_path = path.trim_start_matches('/').to_string();

    debug!("parsed stage: {stage:?}, path: {relative_path}");
    Ok((stage, relative_path))
}
