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
use std::sync::Arc;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyUnit;
use common_ast::ast::Query;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::parse_escape_string;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::SourceInfo;
use common_legacy_planners::StageTableInfo;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
use common_storage::parse_uri_location;
use common_storage::UriLocation;
use common_users::UserApiProvider;
use tracing::debug;

use crate::sql::binder::Binder;
use crate::sql::normalize_identifier;
use crate::sql::plans::CopyPlanV2;
use crate::sql::plans::Plan;
use crate::sql::plans::ValidationMode;
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
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = normalize_identifier(table, &self.name_resolution_ctx).name;

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
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = normalize_identifier(table, &self.name_resolution_ctx).name;

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
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = normalize_identifier(table, &self.name_resolution_ctx).name;

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
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let database_name = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = normalize_identifier(table, &self.name_resolution_ctx).name;

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
            force: stmt.force,
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
            force: stmt.force,
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
) -> Result<(UserStageInfo, String)> {
    let s: Vec<&str> = location.split('@').collect();
    // @my_ext_stage/abc/
    let names: Vec<&str> = s[1].splitn(2, '/').filter(|v| !v.is_empty()).collect();
    let stage = UserApiProvider::instance()
        .get_stage(&ctx.get_tenant(), names[0])
        .await?;

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');

    let prefix = stage.get_prefix();
    let relative_path = format!("{prefix}{path}");

    debug!("parsed stage: {stage:?}, path: {relative_path}");
    Ok((stage, relative_path))
}

/// parse_stage_location_v2 work similar to parse_stage_location.
///
/// Difference is input location has already been parsed by parser.
pub async fn parse_stage_location_v2(
    ctx: &Arc<dyn TableContext>,
    name: &str,
    path: &str,
) -> Result<(UserStageInfo, String)> {
    debug_assert!(path.starts_with('/'), "path should starts with '/'");

    let stage = UserApiProvider::instance()
        .get_stage(&ctx.get_tenant(), name)
        .await?;

    let prefix = stage.get_prefix();
    debug_assert!(prefix.ends_with('/'), "prefix should ends with '/'");

    // prefix must be endswith `/`, so we should trim path here.
    let relative_path = format!("{prefix}{}", path.trim_start_matches('/'));

    debug!("parsed stage: {stage:?}, path: {relative_path}");
    Ok((stage, relative_path))
}

/// TODO(xuanwo): Move those logic into parser
pub fn parse_copy_file_format_options(
    file_format_options: &BTreeMap<String, String>,
) -> Result<FileFormatOptions> {
    // File format type.
    let format = file_format_options
        .get("type")
        .ok_or_else(|| ErrorCode::SyntaxException("File format type must be specified"))?;
    let file_format = StageFileFormatType::from_str(format)
        .map_err(|e| ErrorCode::SyntaxException(format!("File format type error:{:?}", e)))?;

    // Skip header.
    let skip_header = file_format_options
        .get("skip_header")
        .unwrap_or(&"0".to_string())
        .parse::<u64>()?;

    // Field delimiter.
    let field_delimiter = parse_escape_string(
        file_format_options
            .get("field_delimiter")
            .unwrap_or(&"".to_string())
            .as_bytes(),
    );

    // Record delimiter.
    let record_delimiter = parse_escape_string(
        file_format_options
            .get("record_delimiter")
            .unwrap_or(&"".to_string())
            .as_bytes(),
    );

    // Compression delimiter.
    let compression = parse_escape_string(
        file_format_options
            .get("compression")
            .unwrap_or(&"none".to_string())
            .as_bytes(),
    )
    .parse()
    .map_err(ErrorCode::UnknownCompressionType)?;

    Ok(FileFormatOptions {
        format: file_format,
        skip_header,
        field_delimiter,
        record_delimiter,
        compression,
    })
}
