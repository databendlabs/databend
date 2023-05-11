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

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use common_ast::ast::CopyStmt;
use common_ast::ast::CopyUnit;
use common_ast::ast::Expr;
use common_ast::ast::FileLocation;
use common_ast::ast::Identifier;
use common_ast::ast::Query;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::Statement;
use common_ast::ast::TableAlias;
use common_ast::ast::TableReference;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageInfo;
use common_storage::init_stage_operator;
use common_storage::StageFilesInfo;
use common_users::UserApiProvider;
use tracing::debug;
use tracing::info;

use crate::binder::location::parse_uri_location;
use crate::binder::Binder;
use crate::plans::CopyPlan;
use crate::plans::Plan;
use crate::plans::ValidationMode;
use crate::BindContext;

impl<'a> Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_copy(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyStmt,
    ) -> Result<Plan> {
        match (&stmt.src, &stmt.dst) {
            (
                CopyUnit::StageLocation(location),
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                    columns,
                },
            ) => {
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);

                let (mut stage_info, path) =
                    parse_stage_location_v2(&self.ctx, &location.name, &location.path).await?;
                self.apply_stage_options(stmt, &mut stage_info).await?;
                let files_info = StageFilesInfo {
                    path,
                    files: stmt.files.clone(),
                    pattern: stmt.pattern.clone(),
                };

                self.bind_copy_from_stage_into_table(
                    bind_context,
                    stmt,
                    stage_info,
                    files_info,
                    &catalog_name,
                    &database_name,
                    &table_name,
                    columns,
                )
                .await
            }
            (
                CopyUnit::UriLocation(uri_location),
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                    columns,
                },
            ) => {
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);

                let mut uri_location = UriLocation {
                    protocol: uri_location.protocol.clone(),
                    name: uri_location.name.clone(),
                    path: uri_location.path.clone(),
                    part_prefix: uri_location.part_prefix.clone(),
                    connection: uri_location.connection.clone(),
                };

                let (storage_params, path) = parse_uri_location(&mut uri_location)?;
                if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
                    return Err(ErrorCode::StorageInsecure(
                        "copy from insecure storage is not allowed",
                    ));
                }

                let mut stage_info = StageInfo::new_external_stage(storage_params, &path);
                self.apply_stage_options(stmt, &mut stage_info).await?;

                let files_info = StageFilesInfo {
                    path,
                    files: stmt.files.clone(),
                    pattern: stmt.pattern.clone(),
                };

                self.bind_copy_from_stage_into_table(
                    bind_context,
                    stmt,
                    stage_info,
                    files_info,
                    &catalog_name,
                    &database_name,
                    &table_name,
                    columns,
                )
                .await
            }
            (
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                    columns,
                },
                CopyUnit::StageLocation(stage_location),
            ) => {
                if columns.is_some() {
                    return Err(ErrorCode::SyntaxException(
                        "COPY FROM STAGE does not support column list",
                    ));
                }
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
                    columns,
                },
                CopyUnit::UriLocation(uri_location),
            ) => {
                if columns.is_some() {
                    return Err(ErrorCode::SyntaxException(
                        "COPY FROM STAGE does not support column list",
                    ));
                }
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
            (
                CopyUnit::Query(query),
                CopyUnit::Table {
                    catalog,
                    database,
                    table,
                    columns,
                },
            ) => {
                let (catalog_name, database_name, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                let (select_list, location, alias) = check_transform_query(query)?;
                if matches!(location, FileLocation::Uri(_)) {
                    return Err(ErrorCode::SyntaxException(
                        "copy into table from uri with transform not supported yet",
                    ));
                }
                let (mut stage_info, path) =
                    parse_file_location(&self.ctx, location, BTreeMap::new()).await?;
                self.apply_stage_options(stmt, &mut stage_info).await?;
                let files_info = StageFilesInfo {
                    path,
                    pattern: stmt.pattern.clone(),
                    files: stmt.files.clone(),
                };
                self.bind_copy_from_query_into_table(
                    bind_context,
                    stmt,
                    select_list,
                    alias,
                    stage_info,
                    files_info,
                    &catalog_name,
                    &database_name,
                    &table_name,
                    columns,
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
    #[async_backtrace::framed]
    async fn bind_copy_from_stage_into_table(
        &mut self,
        bind_ctx: &BindContext,
        stmt: &CopyStmt,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        dst_catalog_name: &str,
        dst_database_name: &str,
        dst_table_name: &str,
        columns: &Option<Vec<Identifier>>,
    ) -> Result<Plan> {
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let table = self
            .ctx
            .get_table(dst_catalog_name, dst_database_name, dst_table_name)
            .await?;

        let schema = match columns {
            Some(cols) => self.schema_project(&table.schema(), cols)?,
            None => table.schema(),
        };

        if matches!(stage_info.file_format_params, FileFormatParams::Parquet(_)) {
            let select_list = schema
                .fields()
                .iter()
                .map(|f| SelectTarget::AliasedExpr {
                    expr: Box::new(Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: None,
                        column: Identifier {
                            name: f.name().to_string(),
                            quote: None,
                            span: None,
                        },
                    }),
                    alias: None,
                })
                .collect::<Vec<_>>();

            self.bind_copy_from_query_into_table(
                bind_ctx,
                stmt,
                &select_list,
                &None,
                stage_info,
                files_info,
                dst_catalog_name,
                dst_database_name,
                dst_table_name,
                columns,
            )
            .await
        } else {
            let from = DataSourcePlan {
                catalog: dst_catalog_name.to_string(),
                source_info: DataSourceInfo::StageSource(StageTableInfo {
                    schema: schema.clone(),
                    stage_info,
                    files_info,
                    files_to_copy: None,
                }),
                output_schema: schema,
                parts: Partitions::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
                query_internal_columns: false,
            };

            Ok(Plan::Copy(Box::new(CopyPlan::IntoTable {
                catalog_name: dst_catalog_name.to_string(),
                database_name: dst_database_name.to_string(),
                table_name: dst_table_name.to_string(),
                table_id: table.get_id(),
                from: Box::new(from),
                validation_mode,
                force: stmt.force,
            })))
        }
    }

    /// Bind COPY INFO <stage_location> FROM <table>
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    async fn bind_copy_from_table_into_stage(
        &mut self,
        bind_context: &mut BindContext,
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
        let sub_stmt_msg = parse_sql(&tokens, Dialect::PostgreSQL)?;
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
    #[async_backtrace::framed]
    async fn bind_copy_from_table_into_uri(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyStmt,
        src_catalog_name: &str,
        src_database_name: &str,
        src_table_name: &str,
        dst_uri_location: &mut UriLocation,
    ) -> Result<Plan> {
        let subquery =
            format!("SELECT * FROM {src_catalog_name}.{src_database_name}.{src_table_name}");
        let tokens = tokenize_sql(&subquery)?;
        let sub_stmt_msg = parse_sql(&tokens, Dialect::PostgreSQL)?;
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
    #[async_backtrace::framed]
    async fn bind_copy_from_query_into_stage(
        &mut self,
        bind_context: &mut BindContext,
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
    #[async_backtrace::framed]
    async fn bind_copy_from_query_into_uri(
        &mut self,
        bind_context: &mut BindContext,
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

    /// Bind COPY INTO <table> FROM <query>
    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    async fn bind_copy_from_query_into_table(
        &mut self,
        bind_context: &BindContext,
        stmt: &CopyStmt,
        select_list: &'a [SelectTarget],
        alias: &Option<TableAlias>,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        dst_catalog_name: &str,
        dst_database_name: &str,
        dst_table_name: &str,
        columns: &Option<Vec<Identifier>>,
    ) -> Result<Plan> {
        // Validation mode.
        let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        // dst
        let dst_table = self
            .ctx
            .get_table(dst_catalog_name, dst_database_name, dst_table_name)
            .await?;

        let start = Instant::now();
        {
            let status = "begin to list files";
            self.ctx.set_status_info(status);
            info!(status);
        }

        let operator = init_stage_operator(&stage_info)?;
        let max_files = stage_info.copy_options.max_files;
        let max_files = if max_files == 0 {
            None
        } else {
            Some(max_files)
        };
        let mut files = if operator.info().can_blocking() {
            if stmt.force {
                files_info.blocking_list(&operator, false, max_files)
            } else {
                files_info.blocking_list(&operator, false, None)
            }
        } else if stmt.force {
            files_info.list(&operator, false, max_files).await
        } else {
            files_info.list(&operator, false, None).await
        }?;

        let num_all_files = files.len();
        info!("end to list files: {}", num_all_files);

        let need_copy_file_infos = if stmt.force {
            files
        } else {
            // Status.
            {
                let status = "begin to color copied files";
                self.ctx.set_status_info(status);
                info!(status);
            }

            files = self
                .ctx
                .filter_out_copied_files(
                    dst_catalog_name,
                    dst_database_name,
                    dst_table_name,
                    &files,
                    max_files,
                )
                .await?;

            info!("end to color copied files: {}", files.len());
            files
        };

        info!(
            "copy: read all files finished, all:{}, need copy:{}, max_files:{:?}, elapsed:{}",
            num_all_files,
            need_copy_file_infos.len(),
            max_files,
            start.elapsed().as_secs()
        );

        if need_copy_file_infos.is_empty() {
            return Ok(Plan::Copy(Box::new(CopyPlan::NoFileToCopy)));
        }

        let (s_expr, mut from_context) = self
            .bind_stage_table(
                bind_context,
                stage_info.clone(),
                files_info,
                alias,
                Some(need_copy_file_infos.clone()),
            )
            .await?;

        // Generate a analyzed select list with from context
        let select_list = self
            .normalize_select_list(&mut from_context, select_list)
            .await?;
        let (scalar_items, projections) = self.analyze_projection(&from_context, &select_list)?;
        let s_expr =
            self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;
        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;

        let query_plan = Plan::Query {
            s_expr: Box::new(s_expr),
            metadata: self.metadata.clone(),
            bind_context: Box::new(output_context),
            rewrite_kind: None,
            ignore_result: false,
            formatted_ast: None,
        };

        let schema = match columns {
            Some(cols) => Some(self.schema_project(&dst_table.schema(), cols)?),
            None => None,
        };

        Ok(Plan::Copy(Box::new(CopyPlan::IntoTableWithTransform {
            catalog_name: dst_catalog_name.to_string(),
            database_name: dst_database_name.to_string(),
            table_name: dst_table_name.to_string(),
            table_id: dst_table.get_id(),
            schema,
            from: Box::new(query_plan),
            stage_info: Box::new(stage_info),
            need_copy_file_infos,
            validation_mode,
            force: stmt.force,
        })))
    }

    #[async_backtrace::framed]
    async fn apply_stage_options(&mut self, stmt: &CopyStmt, stage: &mut StageInfo) -> Result<()> {
        if !stmt.file_format.is_empty() {
            stage.file_format_params = self.try_resolve_file_format(&stmt.file_format).await?;
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
            if stmt.max_files != 0 {
                stage.copy_options.max_files = stmt.max_files;
            }
            // max_file_size.
            if stmt.max_file_size != 0 {
                stage.copy_options.max_file_size = stmt.max_file_size;
            }
            stage.copy_options.split_size = stmt.split_size;

            stage.copy_options.single = stmt.single;
            stage.copy_options.purge = stmt.purge;
            stage.copy_options.disable_variant_check = stmt.disable_variant_check;
        }

        Ok(())
    }
}

// we can avoid this by specializing the parser.
// make parse a little more complex, now it is COPY ~ INTO ~ #copy_unit ~ FROM ~ #copy_unit
// also check_query here may give a more friendly error msg.
fn check_transform_query(
    query: &Query,
) -> Result<(&Vec<SelectTarget>, &FileLocation, &Option<TableAlias>)> {
    if query.offset.is_none()
        && query.limit.is_empty()
        && query.order_by.is_empty()
        && query.with.is_none()
    {
        if let SetExpr::Select(select) = &query.body {
            if select.group_by.is_none()
                && !select.distinct
                && select.having.is_none()
                && select.from.len() == 1
            {
                if let TableReference::Stage {
                    span: _,
                    location,
                    options,
                    alias,
                } = &select.from[0]
                {
                    if options.is_empty() {
                        return Ok((&select.select_list, location, alias));
                    } else {
                        return Err(ErrorCode::SyntaxException(
                            "stage table function inside copy not allow options, apply them in the outer copy stmt instead.",
                        ));
                    }
                }
            }
        }
    }
    Err(ErrorCode::SyntaxException(
        "query as source of copy only allow projection on one stage table",
    ))
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
#[async_backtrace::framed]
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
#[async_backtrace::framed]
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

#[async_backtrace::framed]
pub async fn parse_file_location(
    ctx: &Arc<dyn TableContext>,
    location: &FileLocation,
    connection: BTreeMap<String, String>,
) -> Result<(StageInfo, String)> {
    match location.clone() {
        FileLocation::Stage(location) => {
            parse_stage_location_v2(ctx, &location.name, &location.path).await
        }
        FileLocation::Uri(uri) => {
            let mut location = UriLocation::from_uri(uri, "".to_string(), connection)?;
            let (storage_params, path) = parse_uri_location(&mut location)?;
            if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
                Err(ErrorCode::StorageInsecure(
                    "copy from insecure storage is not allowed",
                ))
            } else {
                let stage_info = StageInfo::new_external_stage(storage_params, &path);
                Ok((stage_info, path))
            }
        }
    }
}
