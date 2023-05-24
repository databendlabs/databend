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
use common_ast::parser::parser_values_with_placeholder;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::StageAttachment;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Scalar;
use common_meta_app::principal::FileFormatOptionsAst;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageInfo;
use common_storage::StageFilesInfo;
use common_users::UserApiProvider;
use parking_lot::RwLock;
use tracing::debug;

use crate::binder::location::parse_uri_location;
use crate::binder::Binder;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyIntoTablePlan;
use crate::plans::CopyPlan;
use crate::plans::Plan;
use crate::plans::ValidationMode;
use crate::BindContext;
use crate::Metadata;
use crate::NameResolutionContext;

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

                let table = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await?;

                let required_values_schema: DataSchemaRef = Arc::new(
                    match columns {
                        Some(cols) => self.schema_project(&table.schema(), cols)?,
                        None => table.schema(),
                    }
                    .into(),
                );

                let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
                    .map_err(ErrorCode::SyntaxException)?;

                let stage_schema = infer_table_schema(&required_values_schema)?;

                let plan = CopyIntoTablePlan {
                    catalog_name,
                    database_name,
                    table_name,
                    validation_mode,
                    force: stmt.force,
                    stage_table_info: StageTableInfo {
                        schema: stage_schema,
                        files_info,
                        stage_info,
                        files_to_copy: None,
                    },
                    values_consts: vec![],
                    required_source_schema: required_values_schema.clone(),
                    required_values_schema: required_values_schema.clone(),
                    write_mode: CopyIntoTableMode::Copy,
                    query: None,
                };

                self.bind_copy_into_table_from_location(bind_context, plan)
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
                let validation_mode = ValidationMode::from_str(stmt.validation_mode.as_str())
                    .map_err(ErrorCode::SyntaxException)?;

                let table = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await?;

                let required_values_schema: DataSchemaRef = Arc::new(
                    match columns {
                        Some(cols) => self.schema_project(&table.schema(), cols)?,
                        None => table.schema(),
                    }
                    .into(),
                );
                let stage_schema = infer_table_schema(&required_values_schema)?;

                let plan = CopyIntoTablePlan {
                    catalog_name,
                    database_name,
                    table_name,
                    validation_mode,
                    force: stmt.force,
                    stage_table_info: StageTableInfo {
                        schema: stage_schema,
                        files_info,
                        stage_info,
                        files_to_copy: None,
                    },
                    values_consts: vec![],
                    required_source_schema: required_values_schema.clone(),
                    required_values_schema: required_values_schema.clone(),
                    write_mode: CopyIntoTableMode::Copy,
                    query: None,
                };

                self.bind_copy_into_table_from_location(bind_context, plan)
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
                let (mut stage_info, path) =
                    parse_file_location(&self.ctx, location, BTreeMap::new()).await?;
                self.apply_stage_options(stmt, &mut stage_info).await?;
                let files_info = StageFilesInfo {
                    path,
                    pattern: stmt.pattern.clone(),
                    files: stmt.files.clone(),
                };

                let table = self
                    .ctx
                    .get_table(&catalog_name, &database_name, &table_name)
                    .await?;

                let required_values_schema: DataSchemaRef = Arc::new(
                    match columns {
                        Some(cols) => self.schema_project(&table.schema(), cols)?,
                        None => table.schema(),
                    }
                    .into(),
                );

                let plan = CopyIntoTablePlan {
                    catalog_name,
                    database_name,
                    table_name,
                    required_source_schema: required_values_schema.clone(),
                    required_values_schema: required_values_schema.clone(),
                    values_consts: vec![],
                    force: stmt.force,
                    stage_table_info: StageTableInfo {
                        schema: infer_table_schema(&required_values_schema)?,
                        files_info,
                        stage_info,
                        files_to_copy: None,
                    },
                    write_mode: CopyIntoTableMode::Copy,
                    query: None,
                    validation_mode: ValidationMode::None,
                };
                self.bind_copy_from_query_into_table(bind_context, plan, select_list, alias)
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
    async fn bind_copy_into_table_from_location(
        &mut self,
        bind_ctx: &BindContext,
        plan: CopyIntoTablePlan,
    ) -> Result<Plan> {
        if matches!(
            plan.stage_table_info.stage_info.file_format_params,
            FileFormatParams::Parquet(_)
        ) {
            let select_list = plan
                .required_source_schema
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

            self.bind_copy_from_query_into_table(bind_ctx, plan, &select_list, &None)
                .await
        } else {
            Ok(Plan::Copy(Box::new(CopyPlan::IntoTable(plan))))
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_attachment(
        &mut self,
        attachment: StageAttachment,
    ) -> Result<(StageInfo, StageFilesInfo)> {
        let (mut stage_info, path) = parse_stage_location(&self.ctx, &attachment.location).await?;

        if let Some(ref options) = attachment.file_format_options {
            stage_info.file_format_params = FileFormatOptionsAst {
                options: options.clone(),
            }
            .try_into()?;
        }
        if let Some(ref options) = attachment.copy_options {
            stage_info.copy_options.apply(options, true)?;
        }

        let files_info = StageFilesInfo {
            path,
            files: None,
            pattern: None,
        };
        Ok((stage_info, files_info))
    }

    /// Bind COPY INFO <table> FROM <stage_location>
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub(crate) async fn bind_copy_from_attachment(
        &mut self,
        bind_context: &mut BindContext,
        attachment: StageAttachment,
        catalog_name: String,
        database_name: String,
        table_name: String,
        required_values_schema: DataSchemaRef,
        values_str: &str,
        write_mode: CopyIntoTableMode,
    ) -> Result<Plan> {
        let (data_schema, const_columns) = if values_str.is_empty() {
            (required_values_schema.clone(), vec![])
        } else {
            self.prepared_values(values_str, &required_values_schema)
                .await?
        };

        let (mut stage_info, files_info) = self.bind_attachment(attachment).await?;
        stage_info.copy_options.purge = true;

        let stage_schema = infer_table_schema(&data_schema)?;

        let plan = CopyIntoTablePlan {
            catalog_name,
            database_name,
            table_name,
            required_source_schema: data_schema.clone(),
            required_values_schema,
            values_consts: const_columns,
            force: true,
            stage_table_info: StageTableInfo {
                schema: stage_schema,
                files_info,
                stage_info,
                files_to_copy: None,
            },
            write_mode,
            query: None,
            validation_mode: ValidationMode::None,
        };

        self.bind_copy_into_table_from_location(bind_context, plan)
            .await
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
        mut plan: CopyIntoTablePlan,
        select_list: &'a [SelectTarget],
        alias: &Option<TableAlias>,
    ) -> Result<Plan> {
        let need_copy_file_infos = plan.collect_files(&self.ctx).await?;

        if need_copy_file_infos.is_empty() {
            return Ok(Plan::Copy(Box::new(CopyPlan::NoFileToCopy)));
        }

        plan.stage_table_info.files_to_copy = Some(need_copy_file_infos.clone());

        let (s_expr, mut from_context) = self
            .bind_stage_table(
                bind_context,
                plan.stage_table_info.stage_info.clone(),
                plan.stage_table_info.files_info.clone(),
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

        plan.query = Some(Box::new(Plan::Query {
            s_expr: Box::new(s_expr),
            metadata: self.metadata.clone(),
            bind_context: Box::new(output_context),
            rewrite_kind: None,
            ignore_result: false,
            formatted_ast: None,
        }));
        Ok(Plan::Copy(Box::new(CopyPlan::IntoTable(plan))))
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

    #[async_backtrace::framed]
    pub(crate) async fn prepared_values(
        &self,
        values_str: &str,
        source_schema: &DataSchemaRef,
    ) -> Result<(DataSchemaRef, Vec<Scalar>)> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let tokens = tokenize_sql(values_str)?;
        let expr_or_placeholders = parser_values_with_placeholder(&tokens, sql_dialect)?;

        if source_schema.num_fields() != expr_or_placeholders.len() {
            return Err(ErrorCode::SemanticError(format!(
                "need {} fields in values, got only {}",
                source_schema.num_fields(),
                expr_or_placeholders.len()
            )));
        }

        let mut attachment_fields = vec![];
        let mut const_fields = vec![];
        let mut exprs = vec![];
        for (i, eo) in expr_or_placeholders.into_iter().enumerate() {
            match eo {
                Some(e) => {
                    exprs.push(e);
                    const_fields.push(source_schema.fields()[i].clone());
                }
                None => attachment_fields.push(source_schema.fields()[i].clone()),
            }
        }
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let const_schema = Arc::new(DataSchema::new(const_fields));
        let const_values = bind_context
            .exprs_to_scalar(
                exprs,
                &const_schema,
                self.ctx.clone(),
                &name_resolution_ctx,
                metadata,
            )
            .await?;
        Ok((Arc::new(DataSchema::new(attachment_fields)), const_values))
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
