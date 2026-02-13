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

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnID as AstColumnID;
use databend_common_ast::ast::ColumnMatchMode;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::CopyIntoTableOptions;
use databend_common_ast::ast::CopyIntoTableSource;
use databend_common_ast::ast::CopyIntoTableStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::Hint;
use databend_common_ast::ast::HintItem;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::LiteralStringOrVariable;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TypeName;
use databend_common_ast::parser::parse_values;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::plan::list_stage_files;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::shrink_scalar;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_meta_app::principal::FileFormatOptionsReader;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::NullAs;
use databend_common_meta_app::principal::StageInfo;
use databend_common_settings::Settings;
use databend_common_storage::StageFilesInfo;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::table::OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH;
use databend_storages_common_table_meta::table::OPT_KEY_ENABLE_SCHEMA_EVOLUTION;
use derive_visitor::Drive;
use log::LevelFilter;
use log::debug;
use log::warn;
use parking_lot::RwLock;

use crate::BindContext;
use crate::DefaultExprBinder;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::binder::Binder;
use crate::binder::bind_query::MaxColumnPosition;
use crate::binder::insert::STAGE_PLACEHOLDER;
use crate::binder::location::parse_uri_location;
use crate::plans::CopyIntoTableMode;
use crate::plans::CopyIntoTablePlan;
use crate::plans::Plan;
use crate::plans::ValidationMode;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_copy_into_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CopyIntoTableStmt,
    ) -> Result<Plan> {
        match &stmt.src {
            CopyIntoTableSource::Location(location) => {
                let mut plan = self
                    .bind_copy_into_table_common(stmt, location, false)
                    .await?;

                // for copy from location, collect files explicitly
                plan.collect_files(self.ctx.as_ref()).await?;
                self.bind_copy_into_table_from_location(bind_context, plan)
                    .await
            }
            CopyIntoTableSource::Query {
                select_list,
                from,
                alias_name,
            } => {
                let mut max_column_position = MaxColumnPosition::new();
                select_list.drive(&mut max_column_position);
                self.metadata
                    .write()
                    .set_max_column_position(max_column_position.max_pos);
                let plan = self.bind_copy_into_table_common(stmt, from, true).await?;

                let alias = alias_name.as_ref().map(|name| TableAlias {
                    name: name.clone(),
                    columns: vec![],
                    keep_database_name: false,
                });
                self.bind_copy_from_query_into_table(bind_context, plan, select_list, &alias)
                    .await
            }
        }
    }

    pub(crate) fn resolve_copy_pattern(
        ctx: Arc<dyn TableContext>,
        pattern: &LiteralStringOrVariable,
    ) -> Result<String> {
        match pattern {
            LiteralStringOrVariable::Literal(s) => Ok(s.clone()),
            LiteralStringOrVariable::Variable(var_name) => {
                let var_value = ctx.get_variable(var_name).unwrap_or(Scalar::Null);
                let var_value = shrink_scalar(var_value);
                if let Scalar::String(s) = var_value {
                    Ok(s)
                } else {
                    Err(ErrorCode::BadArguments(format!(
                        "invalid pattern expr: {var_value}"
                    )))
                }
            }
        }
    }

    async fn bind_copy_into_table_common(
        &mut self,
        stmt: &CopyIntoTableStmt,
        location: &FileLocation,
        is_transform: bool,
    ) -> Result<CopyIntoTablePlan> {
        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(&stmt.catalog, &stmt.database, &stmt.table);
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let catalog_info = catalog.info();
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let dedup_full_path = table
            .get_table_info()
            .get_option(OPT_KEY_ENABLE_COPY_DEDUP_FULL_PATH, false);

        let enable_schema_evolution = table
            .get_table_info()
            .get_option(OPT_KEY_ENABLE_SCHEMA_EVOLUTION, false);

        let validation_mode = ValidationMode::from_str(stmt.options.validation_mode.as_str())
            .map_err(ErrorCode::SyntaxException)?;

        let (mut stage_info, path) = resolve_file_location(self.ctx.as_ref(), location).await?;
        if !stmt.file_format.is_empty() {
            stage_info.file_format_params = self.try_resolve_file_format(&stmt.file_format).await?;
        }
        let mut options = stmt.options.clone();
        stage_info
            .file_format_params
            .check_copy_options(&mut options)?;

        if !(stmt.options.purge && stmt.options.force)
            && stmt.options.max_files > COPY_MAX_FILES_PER_COMMIT
        {
            return Err(ErrorCode::InvalidArgument(format!(
                "max_files {} is too large, max_files should be less than {COPY_MAX_FILES_PER_COMMIT}",
                stmt.options.max_files
            )));
        }
        let pattern = match &stmt.pattern {
            None => None,
            Some(pattern) => Some(Self::resolve_copy_pattern(self.ctx.clone(), pattern)?),
        };

        let files_info = StageFilesInfo {
            path,
            files: stmt.files.clone(),
            pattern,
        };

        let dest_entity_name = format!("{database_name}.{table_name}");
        let stage_schema = match &stmt.dst_columns {
            Some(cols) => self.schema_project(&table.schema(), cols, &dest_entity_name)?,
            None => self.schema_project(&table.schema(), &[], &dest_entity_name)?,
        };

        let required_values_schema: DataSchemaRef = Arc::new(stage_schema.clone().into());

        let default_values = if stage_info.file_format_params.need_field_default() {
            Some(
                DefaultExprBinder::try_new(self.ctx.clone())?
                    .prepare_default_values(&required_values_schema)?,
            )
        } else {
            None
        };

        Ok(CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            validation_mode,
            is_transform,
            dedup_full_path,
            enable_schema_evolution,
            path_prefix: None,
            no_file_to_copy: false,
            from_attachment: false,
            stage_table_info: StageTableInfo {
                schema: stage_schema,
                files_info,
                stage_info,
                is_select: false,
                default_exprs: default_values,
                copy_into_table_options: stmt.options.clone(),
                is_variant: false,
                ..Default::default()
            },
            values_consts: vec![],
            required_source_schema: required_values_schema.clone(),
            required_values_schema: required_values_schema.clone(),
            write_mode: CopyIntoTableMode::Copy,
            query: None,
            enable_distributed: false,
            files_collected: false,
        })
    }

    /// Bind COPY INFO <table> FROM <stage_location>
    #[async_backtrace::framed]
    async fn bind_copy_into_table_from_location(
        &mut self,
        bind_ctx: &mut BindContext,
        plan: CopyIntoTablePlan,
    ) -> Result<Plan> {
        let use_query = !plan.enable_schema_evolution
            && matches!(&plan.stage_table_info.stage_info.file_format_params,
            FileFormatParams::Parquet(fmt) if fmt.missing_field_as == NullAs::Error);

        if use_query {
            let mut select_list = Vec::with_capacity(plan.required_source_schema.num_fields());
            let case_sensitive = plan
                .stage_table_info
                .copy_into_table_options
                .column_match_mode
                == Some(ColumnMatchMode::CaseSensitive);
            for dest_field in plan.required_source_schema.fields().iter() {
                let column = Expr::ColumnRef {
                    span: None,
                    column: ColumnRef {
                        database: None,
                        table: None,
                        column: AstColumnID::Name(Identifier::from_name_with_quoted(
                            None,
                            if case_sensitive {
                                dest_field.name().to_string()
                            } else {
                                dest_field.name().to_lowercase().to_string()
                            },
                            Some('"'),
                        )),
                    },
                };
                // cast types to variant, tuple will be rewrite as `json_object_keep_null`
                let expr = if dest_field.data_type().remove_nullable() == DataType::Variant {
                    Expr::Cast {
                        span: None,
                        expr: Box::new(column),
                        target_type: TypeName::Variant,
                        pg_style: false,
                    }
                } else {
                    column
                };
                select_list.push(SelectTarget::AliasedExpr {
                    expr: Box::new(expr),
                    alias: Some(Identifier::from_name(
                        Span::None,
                        dest_field.name().to_string(),
                    )),
                });
            }

            self.bind_copy_from_query_into_table(bind_ctx, plan, &select_list, &None)
                .await
        } else {
            Ok(Plan::CopyIntoTable(Box::new(plan)))
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_attachment(
        &mut self,
        attachment: StageAttachment,
    ) -> Result<(StageInfo, StageFilesInfo, CopyIntoTableOptions)> {
        let (mut stage_info, path) =
            resolve_stage_location(self.ctx.as_ref(), &attachment.location[1..]).await?;

        if let Some(ref options) = attachment.file_format_options {
            let mut params = FileFormatParams::try_from_reader(
                FileFormatOptionsReader::from_map(options.clone()),
                false,
            )?;
            if let FileFormatParams::Csv(fmt) = &mut params {
                // TODO: remove this after 1. the old server is no longer supported 2. Driver add the option "EmptyFieldAs=FieldDefault"
                // CSV attachment is mainly used in Drivers for insert.
                // In the future, client should use EmptyFieldAs=STRING or FieldDefault to distinguish NULL and empty string.
                // However, old server does not support `empty_field_as`, so client can not add the option directly at now.
                // So we will get empty_field_as = NULL, which will raise error if there is empty string for non-nullable string field.
                if fmt.empty_field_as == EmptyFieldAs::Null {
                    fmt.empty_field_as = EmptyFieldAs::FieldDefault;
                }
            }
            stage_info.file_format_params = params;
        }
        let mut copy_options = CopyIntoTableOptions::default();
        if let Some(ref options) = attachment.copy_options {
            copy_options.apply(options, true)?;
        }
        copy_options.force = true;
        stage_info
            .file_format_params
            .check_copy_options(&mut copy_options)?;

        let files_info = StageFilesInfo {
            path,
            files: None,
            pattern: None,
        };
        Ok((stage_info, files_info, copy_options))
    }

    /// Bind COPY INFO <table> FROM <location>
    /// called by bind_insert
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub(crate) async fn bind_copy_from_attachment(
        &mut self,
        bind_context: &mut BindContext,
        attachment: StageAttachment,
        catalog_name: String,
        database_name: String,
        table_name: String,
        required_values_schema: TableSchemaRef,
        values_str: &str,
        write_mode: CopyIntoTableMode,
    ) -> Result<Plan> {
        let expr_or_placeholders = if values_str.is_empty() {
            None
        } else {
            Some(self.parse_values_str(values_str).await?)
        };

        let (stage_info, files_info, options) = self.bind_attachment(attachment).await?;
        self.bind_copy_from_upload(
            bind_context,
            catalog_name,
            database_name,
            table_name,
            required_values_schema,
            expr_or_placeholders,
            stage_info,
            files_info,
            options,
            write_mode,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub(crate) async fn bind_copy_from_upload(
        &mut self,
        bind_context: &mut BindContext,
        catalog_name: String,
        database_name: String,
        table_name: String,
        required_values_schema: TableSchemaRef,
        expr_or_placeholders: Option<Vec<Expr>>,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        copy_into_table_options: CopyIntoTableOptions,
        write_mode: CopyIntoTableMode,
    ) -> Result<Plan> {
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let catalog_info = catalog.info();
        let settings = self.ctx.get_settings();
        let (required_source_schema, values_consts) = if let Some(exprs) = expr_or_placeholders {
            self.prepared_values(exprs, &required_values_schema, settings.clone())
                .await?
        } else {
            (required_values_schema.clone(), vec![])
        };

        // list the files to be copied in binding phase
        // note that, this method(`bind_copy_from_attachment`) are used by
        // - bind_insert (insert from attachment)
        // - bind_replace only (replace from attachment),
        // currently, they do NOT enforce the deduplication detection rules,
        // as the vanilla Copy-Into does.
        // thus, we do not care about the "duplicated_files_detected", just set it to empty vector.
        let thread_num = settings.get_max_threads()? as usize;
        let files_to_copy = list_stage_files(&stage_info, &files_info, thread_num, None).await?;
        let duplicated_files_detected = vec![];

        let required_values_schema = Arc::new(DataSchema::from(required_values_schema));
        let default_values = DefaultExprBinder::try_new(self.ctx.clone())?
            .prepare_default_values(&required_values_schema)?;

        let plan = CopyIntoTablePlan {
            catalog_info,
            database_name,
            table_name,
            no_file_to_copy: false,
            from_attachment: true,
            required_source_schema: Arc::new(DataSchema::from(&required_source_schema)),
            required_values_schema,
            dedup_full_path: false,
            path_prefix: None,
            values_consts,
            stage_table_info: StageTableInfo {
                schema: required_source_schema,
                files_info,
                stage_info,
                files_to_copy: Some(files_to_copy),
                duplicated_files_detected,
                is_select: false,
                default_exprs: Some(default_values),
                copy_into_table_options,
                stage_root: "".to_string(),
                is_variant: false,
                ..Default::default()
            },
            write_mode,
            query: None,
            validation_mode: ValidationMode::None,

            enable_distributed: false,
            is_transform: false,
            files_collected: true,
            enable_schema_evolution: false,
        };

        self.bind_copy_into_table_from_location(bind_context, plan)
            .await
    }

    /// Bind COPY INTO <table> FROM <query>
    #[async_backtrace::framed]
    async fn bind_copy_from_query_into_table(
        &mut self,
        bind_context: &mut BindContext,
        mut plan: CopyIntoTablePlan,
        select_list: &[SelectTarget],
        alias: &Option<TableAlias>,
    ) -> Result<Plan> {
        plan.collect_files(self.ctx.as_ref()).await?;
        if plan.no_file_to_copy {
            return Ok(Plan::CopyIntoTable(Box::new(plan)));
        }
        let case_sensitive = plan
            .stage_table_info
            .copy_into_table_options
            .column_match_mode
            == Some(ColumnMatchMode::CaseSensitive);

        let table_ctx = self.ctx.clone();
        let (s_expr, mut from_context) = self
            .bind_stage_table(
                table_ctx,
                bind_context,
                plan.stage_table_info.stage_info.clone(),
                plan.stage_table_info.files_info.clone(),
                alias,
                plan.stage_table_info.files_to_copy.clone(),
                case_sensitive,
                Some(
                    plan.stage_table_info
                        .copy_into_table_options
                        .on_error
                        .clone(),
                ),
            )
            .await?;

        // Generate an analyzed select list with from context
        let select_list = self.normalize_select_list(&mut from_context, select_list)?;

        for item in select_list.items.iter() {
            if !self.check_allowed_scalar_expr_with_subquery_for_copy_table(&item.scalar)? {
                // in fact, if there is a join, we will stop in `check_transform_query()`
                return Err(ErrorCode::SemanticError(
                    "copy into <table> can't contain aggregate|flatten|window functions"
                        .to_string(),
                ));
            };
        }
        let (scalar_items, projections) = self.analyze_projection(
            &from_context.aggregate_info,
            &from_context.windows,
            &select_list,
        )?;

        if projections.len() != plan.required_source_schema.num_fields() {
            return Err(ErrorCode::BadArguments(format!(
                "Number of columns in select list ({}) does not match that of the corresponding table ({})",
                projections.len(),
                plan.required_source_schema.num_fields(),
            )));
        }

        let mut s_expr =
            self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;

        // rewrite async function and udf
        s_expr = self.rewrite_udf(&mut from_context, s_expr)?;
        s_expr = self.add_internal_column_into_expr(&mut from_context, s_expr)?;
        s_expr = self.add_virtual_column_into_expr(&mut from_context, s_expr)?;

        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;

        // disable variant check to allow copy invalid JSON into tables
        let disable_variant_check = plan
            .stage_table_info
            .copy_into_table_options
            .disable_variant_check;
        if disable_variant_check {
            let hints = Hint {
                hints_list: vec![HintItem {
                    name: Identifier::from_name(None, "disable_variant_check"),
                    expr: Expr::Literal {
                        span: None,
                        value: Literal::UInt64(1),
                    },
                }],
            };
            if let Some(e) = self.opt_hints_set_var(&mut output_context, &hints).err() {
                warn!(
                    "In COPY resolve optimize hints {:?} failed, err: {:?}",
                    hints, e
                );
            }
        }

        plan.query = Some(Box::new(Plan::Query {
            s_expr: Box::new(s_expr),
            metadata: self.metadata.clone(),
            bind_context: Box::new(output_context),
            rewrite_kind: None,
            ignore_result: false,
            formatted_ast: None,
        }));

        Ok(Plan::CopyIntoTable(Box::new(plan)))
    }

    pub(crate) async fn parse_values_str(&self, values_str: &str) -> Result<Vec<Expr>> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let tokens = tokenize_sql(values_str)?;
        let values = parse_values(&tokens, sql_dialect)?;
        Ok(values)
    }

    #[async_backtrace::framed]
    pub(crate) async fn prepared_values(
        &self,
        expr_or_placeholders: Vec<Expr>,
        source_schema: &TableSchemaRef,
        settings: Arc<Settings>,
    ) -> Result<(TableSchemaRef, Vec<Scalar>)> {
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
                Expr::Placeholder { .. } => {
                    attachment_fields.push(source_schema.fields()[i].clone());
                }
                e => {
                    exprs.push(e);
                    const_fields.push(DataField::from(&source_schema.fields()[i]));
                }
            }
        }
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let const_schema = Arc::new(DataSchema::new(const_fields));
        let const_values = bind_context
            .exprs_to_scalar(
                &exprs,
                &const_schema,
                self.ctx.clone(),
                &name_resolution_ctx,
                metadata,
            )
            .await?;
        Ok((Arc::new(TableSchema::new(attachment_fields)), const_values))
    }
}

/// Named stage(start with `@`):
///
/// ```sql
/// copy into mytable from @my_ext_stage
///     file_format = (type = csv);
/// ```
/// location can be:
/// - mystage
/// - mystage/
/// - mystage/abc
/// - ~/abc
///
/// Returns the stage name and relative path towards the stage's root.
///
/// If input location is empty we will convert it to `/` means the root of stage
///
/// - mystage => (mystage, "/")
///
/// If input location is endswith `/`, it's a folder.
///
/// - mystage/ => (mystage, "/")
///
/// Otherwise, it's a file
///
/// - mystage/abc => (mystage, "abc")
///
/// - ~/abc => ("~", "abc")
pub fn parse_stage_location(location: &str) -> Result<(String, String)> {
    let location = location.trim_start_matches('@');
    let names: Vec<&str> = location.splitn(2, '/').filter(|v| !v.is_empty()).collect();
    if names.is_empty() {
        return Err(ErrorCode::BadArguments(
            "stage path must include a stage name".to_string(),
        ));
    }
    if names[0] == STAGE_PLACEHOLDER {
        return Err(ErrorCode::BadArguments(
            "placeholder @_databend_upload as location: should be used in streaming_load handler or replaced in client.",
        ));
    }

    let path = names.get(1).unwrap_or(&"").trim_start_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    Ok((names[0].to_string(), path.to_string()))
}

pub fn parse_stage_name(location: &str) -> Result<String> {
    if !location.starts_with('@') {
        return Err(ErrorCode::BadArguments(format!(
            "stage path must start with @, but got {}",
            location
        )));
    }
    let stage_name = location.trim_start_matches('@');
    if stage_name.is_empty() {
        return Err(ErrorCode::BadArguments(
            "stage path must include a stage name".to_string(),
        ));
    }
    if stage_name == STAGE_PLACEHOLDER {
        return Err(ErrorCode::BadArguments(
            "placeholder @_databend_upload as location: should be used in streaming_load handler or replaced in client.".to_string(),
        ));
    }
    if stage_name.contains('/') {
        return Err(ErrorCode::BadArguments(format!(
            "stage argument must be a stage name, but got {}",
            location
        )));
    }
    Ok(stage_name.to_string())
}

#[async_backtrace::framed]
pub async fn resolve_stage_location(
    ctx: &dyn TableContext,
    location: &str,
) -> Result<(StageInfo, String)> {
    let (stage_name, path) = parse_stage_location(location)?;

    let mut stage = if stage_name == "~" {
        StageInfo::new_user_stage(&ctx.get_current_user()?.name)
    } else {
        UserApiProvider::instance()
            .get_stage(&ctx.get_tenant(), &stage_name)
            .await?
    };
    if ThreadTracker::capture_log_settings()
        .is_some_and(|settings| settings.level == LevelFilter::Off)
    {
        // History log transform queries use the internal history stage.
        // Enable credential chain at runtime since the flag is not persisted in meta.
        stage.allow_credential_chain = true;
    }

    debug!("parsed stage: {stage:?}, path: {path}");
    Ok((stage, path))
}

#[async_backtrace::framed]
pub async fn resolve_stage_locations(
    ctx: &dyn TableContext,
    locations: &[String],
) -> Result<Vec<(StageInfo, String)>> {
    let mut results = Vec::with_capacity(locations.len());
    for location in locations {
        results.push(resolve_stage_location(ctx, location).await?);
    }
    Ok(results)
}

#[async_backtrace::framed]
pub async fn resolve_file_location(
    ctx: &dyn TableContext,
    location: &FileLocation,
) -> Result<(StageInfo, String)> {
    match location.clone() {
        FileLocation::Stage(location) => resolve_stage_location(ctx, &location).await,
        FileLocation::Uri(mut uri) => {
            let (storage_params, path) = parse_uri_location(&mut uri, Some(ctx)).await?;
            if !storage_params.is_secure() && !GlobalConfig::instance().storage.allow_insecure {
                Err(ErrorCode::StorageInsecure(
                    "copy from insecure storage is not allowed",
                ))
            } else {
                let stage_info = StageInfo::new_external_stage(storage_params, true);
                Ok((stage_info, path))
            }
        }
    }
}
