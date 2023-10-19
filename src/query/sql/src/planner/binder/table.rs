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
use std::collections::HashMap;
use std::default::Default;
use std::str::FromStr;
use std::sync::Arc;

use async_recursion::async_recursion;
use chrono::TimeZone;
use chrono::Utc;
use common_ast::ast::Connection;
use common_ast::ast::FileLocation;
use common_ast::ast::Indirection;
use common_ast::ast::Join;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::Statement;
use common_ast::ast::TableAlias;
use common_ast::ast::TableReference;
use common_ast::ast::TimeTravelPoint;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Dialect;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::StageTableInfo;
use common_catalog::statistics::BasicColumnStatistics;
use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::TableContext;
use common_catalog::table_function::TableFunction;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::ColumnId;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::FunctionKind;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListIndexesReq;
use common_meta_types::MetaId;
use common_storage::DataOperator;
use common_storage::StageFileInfo;
use common_storage::StageFilesInfo;
use common_storages_parquet::Parquet2Table;
use common_storages_parquet::ParquetRSTable;
use common_storages_result_cache::ResultCacheMetaManager;
use common_storages_result_cache::ResultCacheReader;
use common_storages_result_cache::ResultScan;
use common_storages_stage::StageTable;
use common_storages_view::view_table::QUERY;
use common_users::UserApiProvider;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::binder::copy_into_table::resolve_file_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::table_args::bind_table_args;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::binder::ExprContext;
use crate::binder::Visibility;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::TypeChecker;
use crate::plans::CteScan;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnEntry;
use crate::IndexType;

impl Binder {
    #[async_backtrace::framed]
    pub async fn bind_one_table(
        &mut self,
        bind_context: &BindContext,
        select_list: &Vec<SelectTarget>,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in select_list {
            if let SelectTarget::QualifiedName {
                qualified: names, ..
            } = select_target
            {
                for indirect in names {
                    if let Indirection::Star(span) = indirect {
                        return Err(ErrorCode::SemanticError(
                            "SELECT * with no tables specified is not valid".to_string(),
                        )
                        .set_span(*span));
                    }
                }
            }
        }
        let catalog = CATALOG_DEFAULT;
        let database = "system";
        let tenant = self.ctx.get_tenant();
        let table_meta: Arc<dyn Table> = self
            .resolve_data_source(tenant.as_str(), catalog, database, "one", &None)
            .await?;
        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_owned(),
            database.to_string(),
            table_meta,
            None,
            false,
            false,
        );

        self.bind_base_table(bind_context, database, table_index)
            .await
    }

    fn check_view_dep(bind_context: &BindContext, database: &str, view_name: &str) -> Result<()> {
        match &bind_context.parent {
            Some(parent) => match &parent.view_info {
                Some((db, v)) => {
                    if db == database && v == view_name {
                        Err(ErrorCode::Internal(format!(
                            "View dependency loop detected (view: {}.{})",
                            database, view_name
                        )))
                    } else {
                        Self::check_view_dep(parent, database, view_name)
                    }
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }

    #[async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_single_table(
        &mut self,
        bind_context: &mut BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::Table {
                span,
                catalog,
                database,
                table,
                alias,
                travel_point,
                pivot: _,
                unpivot: _,
            } => {
                let (catalog, database, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                let table_alias_name = if let Some(table_alias) = alias {
                    Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
                } else {
                    None
                };
                let mut bind_cte = true;
                if let Some(cte_name) = &bind_context.cte_name {
                    // If table name equals to cte name, then skip bind cte and find table from catalog
                    // Or will dead loop and stack overflow
                    if cte_name == &table_name {
                        bind_cte = false;
                    }
                }
                // Check and bind common table expression
                let ctes_map = self.ctes_map.clone();
                if let Some(cte_info) = ctes_map.get(&table_name) {
                    if bind_cte {
                        return if !cte_info.materialized {
                            self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
                                .await
                        } else {
                            self.bind_m_cte(bind_context, cte_info, &table_name, alias, span)
                                .await
                        };
                    }
                }

                let tenant = self.ctx.get_tenant();

                let navigation_point = match travel_point {
                    Some(tp) => Some(self.resolve_data_travel_point(bind_context, tp).await?),
                    None => None,
                };

                // Resolve table with catalog
                let table_meta = match self
                    .resolve_data_source(
                        tenant.as_str(),
                        catalog.as_str(),
                        database.as_str(),
                        table_name.as_str(),
                        &navigation_point,
                    )
                    .await
                {
                    Ok(table) => table,
                    Err(_) => {
                        let mut parent = bind_context.parent.as_mut();
                        loop {
                            if parent.is_none() {
                                break;
                            }
                            let bind_context = parent.unwrap().as_mut();
                            let ctes_map = self.ctes_map.clone();
                            if let Some(cte_info) = ctes_map.get(&table_name) {
                                return if !cte_info.materialized {
                                    self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
                                        .await
                                } else {
                                    self.bind_m_cte(
                                        bind_context,
                                        cte_info,
                                        &table_name,
                                        alias,
                                        span,
                                    )
                                    .await
                                };
                            }
                            parent = bind_context.parent.as_mut();
                        }
                        return Err(ErrorCode::UnknownTable(format!(
                            "Unknown table `{database}`.`{table_name}` in catalog '{catalog}'"
                        ))
                        .set_span(*span));
                    }
                };

                match table_meta.engine() {
                    "VIEW" => {
                        Self::check_view_dep(bind_context, &database, &table_name)?;
                        let query = table_meta
                            .options()
                            .get(QUERY)
                            .ok_or_else(|| ErrorCode::Internal("Invalid VIEW object"))?;
                        let tokens = tokenize_sql(query.as_str())?;
                        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
                        // For view, we need use a new context to bind it.
                        let mut new_bind_context =
                            BindContext::with_parent(Box::new(bind_context.clone()));
                        new_bind_context.view_info =
                            Some((database.clone(), table_name.to_string()));
                        if let Statement::Query(query) = &stmt {
                            self.metadata.write().add_table(
                                catalog,
                                database.clone(),
                                table_meta,
                                table_alias_name,
                                false,
                                false,
                            );
                            let (s_expr, mut new_bind_context) =
                                self.bind_query(&mut new_bind_context, query).await?;
                            if let Some(alias) = alias {
                                // view maybe has alias, e.g. select v1.col1 from v as v1;
                                new_bind_context
                                    .apply_table_alias(alias, &self.name_resolution_ctx)?;
                            } else {
                                // e.g. select v0.c0 from v0;
                                for column in new_bind_context.columns.iter_mut() {
                                    column.database_name = None;
                                    column.table_name = Some(
                                        normalize_identifier(table, &self.name_resolution_ctx).name,
                                    );
                                }
                            }
                            Ok((s_expr, new_bind_context))
                        } else {
                            Err(ErrorCode::Internal(format!(
                                "Invalid VIEW object: {}",
                                table_meta.name()
                            ))
                            .set_span(*span))
                        }
                    }
                    _ => {
                        let table_index = self.metadata.write().add_table(
                            catalog,
                            database.clone(),
                            table_meta,
                            table_alias_name,
                            bind_context.view_info.is_some(),
                            bind_context.planning_agg_index,
                        );

                        let (s_expr, mut bind_context) = self
                            .bind_base_table(bind_context, database.as_str(), table_index)
                            .await?;
                        if let Some(alias) = alias {
                            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                        }
                        Ok((s_expr, bind_context))
                    }
                }
            }
            TableReference::TableFunction {
                span,
                name,
                params,
                named_params,
                alias,
            } => {
                let mut scalar_binder = ScalarBinder::new(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    self.m_cte_bound_ctx.clone(),
                    self.ctes_map.clone(),
                );
                let table_args = bind_table_args(&mut scalar_binder, params, named_params).await?;

                let func_name = normalize_identifier(name, &self.name_resolution_ctx);

                if func_name.name.eq_ignore_ascii_case("result_scan") {
                    let query_id = parse_result_scan_args(&table_args)?;
                    if query_id.is_empty() {
                        return Err(ErrorCode::InvalidArgument(
                            "query_id must be specified when using `RESULT_SCAN`",
                        )
                        .set_span(*span));
                    }
                    let kv_store = UserApiProvider::instance().get_meta_store_client();
                    let meta_key = self.ctx.get_result_cache_key(&query_id);
                    if meta_key.is_none() {
                        return Err(ErrorCode::EmptyData(format!(
                            "`RESULT_SCAN` could not find related cache key in current session for this query id: {query_id}"
                        )).set_span(*span));
                    }
                    let result_cache_mgr = ResultCacheMetaManager::create(kv_store, 0);
                    let meta_key = meta_key.unwrap();
                    let (table_schema, block_raw_data) = match result_cache_mgr
                        .get(meta_key.clone())
                        .await?
                    {
                        Some(value) => {
                            let op = DataOperator::instance().operator();
                            ResultCacheReader::read_table_schema_and_data(op, &value.location)
                                .await?
                        }
                        None => {
                            return Err(ErrorCode::EmptyData(format!(
                                "`RESULT_SCAN` could not fetch cache value, maybe the data has touched ttl and was cleaned up.\n\
                            query id: {query_id}, cache key: {meta_key}"
                            )).set_span(*span));
                        }
                    };
                    let table = ResultScan::try_create(table_schema, query_id, block_raw_data)?;

                    let table_alias_name = if let Some(table_alias) = alias {
                        Some(
                            normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name,
                        )
                    } else {
                        None
                    };

                    let table_index = self.metadata.write().add_table(
                        CATALOG_DEFAULT.to_string(),
                        "system".to_string(),
                        table.clone(),
                        table_alias_name,
                        false,
                        false,
                    );

                    let (s_expr, mut bind_context) = self
                        .bind_base_table(bind_context, "system", table_index)
                        .await?;
                    if let Some(alias) = alias {
                        bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    }
                    return Ok((s_expr, bind_context));
                }

                if BUILTIN_FUNCTIONS
                    .get_property(&func_name.name)
                    .map(|p| p.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    // If it is a set-returning function, we bind it as a subquery.
                    let mut bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
                    let stmt = SelectStmt {
                        span: *span,
                        hints: None,
                        distinct: false,
                        select_list: vec![SelectTarget::AliasedExpr {
                            expr: Box::new(common_ast::ast::Expr::FunctionCall {
                                span: *span,
                                distinct: false,
                                name: common_ast::ast::Identifier {
                                    span: *span,
                                    name: func_name.name.clone(),
                                    quote: None,
                                },
                                params: vec![],
                                args: params.clone(),
                                window: None,
                                lambda: None,
                            }),
                            alias: None,
                        }],
                        from: vec![],
                        selection: None,
                        group_by: None,
                        having: None,
                        window_list: None,
                    };
                    self.bind_select_stmt(&mut bind_context, &stmt, &[], 0)
                        .await
                } else {
                    // Other table functions always reside is default catalog
                    let table_meta: Arc<dyn TableFunction> =
                        self.catalogs
                            .get_default_catalog()?
                            .get_table_function(&func_name.name, table_args)?;
                    let table = table_meta.as_table();
                    let table_alias_name = if let Some(table_alias) = alias {
                        Some(
                            normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name,
                        )
                    } else {
                        None
                    };
                    let table_index = self.metadata.write().add_table(
                        CATALOG_DEFAULT.to_string(),
                        "system".to_string(),
                        table.clone(),
                        table_alias_name,
                        false,
                        false,
                    );

                    let (s_expr, mut bind_context) = self
                        .bind_base_table(bind_context, "system", table_index)
                        .await?;
                    if let Some(alias) = alias {
                        bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    }
                    Ok((s_expr, bind_context))
                }
            }
            TableReference::Subquery {
                span: _,
                subquery,
                alias,
            } => {
                // For subquery, we need use a new context to bind it.
                let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
                let (s_expr, mut res_bind_context) =
                    self.bind_query(&mut new_bind_context, subquery).await?;
                if let Some(alias) = alias {
                    res_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    // reset column name as alias column name
                    for i in 0..alias.columns.len() {
                        let column = &res_bind_context.columns[i];
                        self.metadata
                            .write()
                            .change_derived_column_alias(column.index, column.column_name.clone());
                    }
                }
                Ok((s_expr, res_bind_context))
            }
            TableReference::Location {
                span: _,
                location,
                options,
                alias,
            } => {
                let location = match location {
                    FileLocation::Uri(uri) => FileLocation::Uri(UriLocation {
                        connection: Connection::new(options.connection.clone()),
                        ..uri.clone()
                    }),
                    _ => location.clone(),
                };
                let (mut stage_info, path) = resolve_file_location(&self.ctx, &location).await?;
                if let Some(f) = &options.file_format {
                    stage_info.file_format_params = match StageFileFormatType::from_str(f) {
                        Ok(t) => FileFormatParams::default_by_type(t)?,
                        _ => self.ctx.get_file_format(f).await?,
                    }
                }
                let files_info = StageFilesInfo {
                    path,
                    pattern: options.pattern.clone(),
                    files: options.files.clone(),
                };
                let table_ctx = self.ctx.clone();
                self.bind_stage_table(table_ctx, bind_context, stage_info, files_info, alias, None)
                    .await
            }
            TableReference::Join { join, .. } => {
                let (left_expr, left_bind_ctx) =
                    self.bind_table_reference(bind_context, &join.left).await?;
                let (right_expr, right_bind_ctx) =
                    self.bind_table_reference(bind_context, &join.right).await?;
                self.bind_join(
                    bind_context,
                    left_bind_ctx,
                    right_bind_ctx,
                    left_expr,
                    right_expr,
                    join,
                )
                .await
            }
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_stage_table(
        &mut self,
        table_ctx: Arc<dyn TableContext>,
        bind_context: &BindContext,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        alias: &Option<TableAlias>,
        files_to_copy: Option<Vec<StageFileInfo>>,
    ) -> Result<(SExpr, BindContext)> {
        let table = match stage_info.file_format_params {
            FileFormatParams::Parquet(..) => {
                let use_parquet2 = table_ctx.get_settings().get_use_parquet2()?;
                let mut read_options = ParquetReadOptions::default();

                if !table_ctx.get_settings().get_enable_parquet_page_index()? {
                    read_options = read_options.with_prune_pages(false);
                }

                if !table_ctx
                    .get_settings()
                    .get_enable_parquet_rowgroup_pruning()?
                {
                    read_options = read_options.with_prune_row_groups(false);
                }

                if !table_ctx.get_settings().get_enable_parquet_prewhere()? {
                    read_options = read_options.with_do_prewhere(false);
                }

                if use_parquet2 {
                    Parquet2Table::create(
                        table_ctx.clone(),
                        stage_info.clone(),
                        files_info,
                        read_options,
                        files_to_copy,
                    )
                    .await?
                } else {
                    ParquetRSTable::create(
                        table_ctx.clone(),
                        stage_info.clone(),
                        files_info,
                        read_options,
                        files_to_copy,
                    )
                    .await?
                }
            }
            FileFormatParams::NdJson(..) => {
                let schema = Arc::new(TableSchema::new(vec![TableField::new(
                    "_$1", // TODO: this name should be in visible
                    TableDataType::Variant,
                )]));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy: None,
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)?
            }
            FileFormatParams::Csv(..) | FileFormatParams::Tsv(..) => {
                let max_column_position = self.metadata.read().get_max_column_position();
                if max_column_position == 0 {
                    return Err(ErrorCode::SemanticError(
                        "select columns from csv file must in the form of $<column_position>",
                    ));
                }

                let mut fields = vec![];
                for i in 1..(max_column_position + 1) {
                    fields.push(TableField::new(
                        &format!("_${}", i),
                        TableDataType::Nullable(Box::new(TableDataType::String)),
                    ));
                }

                let schema = Arc::new(TableSchema::new(fields));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy: None,
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)?
            }
            _ => {
                return Err(ErrorCode::Unimplemented(
                    "query stage files only support parquet/NDJson/CSV/TSV format for now",
                ));
            }
        };

        let table_alias_name = if let Some(table_alias) = alias {
            Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
        } else {
            None
        };

        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_string(),
            "system".to_string(),
            table.clone(),
            table_alias_name,
            false,
            false,
        );

        let (s_expr, mut bind_context) = self
            .bind_base_table(bind_context, "system", table_index)
            .await?;
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }
        Ok((s_expr, bind_context))
    }

    #[async_backtrace::framed]
    pub async fn bind_table_reference(
        &mut self,
        bind_context: &mut BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        let mut current_ref = table_ref;
        let current_ctx = bind_context;

        // Stack to keep track of the joins
        let mut join_stack: Vec<&Join> = Vec::new();

        // Traverse the table reference hierarchy to get to the innermost table
        while let TableReference::Join { join, .. } = current_ref {
            join_stack.push(join);

            // Check whether the right-hand side is a Join or a TableReference
            match &*join.right {
                TableReference::Join { .. } => {
                    // Traverse the right-hand side if the right-hand side is a Join
                    current_ref = &join.right;
                }
                _ => {
                    // Traverse the left-hand side if the right-hand side is a TableReference
                    current_ref = &join.left;
                }
            }
        }

        // Bind the innermost table
        // current_ref must be left table in its join
        let (mut result_expr, mut result_ctx) =
            self.bind_single_table(current_ctx, current_ref).await?;

        for join in join_stack.iter().rev() {
            match &*join.right {
                TableReference::Join { .. } => {
                    let (left_expr, left_ctx) =
                        self.bind_single_table(current_ctx, &join.left).await?;
                    let (join_expr, ctx) = self
                        .bind_join(
                            current_ctx,
                            left_ctx,
                            result_ctx,
                            left_expr,
                            result_expr,
                            join,
                        )
                        .await?;
                    result_expr = join_expr;
                    result_ctx = ctx;
                }
                _ => {
                    let (right_expr, right_ctx) =
                        self.bind_single_table(current_ctx, &join.right).await?;
                    let (join_expr, ctx) = self
                        .bind_join(
                            current_ctx,
                            result_ctx,
                            right_ctx,
                            result_expr,
                            right_expr,
                            join,
                        )
                        .await?;
                    result_expr = join_expr;
                    result_ctx = ctx;
                }
            }
        }

        Ok((result_expr, result_ctx))
    }

    fn bind_cte_scan(&mut self, cte_info: &CteInfo) -> Result<SExpr> {
        let blocks = Arc::new(RwLock::new(vec![]));
        self.ctx
            .set_materialized_cte((cte_info.cte_idx, cte_info.used_count), blocks)?;
        // Get the fields in the cte
        let mut fields = vec![];
        let mut offsets = vec![];
        for (idx, column) in cte_info.columns.iter().enumerate() {
            fields.push(DataField::new(
                column.index.to_string().as_str(),
                *column.data_type.clone(),
            ));
            offsets.push(idx);
        }
        let cte_scan = SExpr::create_leaf(Arc::new(
            CteScan {
                cte_idx: (cte_info.cte_idx, cte_info.used_count),
                fields,
                // It is safe to unwrap here because we have checked that the cte is materialized.
                offsets,
                stat: cte_info.stat_info.clone().unwrap(),
            }
            .into(),
        ));
        Ok(cte_scan)
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_cte(
        &mut self,
        span: Span,
        bind_context: &mut BindContext,
        table_name: &str,
        alias: &Option<TableAlias>,
        cte_info: &CteInfo,
    ) -> Result<(SExpr, BindContext)> {
        let mut new_bind_context = BindContext {
            parent: Some(Box::new(bind_context.clone())),
            bound_internal_columns: BTreeMap::new(),
            columns: vec![],
            aggregate_info: Default::default(),
            windows: Default::default(),
            lambda_info: Default::default(),
            cte_name: Some(table_name.to_string()),
            cte_map_ref: Box::default(),
            in_grouping: false,
            view_info: None,
            srfs: Default::default(),
            expr_context: ExprContext::default(),
            planning_agg_index: false,
            allow_internal_columns: true,
            window_definitions: DashMap::new(),
        };

        let (s_expr, mut res_bind_context) = self
            .bind_query(&mut new_bind_context, &cte_info.query)
            .await?;
        let mut cols_alias = cte_info.columns_alias.clone();
        if let Some(alias) = alias {
            for (idx, col_alias) in alias.columns.iter().enumerate() {
                if idx < cte_info.columns_alias.len() {
                    cols_alias[idx] = col_alias.name.clone();
                } else {
                    cols_alias.push(col_alias.name.clone());
                }
            }
        }
        let alias_table_name = alias
            .as_ref()
            .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| table_name.to_string());
        for column in res_bind_context.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(alias_table_name.clone());
        }

        if cols_alias.len() > res_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                res_bind_context.columns.len(),
                cols_alias.len()
            ))
            .set_span(span));
        }
        for (index, column_name) in cols_alias.iter().enumerate() {
            res_bind_context.columns[index].column_name = column_name.clone();
        }
        Ok((s_expr, res_bind_context))
    }

    // Bind materialized cte
    #[async_backtrace::framed]
    pub(crate) async fn bind_m_cte(
        &mut self,
        bind_context: &mut BindContext,
        cte_info: &CteInfo,
        table_name: &String,
        alias: &Option<TableAlias>,
        span: &Span,
    ) -> Result<(SExpr, BindContext)> {
        let new_bind_context = if cte_info.used_count == 0 {
            let (cte_s_expr, cte_bind_ctx) = self
                .bind_cte(*span, bind_context, table_name, alias, cte_info)
                .await?;
            let stat_info = RelExpr::with_s_expr(&cte_s_expr).derive_cardinality()?;
            self.ctes_map
                .entry(table_name.clone())
                .and_modify(|cte_info| {
                    cte_info.stat_info = Some(stat_info);
                    cte_info.columns = cte_bind_ctx.columns.clone();
                });
            self.set_m_cte_bound_ctx(cte_info.cte_idx, cte_bind_ctx.clone());
            self.set_m_cte_bound_s_expr(cte_info.cte_idx, cte_s_expr);
            cte_bind_ctx
        } else {
            // If the cte has been bound, get the bound context from `Binder`'s `m_cte_bound_ctx`
            let mut bound_ctx = self.m_cte_bound_ctx.get(&cte_info.cte_idx).unwrap().clone();
            // Resolve the alias name for the bound cte.
            let alias_table_name = alias
                .as_ref()
                .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
                .unwrap_or_else(|| table_name.to_string());
            for column in bound_ctx.columns.iter_mut() {
                column.database_name = None;
                column.table_name = Some(alias_table_name.clone());
            }
            // Pass parent to bound_ctx
            bound_ctx.parent = bind_context.parent.clone();
            bound_ctx
        };
        // `bind_context` is the main BindContext for the whole query
        // Update the `used_count` which will be used in runtime phase
        self.ctes_map
            .entry(table_name.clone())
            .and_modify(|cte_info| {
                cte_info.used_count += 1;
            });
        let cte_info = self.ctes_map.get(table_name).unwrap().clone();
        let s_expr = self.bind_cte_scan(&cte_info)?;
        Ok((s_expr, new_bind_context))
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_base_table(
        &mut self,
        bind_context: &BindContext,
        database_name: &str,
        table_index: IndexType,
    ) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
        let columns = self.metadata.read().columns_by_table_index(table_index);
        let table = self.metadata.read().table(table_index).clone();
        let statistics_provider = table.table().column_statistics_provider().await?;
        let mut col_stats: HashMap<IndexType, Option<BasicColumnStatistics>> = HashMap::new();
        for column in columns.iter() {
            match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_name,
                    column_index,
                    path_indices,
                    data_type,
                    leaf_index,
                    table_index,
                    column_position,
                    virtual_computed_expr,
                    ..
                }) => {
                    let column_binding = ColumnBindingBuilder::new(
                        column_name.clone(),
                        *column_index,
                        Box::new(DataType::from(data_type)),
                        if path_indices.is_some() {
                            Visibility::InVisible
                        } else {
                            Visibility::Visible
                        },
                    )
                    .table_name(Some(table.name().to_string()))
                    .database_name(Some(database_name.to_string()))
                    .table_index(Some(*table_index))
                    .column_position(*column_position)
                    .virtual_computed_expr(virtual_computed_expr.clone())
                    .build();
                    bind_context.add_column_binding(column_binding);
                    if path_indices.is_none() && virtual_computed_expr.is_none() {
                        if let Some(col_id) = *leaf_index {
                            let col_stat =
                                statistics_provider.column_statistics(col_id as ColumnId);
                            col_stats.insert(*column_index, col_stat.cloned());
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::Internal("Invalid column entry"));
                }
            }
        }

        let stat = table.table().table_statistics()?;

        Ok((
            SExpr::create_leaf(Arc::new(
                Scan {
                    table_index,
                    columns: columns.into_iter().map(|col| col.index()).collect(),
                    statistics: Statistics {
                        statistics: stat,
                        col_stats,
                    },
                    ..Default::default()
                }
                .into(),
            )),
            bind_context,
        ))
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_data_source(
        &self,
        tenant: &str,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        travel_point: &Option<NavigationPoint>,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let catalog = self.catalogs.get_catalog(tenant, catalog_name).await?;
        let mut table_meta = catalog.get_table(tenant, database_name, table_name).await?;

        if let Some(tp) = travel_point {
            table_meta = table_meta.navigate_to(tp).await?;
        }
        Ok(table_meta)
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_data_travel_point(
        &self,
        bind_context: &mut BindContext,
        travel_point: &TimeTravelPoint,
    ) -> Result<NavigationPoint> {
        match travel_point {
            TimeTravelPoint::Snapshot(s) => Ok(NavigationPoint::SnapshotID(s.to_owned())),
            TimeTravelPoint::Timestamp(expr) => {
                let mut type_checker = TypeChecker::new(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    false,
                    false,
                );
                let box (scalar, _) = type_checker.resolve(expr).await?;
                let scalar_expr = scalar.as_expr()?;

                let (new_expr, _) = ConstantFolder::fold(
                    &scalar_expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );

                match new_expr {
                    common_expression::Expr::Constant {
                        scalar, data_type, ..
                    } if data_type == DataType::Timestamp => {
                        let value = scalar.as_timestamp().unwrap();
                        Ok(NavigationPoint::TimePoint(
                            Utc.timestamp_nanos(*value * 1000),
                        ))
                    }

                    _ => Err(ErrorCode::InvalidArgument(
                        "TimeTravelPoint must be constant timestamp value",
                    )),
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_table_indexes(
        &self,
        tenant: &str,
        catalog_name: &str,
        table_id: MetaId,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        let catalog = self.catalogs.get_catalog(tenant, catalog_name).await?;
        let index_metas = catalog
            .list_indexes(ListIndexesReq::new(tenant, Some(table_id)))
            .await?;

        Ok(index_metas)
    }
}

// copy from common-storages-fuse to avoid cyclic dependency.
fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => String::from_utf8(val.clone())
            .map_err(|e| ErrorCode::BadArguments(format!("invalid string. {}", e))),
        _ => Err(ErrorCode::BadArguments("invalid string.")),
    }
}

#[inline(always)]
pub fn parse_result_scan_args(table_args: &TableArgs) -> Result<String> {
    let args = table_args.expect_all_positioned("RESULT_SCAN", Some(1))?;
    string_value(&args[0])
}
