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

use std::collections::HashMap;
use std::default::Default;
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use common_ast::ast::FileLocation;
use common_ast::ast::Indirection;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::Statement;
use common_ast::ast::TableAlias;
use common_ast::ast::TableReference;
use common_ast::ast::TimeTravelPoint;
use common_ast::ast::UriLocation;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::table::ColumnStatistics;
use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ColumnId;
use common_expression::ConstantFolder;
use common_expression::Scalar;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::StageInfo;
use common_storage::DataOperator;
use common_storage::StageFilesInfo;
use common_storages_parquet::ParquetTable;
use common_storages_result_cache::ResultCacheMetaManager;
use common_storages_result_cache::ResultCacheReader;
use common_storages_result_cache::ResultScan;
use common_storages_view::view_table::QUERY;
use common_users::UserApiProvider;

use crate::binder::copy::parse_stage_location_v2;
use crate::binder::location::parse_uri_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::table_args::bind_table_args;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::binder::CteInfo;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::TypeChecker;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::IndexType;

impl Binder {
    pub(super) async fn bind_one_table(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in &stmt.select_list {
            if let SelectTarget::QualifiedName {
                qualified: names, ..
            } = select_target
            {
                for indirect in names {
                    if indirect == &Indirection::Star {
                        return Err(ErrorCode::SemanticError(
                            "SELECT * with no tables specified is not valid".to_string(),
                        )
                        .set_span(stmt.span));
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
        );

        self.bind_base_table(bind_context, database, table_index)
            .await
    }

    pub(super) async fn bind_table_reference(
        &mut self,
        bind_context: &BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::Table {
                span: _,
                catalog,
                database,
                table,
                alias,
                travel_point,
            } => {
                let (catalog, database, table_name) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                let table_alias_name = if let Some(table_alias) = alias {
                    Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
                } else {
                    None
                };
                // Check and bind common table expression
                if let Some(cte_info) = bind_context.ctes_map.get(&table_name) {
                    return self.bind_cte(bind_context, &table_name, alias, &cte_info);
                }

                if database == "system" {
                    self.ctx.set_cacheable(false);
                }

                let tenant = self.ctx.get_tenant();

                let navigation_point = match travel_point {
                    Some(tp) => Some(self.resolve_data_travel_point(bind_context, tp).await?),
                    None => None,
                };

                // Resolve table with catalog
                let table_meta: Arc<dyn Table> = self
                    .resolve_data_source(
                        tenant.as_str(),
                        catalog.as_str(),
                        database.as_str(),
                        table_name.as_str(),
                        &navigation_point,
                    )
                    .await?;
                match table_meta.engine() {
                    "VIEW" => {
                        let query = table_meta
                            .options()
                            .get(QUERY)
                            .ok_or_else(|| ErrorCode::Internal("Invalid VIEW object"))?;
                        let tokens = tokenize_sql(query.as_str())?;
                        let backtrace = Backtrace::new();
                        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
                        // For view, we need use a new context to bind it.
                        let mut new_bind_context =
                            BindContext::with_parent(Box::new(bind_context.clone()));
                        new_bind_context.is_view = true;
                        if let Statement::Query(query) = &stmt {
                            self.metadata.write().add_table(
                                catalog,
                                database.clone(),
                                table_meta,
                                table_alias_name,
                                false,
                            );
                            let (s_expr, mut new_bind_context) =
                                self.bind_query(&new_bind_context, query).await?;
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
                            )))
                        }
                    }
                    _ => {
                        let table_index = self.metadata.write().add_table(
                            catalog,
                            database.clone(),
                            table_meta,
                            table_alias_name,
                            bind_context.is_view,
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
                span: _,
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
                );
                let table_args = bind_table_args(&mut scalar_binder, params, named_params).await?;

                let func_name = normalize_identifier(name, &self.name_resolution_ctx);

                if func_name.name.eq_ignore_ascii_case("result_scan") {
                    let query_id = parse_result_scan_args(&table_args)?;
                    if query_id.is_empty() {
                        return Err(ErrorCode::InvalidArgument(
                            "query_id must be specified when using `RESULT_SCAN`",
                        ));
                    }
                    let kv_store = UserApiProvider::instance().get_meta_store_client();
                    let meta_key = self.ctx.get_result_cache_key(&query_id);
                    if meta_key.is_none() {
                        return Err(ErrorCode::EmptyData(format!(
                            "`RESULT_SCAN` could not find related cache key in current session for this query id: {query_id}"
                        )));
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
                            )));
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
                    );

                    let (s_expr, mut bind_context) = self
                        .bind_base_table(bind_context, "system", table_index)
                        .await?;
                    if let Some(alias) = alias {
                        bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    }
                    return Ok((s_expr, bind_context));
                }

                // Table functions always reside is default catalog
                let table_meta: Arc<dyn TableFunction> = self
                    .catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table_function(&func_name.name, table_args)?;
                let table = table_meta.as_table();
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
                );

                let (s_expr, mut bind_context) = self
                    .bind_base_table(bind_context, "system", table_index)
                    .await?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                Ok((s_expr, bind_context))
            }
            TableReference::Join { span: _, join } => self.bind_join(bind_context, join).await,
            TableReference::Subquery {
                span: _,
                subquery,
                alias,
            } => {
                // For subquery, we need use a new context to bind it.
                let new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
                let (s_expr, mut new_bind_context) =
                    self.bind_query(&new_bind_context, subquery).await?;
                if let Some(alias) = alias {
                    new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                Ok((s_expr, new_bind_context))
            }
            TableReference::Stage {
                span: _,
                location,
                options,
                alias,
            } => {
                let (stage_info, path) = match location.clone() {
                    FileLocation::Stage(location) => {
                        parse_stage_location_v2(&self.ctx, &location.name, &location.path).await?
                    }
                    FileLocation::Uri(uri) => {
                        let mut location =
                            UriLocation::from_uri(uri, "".to_string(), options.connection.clone())?;
                        let (storage_params, path) = parse_uri_location(&mut location)?;
                        if !storage_params.is_secure()
                            && !GlobalConfig::instance().storage.allow_insecure
                        {
                            return Err(ErrorCode::StorageInsecure(
                                "copy from insecure storage is not allowed",
                            ));
                        }
                        let stage_info = StageInfo::new_external_stage(storage_params, &path);
                        (stage_info, path)
                    }
                };

                let file_format_options = match &options.file_format {
                    Some(f) => self.ctx.get_file_format(f).await?,
                    None => stage_info.file_format_options.clone(),
                };
                if matches!(file_format_options.format, StageFileFormatType::Parquet) {
                    let files_info = StageFilesInfo {
                        path,
                        pattern: options.pattern.clone(),
                        files: options.files.clone(),
                    };
                    let read_options = ParquetReadOptions::default();

                    let table =
                        ParquetTable::create(stage_info.clone(), files_info, read_options).await?;

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
                    );

                    let (s_expr, mut bind_context) = self
                        .bind_base_table(bind_context, "system", table_index)
                        .await?;
                    if let Some(alias) = alias {
                        bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    }
                    Ok((s_expr, bind_context))
                } else {
                    Err(ErrorCode::Unimplemented(
                        "only support parquet format for 'select from stage' for now.",
                    ))
                }
            }
        }
    }

    fn bind_cte(
        &mut self,
        bind_context: &BindContext,
        table_name: &str,
        alias: &Option<TableAlias>,
        cte_info: &CteInfo,
    ) -> Result<(SExpr, BindContext)> {
        let mut new_bind_context = bind_context.clone();
        new_bind_context.columns = cte_info.bind_context.columns.clone();
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
        for column in new_bind_context.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(alias_table_name.clone());
        }

        if cols_alias.len() > new_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                new_bind_context.columns.len(),
                cols_alias.len()
            )));
        }
        for (index, column_name) in cols_alias.iter().enumerate() {
            new_bind_context.columns[index].column_name = column_name.clone();
        }
        Ok((cte_info.s_expr.clone(), new_bind_context))
    }

    async fn bind_base_table(
        &mut self,
        bind_context: &BindContext,
        database_name: &str,
        table_index: IndexType,
    ) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
        let columns = self.metadata.read().columns_by_table_index(table_index);
        let table = self.metadata.read().table(table_index).clone();
        let statistics_provider = table.table().column_statistics_provider().await?;

        let mut col_stats: HashMap<IndexType, Option<ColumnStatistics>> = HashMap::new();
        for column in columns.iter() {
            match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_name,
                    column_index,
                    path_indices,
                    data_type,
                    leaf_index,
                    ..
                }) => {
                    let column_binding = ColumnBinding {
                        database_name: Some(database_name.to_string()),
                        table_name: Some(table.name().to_string()),
                        column_name: column_name.clone(),
                        index: *column_index,
                        data_type: Box::new(DataType::from(data_type)),
                        visibility: if path_indices.is_some() {
                            Visibility::InVisible
                        } else {
                            Visibility::Visible
                        },
                    };
                    bind_context.add_column_binding(column_binding);
                    if path_indices.is_none() {
                        if let Some(col_id) = *leaf_index {
                            let col_stat =
                                statistics_provider.column_statistics(col_id as ColumnId);
                            col_stats.insert(*column_index, col_stat);
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::Internal("Invalid column entry"));
                }
            }
        }

        let is_accurate = table.table().engine().to_lowercase() == "fuse";
        let stat = table.table().table_statistics()?;
        Ok((
            SExpr::create_leaf(
                Scan {
                    table_index,
                    columns: columns
                        .into_iter()
                        .map(|col| match col {
                            ColumnEntry::BaseTableColumn(BaseTableColumn {
                                column_index, ..
                            }) => column_index,
                            ColumnEntry::DerivedColumn(DerivedColumn { column_index, .. }) => {
                                column_index
                            }
                        })
                        .collect(),
                    push_down_predicates: None,
                    limit: None,
                    order_by: None,
                    statistics: Statistics {
                        statistics: stat,
                        col_stats,
                        is_accurate,
                    },
                    prewhere: None,
                }
                .into(),
            ),
            bind_context,
        ))
    }

    async fn resolve_data_source(
        &self,
        tenant: &str,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        travel_point: &Option<NavigationPoint>,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let catalog = self.catalogs.get_catalog(catalog_name)?;
        let mut table_meta = catalog.get_table(tenant, database_name, table_name).await?;

        if let Some(tp) = travel_point {
            table_meta = table_meta.navigate_to(tp).await?;
        }
        Ok(table_meta)
    }

    pub(crate) async fn resolve_data_travel_point(
        &self,
        bind_context: &BindContext,
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
                );
                let box (scalar, _) = type_checker.resolve(expr, None).await?;
                let scalar_expr = scalar.as_expr_with_col_name()?;

                let (new_expr, _) = ConstantFolder::fold(
                    &scalar_expr,
                    self.ctx.get_function_context()?,
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
