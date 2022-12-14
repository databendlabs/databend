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
use std::path::Path;
use std::sync::Arc;

use common_ast::ast::FileLocation;
use common_ast::ast::Indirection;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::Statement;
use common_ast::ast::TableAlias;
use common_ast::ast::TableReference;
use common_ast::ast::TimeTravelPoint;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_ast::DisplayError;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::ColumnId;
use common_catalog::table::ColumnStatistics;
use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table_function::TableFunction;
use common_config::GlobalConfig;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_storages_stage::get_first_file;
use common_storages_stage::StageTable;
use common_storages_view::view_table::QUERY;

use crate::binder::copy::parse_stage_location_v2;
use crate::binder::location::parse_uri_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::binder::CteInfo;
use crate::binder::Visibility;
use crate::optimizer::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::TypeChecker;
use crate::plans::ConstantExpr;
use crate::plans::LogicalGet;
use crate::plans::Scalar;
use crate::plans::Statistics;
use crate::BindContext;
use crate::ColumnEntry;
use crate::IndexType;

impl<'a> Binder {
    pub(super) async fn bind_one_table(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt<'a>,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in &stmt.select_list {
            if let SelectTarget::QualifiedName {
                qualified: names, ..
            } = select_target
            {
                for indirect in names {
                    if indirect == &Indirection::Star {
                        return Err(ErrorCode::SemanticError(stmt.span.display_error(
                            "SELECT * with no tables specified is not valid".to_string(),
                        )));
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
        );

        self.bind_base_table(bind_context, database, table_index)
            .await
    }

    pub(super) async fn bind_table_reference(
        &mut self,
        bind_context: &BindContext,
        table_ref: &TableReference<'a>,
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
                let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
                let table_alias_name = if let Some(table_alias) = alias {
                    Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
                } else {
                    None
                };
                // Check and bind common table expression
                if let Some(cte_info) = bind_context.ctes_map.get(&table_name) {
                    return self.bind_cte(bind_context, &table_name, alias, &cte_info);
                }
                // Get catalog name
                let catalog = catalog
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_catalog());

                // Get database name
                let database = database
                    .as_ref()
                    .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .unwrap_or_else(|| self.ctx.get_current_database());

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
                        let new_bind_context =
                            BindContext::with_parent(Box::new(bind_context.clone()));
                        if let Statement::Query(query) = &stmt {
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
                alias,
            } => {
                let mut scalar_binder = ScalarBinder::new(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                );
                let mut args = Vec::with_capacity(params.len());
                for arg in params.iter() {
                    args.push(scalar_binder.bind(arg).await?);
                }

                let expressions = args
                    .into_iter()
                    .map(|(scalar, _)| match scalar {
                        Scalar::ConstantExpr(ConstantExpr { value, .. }) => Ok(value),
                        _ => Err(ErrorCode::Unimplemented(format!(
                            "Unsupported table argument type: {:?}",
                            scalar
                        ))),
                    })
                    .collect::<Result<Vec<DataValue>>>()?;

                let table_args = Some(expressions);

                // Table functions always reside is default catalog
                let table_meta: Arc<dyn TableFunction> = self
                    .catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table_function(
                        &normalize_identifier(name, &self.name_resolution_ctx).name,
                        table_args,
                    )?;
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
                files,
                alias,
            } => {
                let table_alias_name = if let Some(table_alias) = alias {
                    Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
                } else {
                    None
                };

                let (mut user_stage_info, path) = match location {
                    FileLocation::Stage(location) => {
                        parse_stage_location_v2(&self.ctx, &location.name, &location.path).await?
                    }
                    FileLocation::Uri(location) => {
                        let (storage_params, path) = parse_uri_location(location)?;
                        if !storage_params.is_secure()
                            && !GlobalConfig::instance().storage.allow_insecure
                        {
                            return Err(ErrorCode::StorageInsecure(
                                "copy from insecure storage is not allowed",
                            ));
                        }
                        let stage_info = UserStageInfo::new_external_stage(storage_params, &path);
                        (stage_info, path)
                    }
                };

                let op = StageTable::get_op(&self.ctx, &user_stage_info)?;

                let first_file = if files.is_empty() {
                    let file = get_first_file(&op, &path).await?;
                    match file {
                        None => {
                            return Err(ErrorCode::BadArguments(format!(
                                "no file in {}",
                                location
                            )));
                        }
                        Some(f) => f.path().to_string(),
                    }
                } else {
                    Path::new(&path)
                        .join(&files[0])
                        .to_string_lossy()
                        .to_string()
                };

                let format_type = StageFileFormatType::Parquet;
                let input_format = InputContext::get_input_format(&format_type)?;
                let schema = input_format.infer_schema(&first_file, &op).await?;
                user_stage_info.file_format_options = FileFormatOptions {
                    format: format_type,
                    record_delimiter: "".to_string(),
                    field_delimiter: "".to_string(),
                    nan_display: "".to_string(),
                    skip_header: 0,
                    escape: "".to_string(),
                    compression: StageFileCompression::default(),
                    row_tag: "".to_string(),
                    quote: "".to_string(),
                };
                let stage_table_info = StageTableInfo {
                    schema,
                    user_stage_info,
                    path: path.to_string(),
                    files: vec![],
                    pattern: Default::default(),
                    files_to_copy: None,
                };

                let stage_table = StageTable::try_create(stage_table_info)?;

                let database = "default".to_string();
                let table_index = self.metadata.write().add_table(
                    database.clone(),
                    "default".to_string(),
                    stage_table,
                    table_alias_name,
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

        let mut col_stats: HashMap<IndexType, Option<ColumnStatistics>> = HashMap::new();
        for column in columns.iter() {
            match column {
                ColumnEntry::BaseTableColumn {
                    column_name,
                    column_index,
                    path_indices,
                    data_type,
                    leaf_index,
                    ..
                } => {
                    let column_binding = ColumnBinding {
                        database_name: Some(database_name.to_string()),
                        table_name: Some(table.name().to_string()),
                        column_name: column_name.clone(),
                        index: *column_index,
                        data_type: Box::new(data_type.clone()),
                        visibility: if path_indices.is_some() {
                            Visibility::InVisible
                        } else {
                            Visibility::Visible
                        },
                    };
                    bind_context.add_column_binding(column_binding);
                    if path_indices.is_none() {
                        if let Some(col_id) = *leaf_index {
                            let col_stat = table
                                .table()
                                .column_statistics_provider()
                                .await?
                                .column_statistics(col_id as ColumnId);
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
                LogicalGet {
                    table_index,
                    columns: columns
                        .into_iter()
                        .map(|col| match col {
                            ColumnEntry::BaseTableColumn { column_index, .. } => column_index,
                            ColumnEntry::DerivedColumn { column_index, .. } => column_index,
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
        travel_point: &TimeTravelPoint<'a>,
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
                let box (scalar, data_type) = type_checker
                    .resolve(expr, Some(TimestampType::new_impl()))
                    .await?;

                if let Scalar::ConstantExpr(ConstantExpr { value, .. }) = scalar {
                    if let DataTypeImpl::Timestamp(datatime_64) = data_type {
                        return Ok(NavigationPoint::TimePoint(
                            datatime_64.utc_timestamp(value.as_i64()?),
                        ));
                    }
                }
                Err(ErrorCode::InvalidArgument(
                    "TimeTravelPoint must be constant timestamp",
                ))
            }
        }
    }
}
