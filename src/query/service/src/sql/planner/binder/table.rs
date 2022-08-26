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

use std::sync::Arc;

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
use common_catalog::catalog::CATALOG_DEFAULT;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::sessions::TableContext;
use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::binder::CteInfo;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::normalize_identifier;
use crate::sql::planner::semantic::TypeChecker;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::IndexType;
use crate::storages::view::view_table::QUERY;
use crate::storages::NavigationPoint;
use crate::storages::Table;
use crate::table_functions::TableFunction;

impl<'a> Binder {
    pub(super) async fn bind_one_table(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt<'a>,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in &stmt.select_list {
            if let SelectTarget::QualifiedName(names) = select_target {
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
                // Check and bind common table expression
                if let Some(cte_info) = bind_context.ctes_map.read().get(&table_name) {
                    return self.bind_cte(bind_context, &table_name, alias, cte_info);
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
                            .ok_or_else(|| ErrorCode::LogicalError("Invalid VIEW object"))?;
                        let tokens = tokenize_sql(query.as_str())?;
                        let backtrace = Backtrace::new();
                        let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &backtrace)?;
                        if let Statement::Query(query) = &stmt {
                            self.bind_query(bind_context, query).await
                        } else {
                            Err(ErrorCode::LogicalError(format!(
                                "Invalid VIEW object: {}",
                                table_meta.name()
                            )))
                        }
                    }
                    _ => {
                        let table_index =
                            self.metadata
                                .write()
                                .add_table(catalog, database.clone(), table_meta);

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
                        Scalar::ConstantExpr(ConstantExpr { value, data_type }) => {
                            Ok(Expression::Literal {
                                value,
                                column_name: None,
                                data_type: *data_type,
                            })
                        }
                        _ => Err(ErrorCode::UnImplement(format!(
                            "Unsupported table argument type: {:?}",
                            scalar
                        ))),
                    })
                    .collect::<Result<Vec<Expression>>>()?;

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

                let table_index = self.metadata.write().add_table(
                    CATALOG_DEFAULT.to_string(),
                    "system".to_string(),
                    table.clone(),
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
                let (s_expr, mut bind_context) = self.bind_query(bind_context, subquery).await?;
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
        for column in columns.iter() {
            let visible_in_unqualified_wildcard = column.path_indices.is_none();
            let column_binding = ColumnBinding {
                database_name: Some(database_name.to_string()),
                table_name: Some(table.name.clone()),
                column_name: column.name.clone(),
                index: column.column_index,
                data_type: Box::new(column.data_type.clone()),
                visible_in_unqualified_wildcard,
            };
            bind_context.add_column_binding(column_binding);
        }
        let stat = table.table.statistics(self.ctx.clone()).await?;
        Ok((
            SExpr::create_leaf(
                LogicalGet {
                    table_index,
                    columns: columns.into_iter().map(|col| col.column_index).collect(),
                    push_down_predicates: None,
                    limit: None,
                    order_by: None,
                    statistics: stat,
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
            table_meta = table_meta.navigate_to(self.ctx.clone(), tp).await?;
        }
        Ok(table_meta)
    }

    async fn resolve_data_travel_point(
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
                    .resolve(expr, Some(TimestampType::new_impl(6)))
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
