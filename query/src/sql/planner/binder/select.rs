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

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::Indirection;
use common_ast::ast::OrderByExpr;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
use common_ast::parser::error::DisplayError as _;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::catalogs::CATALOG_DEFAULT;
use crate::sql::binder::scalar_common::split_conjunctions;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::planner::binder::ColumnBinding;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::storages::Table;
use crate::storages::ToReadDataSourcePlan;
use crate::table_functions::TableFunction;

impl<'a> Binder {
    #[async_recursion]
    pub(crate) async fn bind_query(
        &mut self,
        bind_context: &BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        let (mut s_expr, bind_context) = match &query.body {
            SetExpr::Select(stmt) => {
                self.bind_select_stmt(bind_context, stmt, &query.order_by)
                    .await
            }
            SetExpr::Query(stmt) => self.bind_query(bind_context, stmt).await,
            _ => Err(ErrorCode::UnImplement("Unsupported query type")),
        }?;

        if !query.limit.is_empty() {
            if query.limit.len() == 1 {
                s_expr = self
                    .bind_limit(&bind_context, s_expr, Some(&query.limit[0]), &query.offset)
                    .await?;
            } else {
                s_expr = self
                    .bind_limit(
                        &bind_context,
                        s_expr,
                        Some(&query.limit[0]),
                        &Some(query.limit[1].clone()),
                    )
                    .await?;
            }
        } else if query.offset.is_some() {
            s_expr = self
                .bind_limit(&bind_context, s_expr, None, &query.offset)
                .await?;
        }

        Ok((s_expr, bind_context))
    }

    pub(super) async fn bind_select_stmt(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt<'a>,
        order_by: &[OrderByExpr<'a>],
    ) -> Result<(SExpr, BindContext)> {
        let (mut s_expr, from_context) = if let Some(from) = &stmt.from {
            self.bind_table_reference(bind_context, from).await?
        } else {
            self.bind_one_table(stmt).await?
        };

        if let Some(expr) = &stmt.selection {
            s_expr = self.bind_where(&from_context, expr, s_expr, false).await?;
        }

        // Output of current `SELECT` statement.
        let mut output_context = self
            .normalize_select_list(&from_context, &stmt.select_list)
            .await?;

        let agg_info = self.analyze_aggregate(&output_context)?;
        if !agg_info.aggregate_functions.is_empty() || !stmt.group_by.is_empty() {
            (s_expr, output_context) = self
                .bind_group_by(
                    &from_context,
                    output_context,
                    s_expr,
                    &stmt.group_by,
                    &agg_info,
                )
                .await?;
        }

        if let Some(expr) = &stmt.having {
            s_expr = self.bind_where(&from_context, expr, s_expr, true).await?;
        }

        s_expr = self.bind_projection(&output_context, s_expr)?;

        if !order_by.is_empty() {
            s_expr = self
                .bind_order_by(&from_context, &output_context, s_expr, order_by)
                .await?;
        }

        Ok((s_expr, output_context))
    }

    pub(super) async fn bind_one_table(
        &mut self,
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
            .resolve_data_source(tenant.as_str(), catalog, database, "one")
            .await?;
        let source = table_meta.read_plan(self.ctx.clone(), None).await?;
        let table_index = self.metadata.add_table(
            CATALOG_DEFAULT.to_owned(),
            database.to_string(),
            table_meta,
            source,
        );

        self.bind_base_table(table_index).await
    }

    pub(super) async fn bind_table_reference(
        &mut self,
        bind_context: &BindContext,
        stmt: &TableReference<'a>,
    ) -> Result<(SExpr, BindContext)> {
        match stmt {
            TableReference::Table {
                catalog,
                database,
                table,
                alias,
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| ident.name.clone())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let catalog = catalog
                    .as_ref()
                    .map(|id| id.name.clone())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());
                let table = table.name.clone();
                // TODO: simply normalize table name to lower case, maybe use a more reasonable way
                let table = table.to_lowercase();
                let tenant = self.ctx.get_tenant();

                // Resolve table with catalog
                let table_meta: Arc<dyn Table> = self
                    .resolve_data_source(
                        tenant.as_str(),
                        catalog.as_str(),
                        database.as_str(),
                        table.as_str(),
                    )
                    .await?;
                let source = table_meta.read_plan(self.ctx.clone(), None).await?;
                let table_index = self
                    .metadata
                    .add_table(catalog, database, table_meta, source);

                let (s_expr, mut bind_context) = self.bind_base_table(table_index).await?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias)?;
                }
                Ok((s_expr, bind_context))
            }
            TableReference::TableFunction {
                name,
                params,
                alias,
            } => {
                let scalar_binder = ScalarBinder::new(bind_context, self.ctx.clone());
                let mut args = Vec::with_capacity(params.len());
                for arg in params.iter() {
                    args.push(scalar_binder.bind_expr(arg).await?);
                }

                let expressions = args
                    .into_iter()
                    .map(|(scalar, _)| match scalar {
                        Scalar::ConstantExpr(ConstantExpr { value, data_type }) => {
                            Ok(Expression::Literal {
                                value,
                                column_name: None,
                                data_type,
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
                    .get_table_function(name.name.as_str(), table_args)?;
                let table = table_meta.as_table();

                let source = table.read_plan(self.ctx.clone(), None).await?;
                let table_index = self.metadata.add_table(
                    CATALOG_DEFAULT.to_string(),
                    "system".to_string(),
                    table.clone(),
                    source,
                );

                let (s_expr, mut bind_context) = self.bind_base_table(table_index).await?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias)?;
                }
                Ok((s_expr, bind_context))
            }
            TableReference::Join(join) => self.bind_join(bind_context, join).await,
            TableReference::Subquery { subquery, alias } => {
                let (s_expr, mut bind_context) = self.bind_query(bind_context, subquery).await?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias)?;
                }
                Ok((s_expr, bind_context))
            }
        }
    }

    async fn bind_base_table(&mut self, table_index: IndexType) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::new();
        let columns = self.metadata.columns_by_table_index(table_index);
        let table = self.metadata.table(table_index);
        for column in columns.iter() {
            let column_binding = ColumnBinding {
                table_name: Some(table.name.clone()),
                column_name: column.name.to_lowercase(),
                visible: true,
                index: column.column_index,
                data_type: column.data_type.clone(),
                scalar: None,
                duplicated: false,
            };
            bind_context.add_column_binding(column_binding);
        }
        Ok((
            SExpr::create_leaf(
                LogicalGet {
                    table_index,
                    columns: columns.into_iter().map(|col| col.column_index).collect(),
                }
                .into(),
            ),
            bind_context,
        ))
    }

    pub(super) async fn bind_where(
        &mut self,
        bind_context: &BindContext,
        expr: &Expr<'a>,
        child: SExpr,
        is_having: bool,
    ) -> Result<SExpr> {
        let scalar_binder = ScalarBinder::new(bind_context, self.ctx.clone());
        let (scalar, _) = scalar_binder.bind_expr(expr).await?;
        let filter_plan = FilterPlan {
            predicates: split_conjunctions(&scalar),
            is_having,
        };
        let new_expr = SExpr::create_unary(filter_plan.into(), child);
        Ok(new_expr)
    }
}
