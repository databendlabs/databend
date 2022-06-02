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
use common_ast::ast::TableReference;
use common_ast::parser::error::Backtrace;
use common_ast::parser::error::DisplayError;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::catalogs::CATALOG_DEFAULT;
use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::IndexType;
use crate::storages::view::view_table::QUERY;
use crate::storages::Table;
use crate::storages::ToReadDataSourcePlan;
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
        let source = table_meta.read_plan(self.ctx.clone(), None).await?;
        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_owned(),
            database.to_string(),
            table_meta,
            source,
        );

        self.bind_base_table(bind_context, table_index)
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
                travel_point,
            } => {
                // Get catalog name
                let catalog = catalog
                    .as_ref()
                    .map(|id| id.name.clone())
                    .unwrap_or_else(|| self.ctx.get_current_catalog());

                // Get database name
                let database = database
                    .as_ref()
                    .map(|ident| ident.name.clone())
                    .unwrap_or_else(|| self.ctx.get_current_database());

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
                        travel_point,
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
                        let stmts = parse_sql(&tokens, &backtrace)?;
                        if stmts.len() > 1 {
                            return Err(ErrorCode::UnImplement("unsupported multiple statements"));
                        }
                        if let Statement::Query(query) = &stmts[0] {
                            self.bind_query(bind_context, query).await
                        } else {
                            Err(ErrorCode::LogicalError(format!(
                                "Invalid VIEW object: {}",
                                table_meta.name()
                            )))
                        }
                    }
                    _ => {
                        let source = table_meta
                            .read_plan_with_catalog(self.ctx.clone(), catalog.clone(), None)
                            .await?;
                        let table_index = self
                            .metadata
                            .write()
                            .add_table(catalog, database, table_meta, source);

                        let (s_expr, mut bind_context) =
                            self.bind_base_table(bind_context, table_index)?;
                        if let Some(alias) = alias {
                            bind_context.apply_table_alias(alias)?;
                        }
                        Ok((s_expr, bind_context))
                    }
                }
            }
            TableReference::TableFunction {
                name,
                params,
                alias,
            } => {
                let mut scalar_binder =
                    ScalarBinder::new(bind_context, self.ctx.clone(), self.metadata.clone());
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
                let table_index = self.metadata.write().add_table(
                    CATALOG_DEFAULT.to_string(),
                    "system".to_string(),
                    table.clone(),
                    source,
                );

                let (s_expr, mut bind_context) = self.bind_base_table(bind_context, table_index)?;
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

    fn bind_base_table(
        &mut self,
        bind_context: &BindContext,
        table_index: IndexType,
    ) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
        let metadata = self.metadata.read();
        let columns = metadata.columns_by_table_index(table_index);
        let table = metadata.table(table_index);
        for column in columns.iter() {
            let column_binding = ColumnBinding {
                table_name: Some(table.name.clone()),
                column_name: column.name.to_lowercase(),
                index: column.column_index,
                data_type: column.data_type.clone(),
                visible_in_unqualified_wildcard: true,
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
}
