// Copyright 2021 Datafuse Labs.
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

use std::rc::Rc;
use std::sync::Arc;

use async_recursion::async_recursion;
use common_ast::parser::ast::Expr;
use common_ast::parser::ast::Indirection;
use common_ast::parser::ast::Query;
use common_ast::parser::ast::SelectStmt;
use common_ast::parser::ast::SelectTarget;
use common_ast::parser::ast::SetExpr;
use common_ast::parser::ast::Statement;
use common_ast::parser::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::bind_context::BindContext;
use crate::sql::planner::expression_binder::ExpressionBinder;
use crate::sql::planner::metadata::Metadata;
use crate::sql::planner::plan::LogicalGet;
use crate::sql::planner::plan::LogicalProject;
use crate::sql::planner::plan::Plan;
use crate::sql::planner::scalar::BoundVariable;
use crate::sql::planner::scalar::ScalarExpr;
use crate::sql::IndexType;
use crate::sql::ProjectItem;
use crate::storages::Table;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
pub struct Binder {
    catalog: Arc<dyn Catalog>,
    metadata: Metadata,
    context: Arc<QueryContext>,
}

impl Binder {
    pub fn new(catalog: Arc<dyn Catalog>, context: Arc<QueryContext>) -> Self {
        Binder {
            catalog,
            metadata: Metadata::create(),
            context,
        }
    }

    pub async fn bind(mut self, stmt: &Statement) -> Result<BindResult> {
        let bind_context = self.bind_statement(stmt).await?;
        Ok(BindResult::create(bind_context, self.metadata))
    }

    async fn bind_statement(&mut self, stmt: &Statement) -> Result<BindContext> {
        match stmt {
            Statement::Select(stmt) => {
                let bind_context = self.bind_query(stmt).await?;
                Ok(bind_context)
            }
            _ => todo!(),
        }
    }

    #[async_recursion(? Send)]
    async fn bind_query(&mut self, stmt: &Query) -> Result<BindContext> {
        match &stmt.body {
            SetExpr::Select(stmt) => self.bind_select_stmt(stmt).await,
            SetExpr::Query(stmt) => self.bind_query(stmt).await,
            _ => todo!(),
        }
    }

    async fn bind_select_stmt(&mut self, stmt: &SelectStmt) -> Result<BindContext> {
        let mut bind_context = self.bind_table_reference(&stmt.from).await?;
        let projections = self.normalize_select_list(&stmt.select_list, &mut bind_context)?;
        let bind_context = self.bind_projections(projections, bind_context)?;

        Ok(bind_context)
    }

    async fn bind_table_reference(&mut self, stmt: &TableReference) -> Result<BindContext> {
        match stmt {
            TableReference::Table {
                database,
                table,
                alias,
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| ident.name.clone())
                    .unwrap_or_else(|| self.context.get_current_database());
                let table = table.name.clone();
                // TODO: simply normalize table name to lower case, maybe use a more reasonable way
                let table = table.to_lowercase();
                let tenant = self.context.get_tenant();

                // Resolve table with catalog
                let table_meta: Arc<dyn Table> = Self::resolve_data_source(
                    self.catalog.as_ref(),
                    tenant.as_str(),
                    database.as_str(),
                    table.as_str(),
                )
                .await?;
                let table_index = self.metadata.add_base_table(database, table_meta.clone());

                for field in table_meta.schema().fields() {
                    self.metadata.add_column(
                        field.name().clone(),
                        field.data_type().clone(),
                        field.is_nullable(),
                        table_index,
                    );
                }
                let mut result = self.bind_base_table(table_index).await?;
                if let Some(alias) = alias {
                    result.apply_table_alias(&table, alias)?;
                }
                Ok(result)
            }
            _ => todo!(),
        }
    }

    async fn bind_base_table(&mut self, table_index: IndexType) -> Result<BindContext> {
        let mut bind_context = BindContext::create();
        let columns = self.metadata.columns_by_table_index(table_index);
        let table = self.metadata.table(table_index);
        for column in columns.iter() {
            bind_context.add_column_binding(
                column.column_index,
                table.name.clone(),
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
                None,
            );
        }
        bind_context.expression = Some(SExpr::create_leaf(Rc::new(Plan::LogicalGet(LogicalGet {
            table_index,
            columns: columns.into_iter().map(|col| col.column_index).collect(),
        }))));

        Ok(bind_context)
    }

    /// Expand wildcard
    #[allow(unreachable_patterns)]
    fn normalize_select_list(
        &mut self,
        select_list: &[SelectTarget],
        bind_context: &mut BindContext,
    ) -> Result<Vec<ProjectItem>> {
        let mut result = vec![];

        for select_target in select_list {
            match select_target {
                SelectTarget::Projection { expr, alias } => {
                    let expr_binder = ExpressionBinder::create();
                    let scalar_expr = expr_binder.bind_expr(expr, bind_context)?;
                    let alias = match alias {
                        None => get_expr_display_string(expr),
                        Some(alias) => alias.name.clone(),
                    };
                    let column_index: IndexType = match scalar_expr {
                        ScalarExpr::BoundVariable(BoundVariable { index, .. }) => index,
                        _ => self.metadata.add_derived_column(
                            alias.clone(),
                            scalar_expr.data_type()?.clone(),
                            scalar_expr.nullable(),
                        ),
                    };
                    let item = ProjectItem {
                        index: column_index,
                        expr: scalar_expr,
                    };
                    result.push(item);
                }
                SelectTarget::Indirections(indirections) => {
                    if indirections.len() > 2 || indirections.is_empty() {
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }
                    if indirections.len() == 1 {
                        let indirection = &indirections[0];
                        match indirection {
                            Indirection::Identifier(ident) => {
                                let alias = ident.name.clone();
                                let column_binding =
                                    bind_context.resolve_column(None, alias.clone())?;
                                let item = ProjectItem {
                                    index: column_binding.index,
                                    expr: match column_binding.expr {
                                        None => ScalarExpr::BoundVariable(BoundVariable {
                                            index: column_binding.index,
                                            data_type: column_binding.data_type,
                                            nullable: column_binding.nullable,
                                        }),
                                        Some(scalar) => scalar,
                                    },
                                };
                                result.push(item);
                            }
                            Indirection::Star => {
                                // All columns in current context
                                for column in bind_context.all_column_bindings() {
                                    let item = ProjectItem {
                                        index: column.index,
                                        expr: match &column.expr {
                                            None => ScalarExpr::BoundVariable(BoundVariable {
                                                index: column.index,
                                                data_type: column.data_type.clone(),
                                                nullable: column.nullable,
                                            }),
                                            Some(scalar) => scalar.clone(),
                                        },
                                    };
                                    result.push(item);
                                }
                            }
                        }
                    } else if indirections.len() == 2 {
                        // TODO: Support indirection like `a.b`
                        return Err(ErrorCode::SemanticError("Unsupported indirection type"));
                    }
                }
            }
        }
        Ok(result)
    }

    fn bind_projections(
        &mut self,
        projections: Vec<ProjectItem>,
        mut bind_context: BindContext,
    ) -> Result<BindContext> {
        bind_context.expression = Some(SExpr::create_unary(
            Rc::new(Plan::LogicalProject(LogicalProject { items: projections })),
            bind_context.expression.unwrap(),
        ));
        Ok(bind_context)
    }

    pub async fn resolve_data_source(
        catalog: &dyn Catalog,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let table_meta = catalog.get_table(tenant, database, table).await?;
        Ok(table_meta)
    }
}

pub fn get_expr_display_string(_expr: &Expr) -> String {
    // TODO: this is Postgres style name for anonymous select item
    "?column?".to_string()
}

pub struct BindResult {
    pub bind_context: BindContext,
    pub metadata: Metadata,
}

impl BindResult {
    pub fn create(bind_context: BindContext, metadata: Metadata) -> Self {
        BindResult {
            bind_context,
            metadata,
        }
    }

    pub fn s_expr(&self) -> &SExpr {
        self.bind_context.expression.as_ref().unwrap()
    }
}
