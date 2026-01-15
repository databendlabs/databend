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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::CTE;
use databend_common_ast::ast::ColumnDefinition;
use databend_common_ast::ast::CreateOption;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::TableType;
use databend_common_ast::ast::With;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::convert_to_type_name;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;

use crate::NameResolutionContext;
use crate::normalize_identifier;
use crate::optimizer::ir::SExpr;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::planner::binder::scalar::ScalarBinder;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::SortItem;
#[derive(Debug, Default, Visitor)]
#[visitor(TableReference(enter))]
struct CTERefCounter {
    cte_ref_count: HashMap<String, usize>,
    name_resolution_ctx: NameResolutionContext,
}

impl CTERefCounter {
    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        if let TableReference::Table { table, .. } = table_ref {
            let table_name = normalize_identifier(&table.table, &self.name_resolution_ctx).name;
            if let Some(count) = self.cte_ref_count.get_mut(&table_name) {
                *count += 1;
            }
        }
    }
}

impl Binder {
    #[recursive::recursive]
    pub(crate) fn bind_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        let mut with = query.with.clone();
        if self.ctx.get_settings().get_enable_auto_materialize_cte()? {
            if let Some(with) = &mut with {
                if !with.recursive {
                    self.auto_materialize_cte(with, query)?;
                }
            }
        }

        self.init_cte(bind_context, &with)?;

        // Extract limit and offset from query.
        let (limit, offset) = self.extract_limit_and_offset(query)?;

        // Bind query body.
        let (mut s_expr, mut bind_context) =
            self.bind_set_expr(bind_context, &query.body, &query.order_by, limit, None)?;

        // Bind order by for `SetOperation` and `Values`.
        s_expr = self.bind_query_order_by(&mut bind_context, query, s_expr)?;

        // Bind limit.
        s_expr = self.bind_query_limit(query, s_expr, limit, offset);

        if let Some(with) = &with {
            s_expr = self.bind_materialized_cte(with, s_expr, bind_context.cte_context.clone())?;
        }

        bind_context.reset_result_column_positions();

        Ok((s_expr, bind_context))
    }

    fn auto_materialize_cte(&mut self, with: &mut With, query: &Query) -> Result<()> {
        let cte_ref_count = self.compute_cte_ref_count(with, query)?;

        // Update materialization based on reference count
        for cte in with.ctes.iter_mut() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            if let Some(count) = cte_ref_count.get(&table_name) {
                log::info!("[CTE]cte_ref_count: {table_name} {count}");
                // Materialize if referenced more than once
                cte.materialized = !cte.user_specified_materialized && *count > 1;
            }
        }

        Ok(())
    }

    #[recursive::recursive]
    pub fn compute_cte_ref_count(
        &self,
        with: &With,
        query: &Query,
    ) -> Result<HashMap<String, usize>> {
        // Initialize the count of each CTE to 0
        let mut cte_ref_count: HashMap<String, usize> = HashMap::new();
        for cte in with.ctes.iter() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            cte_ref_count.insert(table_name, 0);
        }

        // Count the number of times each CTE is referenced in the query
        let mut visitor = CTERefCounter {
            cte_ref_count,
            name_resolution_ctx: self.name_resolution_ctx.clone(),
        };
        query.drive(&mut visitor);

        Ok(visitor.cte_ref_count)
    }

    pub(crate) fn bind_query_order_by(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
        child: SExpr,
    ) -> Result<SExpr> {
        if !matches!(
            query.body,
            SetExpr::SetOperation(_) | SetExpr::Values { .. }
        ) || query.order_by.is_empty()
        {
            return Ok(child);
        }

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let mut order_by_items = Vec::with_capacity(query.order_by.len());

        let settings = self.ctx.get_settings();
        let default_nulls_first = settings.get_nulls_first();

        for order in query.order_by.iter() {
            match order.expr {
                Expr::ColumnRef { .. } => {
                    let scalar = scalar_binder.bind(&order.expr)?.0;
                    match scalar {
                        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) => {
                            let asc = order.asc.unwrap_or(true);
                            let order_by_item = SortItem {
                                index: column.index,
                                asc,
                                nulls_first: order
                                    .nulls_first
                                    .unwrap_or_else(|| default_nulls_first(asc)),
                            };
                            order_by_items.push(order_by_item);
                        }
                        _ => {
                            return Err(ErrorCode::Internal("scalar should be BoundColumnRef")
                                .set_span(order.expr.span()));
                        }
                    }
                }
                _ => {
                    return Err(
                        ErrorCode::SemanticError("can only order by column".to_string())
                            .set_span(order.expr.span()),
                    );
                }
            }
        }
        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
            after_exchange: None,
            pre_projection: None,
            window_partition: None,
        };
        Ok(SExpr::create_unary(
            Arc::new(sort_plan.into()),
            Arc::new(child),
        ))
    }

    #[recursive::recursive]
    pub fn m_cte_to_temp_table(
        &mut self,
        cte: &CTE,
        cte_index: usize,
        mut with: With,
    ) -> Result<()> {
        let engine = if self.ctx.get_settings().get_persist_materialized_cte()? {
            Engine::Fuse
        } else {
            Engine::Memory
        };
        let query_id = self.ctx.get_id();
        let database = self.ctx.get_current_database();
        let mut table_identifier = cte.alias.name.clone();
        table_identifier.name = format!("{}${}", table_identifier.name, query_id.replace("-", ""));
        let table_name = normalize_identifier(&table_identifier, &self.name_resolution_ctx).name;
        self.m_cte_table_name.insert(
            normalize_identifier(&cte.alias.name, &self.name_resolution_ctx).name,
            table_name.clone(),
        );
        if self
            .ctx
            .is_temp_table(CATALOG_DEFAULT, &database, &table_name)
        {
            return Err(ErrorCode::Internal(format!(
                "Temporary table {:?} already exists in current session, please change the materialized CTE name",
                table_name
            )));
        }

        let mut expr_replacer = TableNameReplacer::new(
            database.clone(),
            self.m_cte_table_name.clone(),
            self.name_resolution_ctx.clone(),
        );
        let mut as_query = cte.query.clone();
        with.ctes.truncate(cte_index);
        with.ctes.retain(|cte| !cte.user_specified_materialized);
        as_query.with = if !with.ctes.is_empty() {
            Some(with)
        } else {
            None
        };
        as_query.drive_mut(&mut expr_replacer);

        let source = if cte.alias.columns.is_empty() {
            None
        } else {
            let mut bind_context = BindContext::new();
            let (_, bind_context) = self.bind_query(&mut bind_context, &as_query)?;
            let columns = &bind_context.columns;
            if columns.len() != cte.alias.columns.len() {
                return Err(ErrorCode::Internal("Number of columns does not match"));
            }
            Some(CreateTableSource::Columns {
                columns: columns
                    .iter()
                    .zip(cte.alias.columns.iter())
                    .map(|(column, ident)| {
                        let data_type = convert_to_type_name(&column.data_type);
                        ColumnDefinition {
                            name: ident.clone(),
                            data_type,
                            expr: None,
                            check: None,
                            comment: None,
                        }
                    })
                    .collect(),
                opt_table_indexes: None,
                opt_column_constraints: None,
                opt_table_constraints: None,
            })
        };

        let catalog = self.ctx.get_current_catalog();
        let create_table_stmt = CreateTableStmt {
            create_option: CreateOption::Create,
            catalog: Some(Identifier::from_name(Span::None, catalog.clone())),
            database: Some(Identifier::from_name(Span::None, database.clone())),
            table: table_identifier,
            source,
            engine: Some(engine),
            uri_location: None,
            cluster_by: None,
            table_options: Default::default(),
            iceberg_table_partition: None,
            table_properties: Default::default(),
            as_query: Some(as_query),
            table_type: TableType::Temporary,
        };

        let create_table_sql = create_table_stmt.to_string();
        log::info!("[CTE]create_table_sql: {create_table_sql}");
        if let Some(subquery_executor) = &self.subquery_executor {
            let _ = databend_common_base::runtime::block_on(async move {
                subquery_executor
                    .execute_query_with_sql_string(&create_table_sql)
                    .await
            })?;
        } else {
            return Err(ErrorCode::Internal("Binder's Subquery executor is not set"));
        };

        self.ctx.add_m_cte_temp_table(&database, &table_name);

        self.ctx
            .evict_table_from_cache(&catalog, &database, &table_name)
    }
}

#[derive(VisitorMut)]
#[visitor(TableReference(enter), Expr(enter))]
pub struct TableNameReplacer {
    database: String,
    new_name: HashMap<String, String>,
    name_resolution_ctx: NameResolutionContext,
}

impl TableNameReplacer {
    pub fn new(
        database: String,
        new_name: HashMap<String, String>,
        name_resolution_ctx: NameResolutionContext,
    ) -> Self {
        Self {
            database,
            new_name,
            name_resolution_ctx,
        }
    }

    fn replace_identifier(&mut self, identifier: &mut Identifier) {
        let name = normalize_identifier(identifier, &self.name_resolution_ctx).name;
        if let Some(new_name) = self.new_name.get(&name) {
            identifier.name = new_name.clone();
        }
    }

    #[recursive::recursive]
    fn enter_table_reference(&mut self, table_reference: &mut TableReference) {
        if let TableReference::Table { table, .. } = table_reference {
            if table.database.is_none() || table.database.as_ref().unwrap().name == self.database {
                self.replace_identifier(&mut table.table);
            }
        }
    }

    #[recursive::recursive]
    fn enter_expr(&mut self, expr: &mut Expr) {
        if let Expr::ColumnRef { column, .. } = expr {
            if column.database.is_none() || column.database.as_ref().unwrap().name == self.database
            {
                if let Some(table_identifier) = &mut column.table {
                    self.replace_identifier(table_identifier);
                }
            }
        }
    }
}
