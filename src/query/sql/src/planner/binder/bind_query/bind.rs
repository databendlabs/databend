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

use std::sync::Arc;

use databend_common_ast::ast::CreateOption;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableType;
use databend_common_ast::ast::With;
use databend_common_ast::ast::CTE;
use databend_common_ast::Span;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::CteInfo;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::SortItem;

impl Binder {
    pub(crate) fn bind_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        // Initialize cte map.
        self.init_cte(bind_context, &query.with)?;

        // Extract limit and offset from query.
        let (limit, offset) = self.extract_limit_and_offset(query)?;

        // Bind query body.
        let (mut s_expr, mut bind_context) =
            self.bind_set_expr(bind_context, &query.body, &query.order_by, limit)?;

        // Bind order by for `SetOperation` and `Values`.
        s_expr = self.bind_query_order_by(&mut bind_context, query, s_expr)?;

        // Bind limit.
        s_expr = self.bind_query_limit(query, s_expr, limit, offset);

        Ok((s_expr, bind_context))
    }

    // Initialize cte map.
    pub(crate) fn init_cte(
        &mut self,
        bind_context: &mut BindContext,
        with: &Option<With>,
    ) -> Result<()> {
        let with = if let Some(with) = with {
            with
        } else {
            return Ok(());
        };

        for (idx, cte) in with.ctes.iter().enumerate() {
            let table_name = self.normalize_identifier(&cte.alias.name).name;
            if bind_context.cte_context.cte_map.contains_key(&table_name) {
                return Err(ErrorCode::SemanticError(format!(
                    "Duplicate common table expression: {table_name}"
                )));
            }
            let column_name = cte
                .alias
                .columns
                .iter()
                .map(|ident| self.normalize_identifier(ident).name)
                .collect();
            let cte_info = CteInfo {
                columns_alias: column_name,
                query: *cte.query.clone(),
                recursive: with.recursive,
                cte_idx: idx,
                columns: vec![],
                materialized: cte.materialized,
            };
            // If the CTE is materialized, we'll construct a temp table for it.
            if cte.materialized {
                self.m_cte_to_temp_table(cte)?;
            }
            bind_context
                .cte_context
                .cte_map
                .insert(table_name, cte_info);
        }

        Ok(())
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

        let default_nulls_first = self.ctx.get_settings().get_nulls_first();

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

    // The return value is temp_table name`
    fn m_cte_to_temp_table(&self, cte: &CTE) -> Result<()> {
        let engine = if self.ctx.get_settings().get_persist_materialized_cte()? {
            Engine::Fuse
        } else {
            Engine::Memory
        };
        let database = self.ctx.get_current_database();
        let table_name = normalize_identifier(&cte.alias.name, &self.name_resolution_ctx).name;
        if self
            .ctx
            .is_temp_table(CATALOG_DEFAULT, &database, &table_name)
        {
            return Err(ErrorCode::Internal(format!(
                "Temporary table {:?} already exists in current session, please change the materialized CTE name",
                table_name
            )));
        }
        let create_table_stmt = CreateTableStmt {
            create_option: CreateOption::CreateOrReplace,
            catalog: Some(Identifier::from_name(Span::None, CATALOG_DEFAULT)),
            database: Some(Identifier::from_name(Span::None, database.clone())),
            table: cte.alias.name.clone(),
            source: None,
            engine: Some(engine),
            uri_location: None,
            cluster_by: None,
            table_options: Default::default(),
            as_query: Some(cte.query.clone()),
            table_type: TableType::Temporary,
        };

        let create_table_sql = create_table_stmt.to_string();
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
            .remove_table_from_cache(CATALOG_DEFAULT, &database, &table_name);
        Ok(())
    }
}
