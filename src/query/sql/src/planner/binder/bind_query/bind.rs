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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::With;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::CteInfo;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::SortItem;

impl Binder {
    #[async_backtrace::framed]
    pub(crate) async fn bind_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        // Initialize cte map.
        self.init_cte(bind_context, &query.with)?;

        // Extract limit and offset from query.
        let (limit, offset) = self.extract_limit_and_offset(query)?;

        // Bind query body.
        let (mut s_expr, mut bind_context) = Box::pin(self.bind_set_expr(
            bind_context,
            &query.body,
            &query.order_by,
            limit.unwrap_or_default(),
        ))
        .await?;

        // Bind order by for `SetOperation` and `Values`.
        s_expr = self
            .bind_query_order_by(&mut bind_context, query, s_expr)
            .await?;

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
            if bind_context.cte_map_ref.contains_key(&table_name) {
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
                materialized: cte.materialized,
                recursive: with.recursive,
                cte_idx: idx,
                used_count: 0,
                columns: vec![],
            };
            self.ctes_map.insert(table_name.clone(), cte_info.clone());
            bind_context.cte_map_ref.insert(table_name, cte_info);
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_query_order_by(
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
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let mut order_by_items = Vec::with_capacity(query.order_by.len());
        for order in query.order_by.iter() {
            match order.expr {
                Expr::ColumnRef { .. } => {
                    let scalar = scalar_binder.bind(&order.expr).await?.0;
                    match scalar {
                        ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) => {
                            let order_by_item = SortItem {
                                index: column.index,
                                asc: order.asc.unwrap_or(true),
                                nulls_first: order.nulls_first.unwrap_or(false),
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
        };
        Ok(SExpr::create_unary(
            Arc::new(sort_plan.into()),
            Arc::new(child),
        ))
    }
}
