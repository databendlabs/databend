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

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::OrderByExpr;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SelectTarget;
use common_ast::ast::SetExpr;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar_common::split_conjunctions;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::Scalar;

// A normalized IR for `SELECT` clause.
#[derive(Debug, Default)]
pub struct SelectList<'a> {
    pub items: Vec<SelectItem<'a>>,
}

#[derive(Debug)]
pub struct SelectItem<'a> {
    pub select_target: &'a SelectTarget<'a>,
    pub scalar: Scalar,
    pub alias: String,
}

impl<'a> Binder {
    pub(super) async fn bind_select_stmt(
        &mut self,
        bind_context: &BindContext,
        stmt: &SelectStmt<'a>,
        order_by: &[OrderByExpr<'a>],
    ) -> Result<(SExpr, BindContext)> {
        let (mut s_expr, mut from_context) = if let Some(from) = &stmt.from {
            self.bind_table_reference(bind_context, from).await?
        } else {
            self.bind_one_table(bind_context, stmt).await?
        };

        if let Some(expr) = &stmt.selection {
            s_expr = self.bind_where(&from_context, expr, s_expr).await?;
        }

        // Generate a analyzed select list with from context
        let mut select_list = self
            .normalize_select_list(&from_context, &stmt.select_list)
            .await?;

        let (mut scalar_items, projections) = self.analyze_projection(&select_list)?;

        self.analyze_aggregate_select(&mut from_context, &mut select_list)?;
        let having = if let Some(having) = &stmt.having {
            Some(
                self.analyze_aggregate_having(&mut from_context, having)
                    .await?,
            )
        } else {
            None
        };
        let order_items = self.analyze_order_items(
            &from_context,
            &scalar_items,
            &projections,
            order_by,
            stmt.distinct,
        )?;

        if !from_context.aggregate_info.aggregate_functions.is_empty()
            || !stmt.group_by.is_empty()
            || stmt.having.is_some()
        {
            s_expr = self
                .bind_aggregate(&mut from_context, &select_list, &stmt.group_by, s_expr)
                .await?;

            if let Some(having) = having {
                s_expr = self.bind_having(&from_context, having, s_expr).await?;
            }
        }

        if stmt.distinct {
            s_expr = self.bind_distinct(&from_context, &projections, &mut scalar_items, s_expr)?;
        }

        if !order_by.is_empty() {
            s_expr = self
                .bind_order_by(&from_context, order_items, &mut scalar_items, s_expr)
                .await?;
        }

        s_expr = self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;

        Ok((s_expr, from_context))
    }

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

    pub(super) async fn bind_where(
        &mut self,
        bind_context: &BindContext,
        expr: &Expr<'a>,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut scalar_binder =
            ScalarBinder::new(bind_context, self.ctx.clone(), self.metadata.clone());
        let (scalar, _) = scalar_binder.bind(expr).await?;
        let filter_plan = FilterPlan {
            predicates: split_conjunctions(&scalar),
            is_having: false,
        };
        let new_expr = SExpr::create_unary(filter_plan.into(), child);
        Ok(new_expr)
    }
}
