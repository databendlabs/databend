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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::ExprContext;
use crate::binder::aggregate::AggregateRewriter;
use crate::binder::scalar::ScalarBinder;
use crate::binder::select::SelectList;
use crate::binder::window::WindowRewriter;
use crate::binder::Binder;
use crate::binder::ColumnBinding;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::GroupingChecker;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::plans::UDFCall;
use crate::plans::VisitorMut as _;
use crate::BindContext;
use crate::IndexType;

#[derive(Debug)]
pub struct OrderItems {
    pub(crate) items: Vec<OrderItem>,
}

#[derive(Debug)]
pub struct OrderItem {
    pub index: IndexType,
    pub asc: bool,
    pub nulls_first: bool,
    pub name: String,
}

impl Binder {
    pub fn analyze_order_items(
        &mut self,
        bind_context: &mut BindContext,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        aliases: &[(String, ScalarExpr)],
        projections: &[ColumnBinding],
        order_by: &[OrderByExpr],
        distinct: bool,
    ) -> Result<OrderItems> {
        bind_context.set_expr_context(ExprContext::OrderByClause);
        let default_nulls_first = self.ctx.get_settings().get_nulls_first();

        let mut order_items = Vec::with_capacity(order_by.len());
        for order in order_by {
            match &order.expr {
                Expr::Literal {
                    value: Literal::UInt64(index),
                    ..
                } => {
                    let index = *index as usize;
                    if index == 0 || index > projections.len() {
                        return Err(ErrorCode::SemanticError(format!(
                            "ORDER BY position {} is not in select list",
                            index
                        ))
                        .set_span(order.expr.span()));
                    }

                    let index = index - 1;
                    let projection = &projections[index];

                    let asc = order.asc.unwrap_or(true);
                    order_items.push(OrderItem {
                        index: projection.index,
                        name: projection.column_name.clone(),
                        asc,
                        nulls_first: order
                            .nulls_first
                            .unwrap_or_else(|| default_nulls_first(asc)),
                    });
                }
                _ => {
                    let mut scalar_binder = ScalarBinder::new(
                        bind_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        aliases,
                    );
                    let (bound_expr, _) = scalar_binder.bind(&order.expr)?;

                    if let Some((idx, (alias, _))) = aliases
                        .iter()
                        .enumerate()
                        .find(|(_, (_, scalar))| bound_expr.eq(scalar))
                    {
                        // The order by expression is in the select list.
                        let asc = order.asc.unwrap_or(true);
                        order_items.push(OrderItem {
                            index: projections[idx].index,
                            name: alias.clone(),
                            asc,
                            nulls_first: order
                                .nulls_first
                                .unwrap_or_else(|| default_nulls_first(asc)),
                        });
                    } else if distinct {
                        return Err(ErrorCode::SemanticError(
                            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                                .to_string(),
                        ));
                    } else {
                        let mut rewrite_scalar = self
                            .rewrite_scalar_with_replacement(
                                bind_context,
                                &bound_expr,
                                &|nest_scalar| {
                                    if let ScalarExpr::BoundColumnRef(BoundColumnRef {
                                        column,
                                        ..
                                    }) = nest_scalar
                                    {
                                        if let Some(scalar_item) = scalar_items.get(&column.index) {
                                            return Ok(Some(scalar_item.scalar.clone()));
                                        }
                                    }
                                    Ok(None)
                                },
                            )
                            .map_err(|e| ErrorCode::SemanticError(e.message()))?;

                        let mut rewriter =
                            AggregateRewriter::new(bind_context, self.metadata.clone());
                        rewriter.visit(&mut rewrite_scalar)?;

                        if let ScalarExpr::ConstantExpr(..) = rewrite_scalar {
                            continue;
                        }

                        let column_binding =
                            if let ScalarExpr::BoundColumnRef(col) = &rewrite_scalar {
                                col.column.clone()
                            } else {
                                self.create_derived_column_binding(
                                    format!("{:#}", order.expr),
                                    rewrite_scalar.data_type()?,
                                    Some(rewrite_scalar.clone()),
                                )
                            };
                        let item = ScalarItem {
                            scalar: rewrite_scalar,
                            index: column_binding.index,
                        };
                        scalar_items.insert(column_binding.index, item);
                        let asc = order.asc.unwrap_or(true);
                        order_items.push(OrderItem {
                            index: column_binding.index,
                            name: column_binding.column_name,
                            asc,
                            nulls_first: order
                                .nulls_first
                                .unwrap_or_else(|| default_nulls_first(asc)),
                        });
                    }
                }
            }
        }
        Ok(OrderItems { items: order_items })
    }

    pub fn bind_order_by(
        &mut self,
        from_context: &BindContext,
        order_by: OrderItems,
        select_list: &SelectList<'_>,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut order_by_items = Vec::with_capacity(order_by.items.len());
        for order in order_by.items {
            if from_context.in_grouping {
                let mut group_checker = GroupingChecker::new(from_context);
                // Perform grouping check on original scalar expression if order item is alias.
                if let Some(scalar_item) = select_list
                    .items
                    .iter()
                    .find(|item| item.alias == order.name)
                {
                    let mut scalar = scalar_item.scalar.clone();
                    group_checker.visit(&mut scalar)?;
                }
            }

            let order_by_item = SortItem {
                index: order.index,
                asc: order.asc,
                nulls_first: order.nulls_first,
            };

            order_by_items.push(order_by_item);
        }

        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
            after_exchange: None,
            pre_projection: None,
            window_partition: None,
        };
        let new_expr = SExpr::create_unary(Arc::new(sort_plan.into()), Arc::new(child));
        Ok(new_expr)
    }

    #[allow(clippy::only_used_in_recursion)]
    pub(crate) fn rewrite_scalar_with_replacement<F>(
        &self,
        bind_context: &mut BindContext,
        original_scalar: &ScalarExpr,
        replacement_fn: &F,
    ) -> Result<ScalarExpr>
    where
        F: Fn(&ScalarExpr) -> Result<Option<ScalarExpr>>,
    {
        let replacement_opt = replacement_fn(original_scalar)?;
        match replacement_opt {
            Some(replacement) => Ok(replacement),
            None => match original_scalar {
                aggregate @ ScalarExpr::AggregateFunction(_) => {
                    let mut aggregate = aggregate.clone();
                    let mut rewriter = AggregateRewriter::new(bind_context, self.metadata.clone());
                    rewriter.visit(&mut aggregate)?;
                    Ok(aggregate)
                }
                ScalarExpr::LambdaFunction(lambda_func) => {
                    let args = lambda_func
                        .args
                        .iter()
                        .map(|arg| {
                            self.rewrite_scalar_with_replacement(bind_context, arg, replacement_fn)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ScalarExpr::LambdaFunction(LambdaFunc {
                        span: lambda_func.span,
                        func_name: lambda_func.func_name.clone(),
                        args,
                        lambda_expr: lambda_func.lambda_expr.clone(),
                        lambda_display: lambda_func.lambda_display.clone(),
                        return_type: lambda_func.return_type.clone(),
                    }))
                }
                window @ ScalarExpr::WindowFunction(_) => {
                    let mut window = window.clone();
                    let mut rewriter = WindowRewriter::new(bind_context, self.metadata.clone());
                    rewriter.visit(&mut window)?;
                    Ok(window)
                }
                ScalarExpr::FunctionCall(func) => {
                    let arguments = func
                        .arguments
                        .iter()
                        .map(|arg| {
                            self.rewrite_scalar_with_replacement(bind_context, arg, replacement_fn)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(ScalarExpr::FunctionCall(FunctionCall {
                        span: func.span,
                        func_name: func.func_name.clone(),
                        params: func.params.clone(),
                        arguments,
                    }))
                }
                ScalarExpr::CastExpr(CastExpr {
                    span,
                    is_try,
                    argument,
                    target_type,
                }) => {
                    let argument = Box::new(self.rewrite_scalar_with_replacement(
                        bind_context,
                        argument,
                        replacement_fn,
                    )?);
                    Ok(ScalarExpr::CastExpr(CastExpr {
                        span: *span,
                        is_try: *is_try,
                        argument,
                        target_type: target_type.clone(),
                    }))
                }
                ScalarExpr::UDFCall(udf) => {
                    let new_args = udf
                        .arguments
                        .iter()
                        .map(|arg| {
                            self.rewrite_scalar_with_replacement(bind_context, arg, replacement_fn)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(UDFCall {
                        span: udf.span,
                        name: udf.name.clone(),
                        handler: udf.handler.clone(),
                        display_name: udf.display_name.clone(),
                        udf_type: udf.udf_type.clone(),
                        arg_types: udf.arg_types.clone(),
                        return_type: udf.return_type.clone(),
                        arguments: new_args,
                    }
                    .into())
                }
                _ => Ok(original_scalar.clone()),
            },
        }
    }
}
