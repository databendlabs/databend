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
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::ExprContext;
use crate::BindContext;
use crate::Symbol;
use crate::binder::Binder;
use crate::binder::aggregate::AggregateRewriter;
use crate::binder::project::SelectInfo;
use crate::binder::scalar::ScalarBinder;
use crate::binder::select::SelectClauseFact;
use crate::binder::window::WindowRewriter;
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
use crate::plans::VisitorMut;

#[derive(Debug)]
pub struct OrderItems {
    pub(crate) items: Vec<OrderItem>,
}

#[derive(Debug)]
pub struct OrderItem {
    pub index: Symbol,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug)]
struct OrderByRewriteFlags {
    needs_recursive_rewrite: bool,
    needs_select_item_replacement: bool,
    needs_aggregate_rewrite: bool,
    needs_window_rewrite: bool,
    needs_post_aggregate_rewrite: bool,
}

impl OrderByRewriteFlags {
    const fn no_rewrite() -> Self {
        Self {
            needs_recursive_rewrite: false,
            needs_select_item_replacement: false,
            needs_aggregate_rewrite: false,
            needs_window_rewrite: false,
            needs_post_aggregate_rewrite: false,
        }
    }

    fn from_select_clause_fact(fact: &SelectClauseFact) -> Self {
        let needs_select_item_replacement = fact.needs_order_by_select_item_replacement();
        let needs_aggregate_rewrite = fact.contains_or_references_aggregate();
        let needs_window_rewrite = fact.contains_or_references_window();
        Self {
            needs_recursive_rewrite: needs_select_item_replacement
                || needs_aggregate_rewrite
                || needs_window_rewrite,
            needs_select_item_replacement,
            needs_aggregate_rewrite,
            needs_window_rewrite,
            needs_post_aggregate_rewrite: needs_select_item_replacement && needs_aggregate_rewrite,
        }
    }
}

impl Binder {
    pub(crate) fn analyze_order_items(
        &mut self,
        bind_context: &mut BindContext,
        select_info: &mut SelectInfo,
        aliases: &[(String, ScalarExpr)],
        order_by_facts: Option<&[SelectClauseFact]>,
        order_by: &[OrderByExpr],
        distinct: bool,
    ) -> Result<OrderItems> {
        bind_context.expr_context = ExprContext::OrderByClause;
        let settings = self.ctx.get_settings();
        let default_nulls_first = settings.get_nulls_first();

        let rewrite_flags = if let Some(order_by_facts) = order_by_facts {
            assert_eq!(
                order_by_facts.len(),
                order_by.len(),
                "ORDER BY facts must align with ORDER BY expressions",
            );
            order_by_facts
                .iter()
                .map(OrderByRewriteFlags::from_select_clause_fact)
                .collect()
        } else {
            vec![OrderByRewriteFlags::no_rewrite(); order_by.len()]
        };

        let order_items = order_by
            .iter()
            .zip(rewrite_flags)
            .filter_map(|(order, flags)| match &order.expr {
                Expr::Literal {
                    value: Literal::UInt64(index),
                    ..
                } => {
                    let index = *index as usize;
                    let projection_column = if index > 0
                        && let Some(projection_column) = select_info.column_at(index - 1)
                    {
                        projection_column
                    } else {
                        return Some(Err(ErrorCode::SemanticError(format!(
                            "ORDER BY position {index} is not in select list",
                        ))
                        .set_span(order.expr.span())));
                    };

                    let asc = order.asc.unwrap_or(true);
                    Some(Ok(OrderItem {
                        index: projection_column.index,
                        asc,
                        nulls_first: order
                            .nulls_first
                            .unwrap_or_else(|| default_nulls_first(asc)),
                    }))
                }
                _ => {
                    let mut scalar_binder = ScalarBinder::new(
                        bind_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        aliases,
                    );
                    let (bound_expr, _) = match scalar_binder.bind(&order.expr) {
                        Ok(bound_expr) => bound_expr,
                        Err(err) => return Some(Err(err)),
                    };

                    if let Some((idx, (_, scalar))) = aliases
                        .iter()
                        .enumerate()
                        .find(|(_, (_, scalar))| bound_expr.eq(scalar))
                    {
                        if bind_context.in_grouping {
                            let mut group_checker = GroupingChecker::new(bind_context, None);
                            let mut scalar = scalar.clone();
                            if let Err(err) = group_checker.visit(&mut scalar) {
                                return Some(Err(err));
                            }
                        }

                        // The order by expression is in the select list.
                        let asc = order.asc.unwrap_or(true);
                        return Some(Ok(OrderItem {
                            index: select_info.column_at(idx).unwrap().index,
                            asc,
                            nulls_first: order
                                .nulls_first
                                .unwrap_or_else(|| default_nulls_first(asc)),
                        }));
                    }

                    if distinct {
                        return Some(Err(ErrorCode::SemanticError(
                            "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                                .to_string(),
                        )));
                    }
                    let mut rewrite_scalar = if flags.needs_recursive_rewrite {
                        match self.rewrite_scalar_with_replacement(
                            bind_context,
                            select_info,
                            &bound_expr,
                            flags,
                        ) {
                            Ok(scalar) => scalar,
                            Err(err) => {
                                return Some(Err(ErrorCode::SemanticError(err.message())));
                            }
                        }
                    } else {
                        bound_expr
                    };

                    if flags.needs_post_aggregate_rewrite
                        && let Err(err) = AggregateRewriter::rewrite_expr(
                            &mut bind_context.aggregate_info,
                            self.metadata.clone(),
                            &mut rewrite_scalar,
                        )
                    {
                        return Some(Err(err));
                    }

                    let column_binding = match &rewrite_scalar {
                        ScalarExpr::ConstantExpr(..) => return None,
                        ScalarExpr::BoundColumnRef(col) => col.column.clone(),
                        _ => match rewrite_scalar.data_type() {
                            Ok(data_type) => self.create_derived_column_binding(
                                format!("{:#}", order.expr),
                                data_type,
                            ),
                            Err(err) => return Some(Err(err)),
                        },
                    };

                    let item = ScalarItem {
                        scalar: rewrite_scalar,
                        index: column_binding.index,
                    };
                    let projection_item = match self.prepare_select_output_item(bind_context, &item)
                    {
                        Ok(projection_item) => projection_item,
                        Err(err) => return Some(Err(err)),
                    };
                    select_info.insert_scalar(item, projection_item);
                    let asc = order.asc.unwrap_or(true);
                    Some(Ok(OrderItem {
                        index: column_binding.index,
                        asc,
                        nulls_first: order
                            .nulls_first
                            .unwrap_or_else(|| default_nulls_first(asc)),
                    }))
                }
            })
            .collect::<Result<_>>()?;
        Ok(OrderItems { items: order_items })
    }

    pub fn bind_order_by(&mut self, order_by: OrderItems, child: SExpr) -> Result<SExpr> {
        let mut order_by_items = Vec::with_capacity(order_by.items.len());
        for order in order_by.items {
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

    fn rewrite_scalar_with_replacement(
        &self,
        bind_context: &mut BindContext,
        select_info: &SelectInfo,
        original_scalar: &ScalarExpr,
        rewrite_flags: OrderByRewriteFlags,
    ) -> Result<ScalarExpr> {
        if rewrite_flags.needs_select_item_replacement
            && let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = original_scalar
            && let Some(replacement) = select_info
                .source_scalar_item(column.index)
                .map(|scalar_item| scalar_item.scalar.clone())
        {
            return Ok(replacement);
        }

        match original_scalar {
            aggregate @ ScalarExpr::AggregateFunction(_) => {
                if !rewrite_flags.needs_aggregate_rewrite {
                    return Ok(aggregate.clone());
                }
                let mut aggregate = aggregate.clone();
                AggregateRewriter::rewrite_expr(
                    &mut bind_context.aggregate_info,
                    self.metadata.clone(),
                    &mut aggregate,
                )?;
                Ok(aggregate)
            }
            udaf @ ScalarExpr::UDAFCall(_) => {
                if !rewrite_flags.needs_aggregate_rewrite {
                    return Ok(udaf.clone());
                }
                let mut udaf = udaf.clone();
                AggregateRewriter::rewrite_expr(
                    &mut bind_context.aggregate_info,
                    self.metadata.clone(),
                    &mut udaf,
                )?;
                Ok(udaf)
            }
            ScalarExpr::LambdaFunction(lambda_func) => {
                let args = lambda_func
                    .args
                    .iter()
                    .map(|arg| {
                        self.rewrite_scalar_with_replacement(
                            bind_context,
                            select_info,
                            arg,
                            rewrite_flags,
                        )
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
                if !rewrite_flags.needs_window_rewrite {
                    return Ok(window.clone());
                }
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
                        self.rewrite_scalar_with_replacement(
                            bind_context,
                            select_info,
                            arg,
                            rewrite_flags,
                        )
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
                    select_info,
                    argument,
                    rewrite_flags,
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
                        self.rewrite_scalar_with_replacement(
                            bind_context,
                            select_info,
                            arg,
                            rewrite_flags,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    handler: udf.handler.clone(),
                    headers: udf.headers.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: new_args,
                }
                .into())
            }
            _ => Ok(original_scalar.clone()),
        }
    }
}
