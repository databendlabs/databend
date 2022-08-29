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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::OrderByExpr;
use common_ast::DisplayError;
use common_exception::ErrorCode;
use common_exception::Result;

use super::bind_context::NameResolutionResult;
use crate::sessions::TableContext;
use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::select::SelectList;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::normalize_identifier;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarItem;
use crate::sql::plans::Sort;
use crate::sql::plans::SortItem;
use crate::sql::BindContext;
use crate::sql::IndexType;
use crate::sql::ScalarExpr;

pub struct OrderItems<'a> {
    items: Vec<OrderItem<'a>>,
}

pub struct OrderItem<'a> {
    pub expr: OrderByExpr<'a>,
    pub index: IndexType,
    pub name: String,
    // True if item need to wrap EvalScalar plan.
    pub need_eval_scalar: bool,
}

impl<'a> Binder {
    pub(super) async fn analyze_order_items(
        &mut self,
        from_context: &BindContext,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        projections: &[ColumnBinding],
        order_by: &'a [OrderByExpr<'a>],
        distinct: bool,
    ) -> Result<OrderItems<'a>> {
        let mut order_items = Vec::with_capacity(order_by.len());
        for order in order_by {
            match &order.expr {
                Expr::ColumnRef {
                    database: ref database_name,
                    table: ref table_name,
                    column: ref ident,
                    ..
                } => {
                    // We first search the identifier in select list
                    let mut found = false;
                    let database = database_name
                        .as_ref()
                        .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name);
                    let table = table_name
                        .as_ref()
                        .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name);
                    let column = normalize_identifier(ident, &self.name_resolution_ctx).name;

                    for item in projections.iter() {
                        if BindContext::match_column_binding(
                            database.as_deref(),
                            table.as_deref(),
                            column.as_str(),
                            item,
                        ) {
                            order_items.push(OrderItem {
                                expr: order.clone(),
                                index: item.index,
                                name: item.column_name.clone(),
                                need_eval_scalar: scalar_items.get(&item.index).map_or(
                                    false,
                                    |scalar_item| {
                                        !matches!(&scalar_item.scalar, Scalar::BoundColumnRef(_))
                                    },
                                ),
                            });
                            found = true;
                            break;
                        }
                    }

                    if found {
                        continue;
                    }

                    // If there isn't a matched alias in select list, we will fallback to
                    // from clause.
                    let result = from_context.resolve_name(
                        database.as_deref(),
                        table.as_deref(),
                        &column,
                        &ident.span,
                       &[])
                    .and_then(|v| {
                        if distinct {
                            Err(ErrorCode::SemanticError(order.expr.span().display_error("for SELECT DISTINCT, ORDER BY expressions must appear in select list".to_string())))
                        } else {
                            Ok(v)
                        }
                    })?;
                    match result {
                        NameResolutionResult::Column(column) => {
                            order_items.push(OrderItem {
                                expr: order.clone(),
                                name: column.column_name.clone(),
                                index: column.index,
                                need_eval_scalar: false,
                            });
                        }
                        NameResolutionResult::Alias { .. } => {
                            return Err(ErrorCode::LogicalError("Invalid name resolution result"));
                        }
                    }
                }
                Expr::Literal {
                    lit: Literal::Integer(index),
                    ..
                } => {
                    let index = *index as usize - 1;
                    if index >= projections.len() {
                        return Err(ErrorCode::SemanticError(order.expr.span().display_error(
                            format!("ORDER BY position {} is not in select list", index + 1),
                        )));
                    }
                    order_items.push(OrderItem {
                        expr: order.clone(),
                        name: projections[index].column_name.clone(),
                        index: projections[index].index,
                        need_eval_scalar: false,
                    });
                }
                _ => {
                    let mut bind_context = from_context.clone();
                    for column_binding in projections.iter() {
                        if bind_context.columns.contains(column_binding) {
                            continue;
                        }
                        bind_context.columns.push(column_binding.clone());
                    }
                    let mut scalar_binder = ScalarBinder::new(
                        &bind_context,
                        self.ctx.clone(),
                        &self.name_resolution_ctx,
                        self.metadata.clone(),
                        &[],
                    );
                    let (bound_expr, _) = scalar_binder.bind(&order.expr).await?;
                    let rewrite_scalar = self
                        .rewrite_scalar_with_replacement(&bound_expr, &|nest_scalar| {
                            if let Scalar::BoundColumnRef(BoundColumnRef { column }) = nest_scalar {
                                if let Some(scalar_item) = scalar_items.get(&column.index) {
                                    return Ok(Some(scalar_item.scalar.clone()));
                                }
                            }
                            Ok(None)
                        })
                        .map_err(|e| ErrorCode::SemanticError(e.message()))?;
                    let column_binding = self.create_column_binding(
                        None,
                        None,
                        format!("{:#}", order.expr),
                        rewrite_scalar.data_type(),
                    );
                    order_items.push(OrderItem {
                        expr: order.clone(),
                        name: column_binding.column_name.clone(),
                        index: column_binding.index,
                        need_eval_scalar: true,
                    });
                    scalar_items.insert(column_binding.index, ScalarItem {
                        scalar: rewrite_scalar,
                        index: column_binding.index,
                    });
                }
            }
        }
        Ok(OrderItems { items: order_items })
    }

    pub(super) async fn bind_order_by(
        &mut self,
        from_context: &BindContext,
        order_by: OrderItems<'a>,
        select_list: &'a SelectList<'a>,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut order_by_items = Vec::with_capacity(order_by.items.len());
        let mut scalars = vec![];
        for order in order_by.items {
            if from_context.in_grouping {
                let mut group_checker = GroupingChecker::new(from_context);
                // Perform grouping check on original scalar expression if order item is alias.
                if let Some(scalar_item) = select_list
                    .items
                    .iter()
                    .find(|item| item.alias == order.name)
                {
                    group_checker.resolve(&scalar_item.scalar, None)?;
                }
            }
            if let Expr::ColumnRef {
                database: ref database_name,
                table: ref table_name,
                ..
            } = order.expr.expr
            {
                if let (Some(table_name), Some(database_name)) = (table_name, database_name) {
                    let catalog_name = self.ctx.get_current_catalog();
                    let catalog = self.ctx.get_catalog(catalog_name.as_str())?;
                    catalog
                        .get_table(
                            &self.ctx.get_tenant(),
                            &database_name.name,
                            &table_name.name,
                        )
                        .await?;
                }
            }
            if order.need_eval_scalar {
                if let Entry::Occupied(entry) = scalar_items.entry(order.index) {
                    let (index, item) = entry.remove_entry();
                    let mut scalar = item.scalar;

                    if from_context.in_grouping {
                        let mut group_checker = GroupingChecker::new(from_context);
                        scalar = group_checker.resolve(&scalar, None)?;
                    }
                    scalars.push(ScalarItem { scalar, index });
                }
            }
            let order_by_item = SortItem {
                index: order.index,
                asc: order.expr.asc.unwrap_or(true),
                nulls_first: order.expr.nulls_first.unwrap_or(false),
            };
            order_by_items.push(order_by_item);
        }

        let mut new_expr = if !scalars.is_empty() {
            let eval_scalar = EvalScalar { items: scalars };
            SExpr::create_unary(eval_scalar.into(), child)
        } else {
            child
        };

        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
        };
        new_expr = SExpr::create_unary(sort_plan.into(), new_expr);
        Ok(new_expr)
    }

    pub(crate) async fn bind_order_by_for_set_operation(
        &mut self,
        bind_context: &BindContext,
        child: SExpr,
        order_by: &[OrderByExpr<'_>],
    ) -> Result<SExpr> {
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let mut order_by_items = Vec::with_capacity(order_by.len());
        for order in order_by.iter() {
            match order.expr {
                Expr::ColumnRef { .. } => {
                    let scalar = scalar_binder.bind(&order.expr).await?.0;
                    match scalar {
                        Scalar::BoundColumnRef(BoundColumnRef { column }) => {
                            let order_by_item = SortItem {
                                index: column.index,
                                asc: order.asc.unwrap_or(true),
                                nulls_first: order.nulls_first.unwrap_or(false),
                            };
                            order_by_items.push(order_by_item);
                        }
                        _ => {
                            return Err(ErrorCode::LogicalError("scalar should be BoundColumnRef"));
                        }
                    }
                }
                _ => {
                    return Err(ErrorCode::SemanticError(
                        order
                            .expr
                            .span()
                            .display_error("can only order by column".to_string()),
                    ));
                }
            }
        }
        let sort_plan = Sort {
            items: order_by_items,
            limit: None,
        };
        Ok(SExpr::create_unary(sort_plan.into(), child))
    }

    fn rewrite_scalar_with_replacement<F>(
        &self,
        original_scalar: &Scalar,
        replacement_fn: &F,
    ) -> Result<Scalar>
    where
        F: Fn(&Scalar) -> Result<Option<Scalar>>,
    {
        let replacement_opt = replacement_fn(original_scalar)?;
        match replacement_opt {
            Some(replacement) => Ok(replacement),
            None => match original_scalar {
                Scalar::AndExpr(AndExpr {
                    left,
                    right,
                    return_type,
                }) => {
                    let left =
                        Box::new(self.rewrite_scalar_with_replacement(left, replacement_fn)?);
                    let right =
                        Box::new(self.rewrite_scalar_with_replacement(right, replacement_fn)?);
                    Ok(Scalar::AndExpr(AndExpr {
                        left,
                        right,
                        return_type: return_type.clone(),
                    }))
                }
                Scalar::OrExpr(OrExpr {
                    left,
                    right,
                    return_type,
                }) => {
                    let left =
                        Box::new(self.rewrite_scalar_with_replacement(left, replacement_fn)?);
                    let right =
                        Box::new(self.rewrite_scalar_with_replacement(right, replacement_fn)?);
                    Ok(Scalar::OrExpr(OrExpr {
                        left,
                        right,
                        return_type: return_type.clone(),
                    }))
                }
                Scalar::ComparisonExpr(ComparisonExpr {
                    op,
                    left,
                    right,
                    return_type,
                }) => {
                    let left =
                        Box::new(self.rewrite_scalar_with_replacement(left, replacement_fn)?);
                    let right =
                        Box::new(self.rewrite_scalar_with_replacement(right, replacement_fn)?);
                    Ok(Scalar::ComparisonExpr(ComparisonExpr {
                        op: op.clone(),
                        left,
                        right,
                        return_type: return_type.clone(),
                    }))
                }
                Scalar::AggregateFunction(AggregateFunction {
                    display_name,
                    func_name,
                    distinct,
                    params,
                    args,
                    return_type,
                }) => {
                    let args = args
                        .iter()
                        .map(|arg| self.rewrite_scalar_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Scalar::AggregateFunction(AggregateFunction {
                        display_name: display_name.clone(),
                        func_name: func_name.clone(),
                        distinct: *distinct,
                        params: params.clone(),
                        args,
                        return_type: return_type.clone(),
                    }))
                }
                Scalar::FunctionCall(FunctionCall {
                    arguments,
                    func_name,
                    arg_types,
                    return_type,
                }) => {
                    let arguments = arguments
                        .iter()
                        .map(|arg| self.rewrite_scalar_with_replacement(arg, replacement_fn))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Scalar::FunctionCall(FunctionCall {
                        arguments,
                        func_name: func_name.clone(),
                        arg_types: arg_types.clone(),
                        return_type: return_type.clone(),
                    }))
                }
                Scalar::CastExpr(CastExpr {
                    argument,
                    from_type,
                    target_type,
                }) => {
                    let argument =
                        Box::new(self.rewrite_scalar_with_replacement(argument, replacement_fn)?);
                    Ok(Scalar::CastExpr(CastExpr {
                        argument,
                        from_type: from_type.clone(),
                        target_type: target_type.clone(),
                    }))
                }
                _ => Ok(original_scalar.clone()),
            },
        }
    }
}
