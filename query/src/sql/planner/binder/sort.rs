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
use common_ast::ast::OrderByExpr;
use common_ast::parser::error::DisplayError;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::semantic::GroupingChecker;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarItem;
use crate::sql::plans::SortItem;
use crate::sql::plans::SortPlan;
use crate::sql::BindContext;
use crate::sql::IndexType;

pub struct OrderItems<'a> {
    items: Vec<OrderItem<'a>>,
}

pub struct OrderItem<'a> {
    pub order_expr: &'a OrderByExpr<'a>,
    pub index: IndexType,
    pub name: String,
    // True if item reference a alias scalar expression in select list
    pub need_project: bool,
}

impl<'a> Binder {
    pub(super) fn analyze_order_items(
        &mut self,
        from_context: &BindContext,
        scalar_items: &HashMap<IndexType, ScalarItem>,
        projections: &[ColumnBinding],
        order_by: &'a [OrderByExpr<'a>],
        distinct: bool,
    ) -> Result<OrderItems<'a>> {
        let mut order_items = Vec::with_capacity(order_by.len());
        for order in order_by {
            if let Expr::ColumnRef {
                database: None,
                table: None,
                column: ref ident,
                ..
            } = order.expr
            {
                // We first search the identifier in select list
                let mut found = false;
                for item in projections.iter() {
                    if item.column_name == ident.name {
                        order_items.push(OrderItem {
                            order_expr: order,
                            index: item.index,
                            name: item.column_name.clone(),
                            need_project: scalar_items.get(&item.index).map_or(
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
                let column = from_context.resolve_column(None, ident).and_then(|v| {
                    if distinct {
                        Err(ErrorCode::SemanticError(order.expr.span().display_error("for SELECT DISTINCT, ORDER BY expressions must appear in select list".to_string())))
                    } else {
                        Ok(v)
                    }
                })?;
                order_items.push(OrderItem {
                    order_expr: order,
                    name: column.column_name.clone(),
                    index: column.index,
                    need_project: false,
                });
            } else {
                return Err(ErrorCode::SemanticError(
                    order
                        .expr
                        .span()
                        .display_error("can only order by column".to_string()),
                ));
            }
        }
        Ok(OrderItems { items: order_items })
    }

    pub(super) async fn bind_order_by(
        &mut self,
        from_context: &BindContext,
        order_by: OrderItems<'a>,
        scalar_items: &mut HashMap<IndexType, ScalarItem>,
        child: SExpr,
    ) -> Result<SExpr> {
        let mut order_by_items = Vec::with_capacity(order_by.items.len());
        let mut scalars = vec![];
        for order in order_by.items {
            if order.need_project {
                if let Entry::Occupied(entry) = scalar_items.entry(order.index) {
                    let (index, item) = entry.remove_entry();
                    let mut scalar = item.scalar;

                    if from_context.in_grouping {
                        let mut group_checker = GroupingChecker::new(from_context);
                        scalar = group_checker.resolve(&scalar)?;
                    }
                    scalars.push(ScalarItem { scalar, index });
                }
            }
            let order_by_item = SortItem {
                index: order.index,
                asc: order.order_expr.asc,
                nulls_first: order.order_expr.nulls_first,
            };
            order_by_items.push(order_by_item);
        }

        let mut new_expr = if !scalars.is_empty() {
            let eval_scalar = EvalScalar { items: scalars };
            SExpr::create_unary(eval_scalar.into(), child)
        } else {
            child
        };

        let sort_plan = SortPlan {
            items: order_by_items,
        };
        new_expr = SExpr::create_unary(sort_plan.into(), new_expr);
        Ok(new_expr)
    }
}
