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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::BindContext;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;
use crate::plans::walk_expr_mut;

/// Check validity of scalar expression in a grouping context.
/// The matched grouping item will be replaced with a BoundColumnRef
/// to corresponding grouping item column.
///
/// Also replaced the matched window function with a BoundColumnRef.
pub struct GroupingChecker<'a> {
    bind_context: &'a BindContext,
    forbid_aggregate: Option<&'static str>,
}

impl<'a> GroupingChecker<'a> {
    pub fn new(bind_context: &'a BindContext, forbid_aggregate: Option<&'static str>) -> Self {
        Self {
            bind_context,
            forbid_aggregate,
        }
    }
}

const GROUP_ITEM_NAME: &str = "group_item";
const GROUPING_FUNC_NAME: &str = "grouping";

impl VisitorMut<'_> for GroupingChecker<'_> {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        if let Some(column_binding) = self.bind_context.aggregate_info.lookup_group_item_column(
            expr,
            GROUP_ITEM_NAME,
            Visibility::Visible,
        )? {
            *expr = BoundColumnRef {
                span: expr.span(),
                column: column_binding,
            }
            .into();
            return Ok(());
        }

        match expr {
            ScalarExpr::WindowFunction(window) => {
                if let Some(column) = self
                    .bind_context
                    .windows
                    .window_functions_map
                    .get(&window.display_name)
                {
                    // The exprs in `win` has already been rewrittern to `BoundColumnRef` in `WindowRewriter`.
                    // So we need to check the exprs in `bind_context.windows`
                    let mut window_info =
                        self.bind_context.windows.window_functions[*column].clone();
                    // Just check if the exprs are in grouping items.
                    for part in window_info.partition_by_items.iter_mut() {
                        self.visit(&mut part.scalar)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for order in window_info.order_by_items.iter_mut() {
                        self.visit(&mut order.order_by_item.scalar)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for arg in window_info.arguments.iter_mut() {
                        self.visit(&mut arg.scalar)?;
                    }

                    let column_binding = ColumnBindingBuilder::new(
                        window.display_name.clone(),
                        window_info.index,
                        Box::new(window_info.func.return_type()),
                        Visibility::Visible,
                    )
                    .build();
                    *expr = BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into();
                    return Ok(());
                }

                return Err(ErrorCode::Internal("Group Check: Invalid window function"));
            }
            ScalarExpr::AggregateFunction(agg) => {
                if let Some(msg) = self.forbid_aggregate {
                    return Err(ErrorCode::SemanticError(msg.to_string()).set_span(agg.span));
                }

                let Some(agg_func) = self
                    .bind_context
                    .aggregate_info
                    .lookup_aggregate_function(agg)
                else {
                    return Err(ErrorCode::Internal("Invalid aggregate function"));
                };

                let column_binding = ColumnBindingBuilder::new(
                    agg.display_name.clone(),
                    agg_func.index,
                    Box::new(agg_func.scalar.data_type()?),
                    Visibility::Visible,
                )
                .build();
                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();
                return Ok(());
            }
            ScalarExpr::UDAFCall(udaf) => {
                if let Some(msg) = self.forbid_aggregate {
                    return Err(ErrorCode::SemanticError(msg.to_string()).set_span(udaf.span));
                }
                let Some(agg_func) = self.bind_context.aggregate_info.lookup_udaf_call(udaf) else {
                    return Err(ErrorCode::Internal("Invalid udaf function"));
                };

                let column_binding = ColumnBindingBuilder::new(
                    udaf.display_name.clone(),
                    agg_func.index,
                    Box::new(agg_func.scalar.data_type()?),
                    Visibility::Visible,
                )
                .build();
                *expr = BoundColumnRef {
                    span: None,
                    column: column_binding,
                }
                .into();
                return Ok(());
            }
            ScalarExpr::BoundColumnRef(column_ref) => {
                if let Some(index) = self
                    .bind_context
                    .srf_info
                    .srfs_map
                    .get(&column_ref.column.column_name)
                {
                    // If the srf has been rewrote as a column,
                    // check whether the srf arguments are group item.
                    let srf_item = &self.bind_context.srf_info.srfs[*index];
                    if let ScalarExpr::FunctionCall(func) = &srf_item.scalar {
                        for mut arg in func.arguments.clone() {
                            walk_expr_mut(self, &mut arg)?;
                        }
                    }
                    return Ok(());
                }
            }
            // don't check and rewrite grouping function
            ScalarExpr::FunctionCall(func)
                if func.func_name.eq_ignore_ascii_case(GROUPING_FUNC_NAME) =>
            {
                return Ok(());
            }
            _ => {}
        }
        walk_expr_mut(self, expr)
    }

    fn visit_cast_expr(&mut self, cast: &'_ mut crate::plans::CastExpr) -> Result<()> {
        let source_type = cast.argument.data_type()?;
        self.visit(&mut cast.argument)?;
        let after_type = cast.argument.data_type()?;

        if !source_type.is_nullable() && after_type.is_nullable() {
            cast.target_type = Box::new(cast.target_type.wrap_nullable());
        }
        Ok(())
    }

    fn visit_bound_column_ref(&mut self, column: &mut BoundColumnRef) -> Result<()> {
        if self
            .bind_context
            .aggregate_info
            .has_group_item_index(column.column.index)
        {
            return Ok(());
        }

        if self
            .bind_context
            .aggregate_info
            .has_aggregate_call_index(column.column.index)
        {
            if let Some(msg) = self.forbid_aggregate {
                return Err(ErrorCode::SemanticError(msg.to_string()).set_span(column.span));
            }

            // Be replaced by `AggregateRewriter`.
            return Ok(());
        }

        if self
            .bind_context
            .windows
            .get_window_info(&column.column.column_name)
            .is_some()
        {
            // Be replaced by `WindowRewriter`.
            return Ok(());
        }

        // If this is a group item, then it should have been replaced with `group_items_map`
        Err(ErrorCode::SemanticError(format!(
            "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
            &column.column.column_name
        ))
        .set_span(column.span))
    }
}
