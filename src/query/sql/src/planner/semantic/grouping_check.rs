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

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use crate::binder::ColumnBinding;
use crate::binder::Visibility;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::FunctionCall;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::ScalarExpr;
use crate::BindContext;

/// Check validity of scalar expression in a grouping context.
/// The matched grouping item will be replaced with a BoundColumnRef
/// to corresponding grouping item column.
///
/// Also replaced the matched window function with a BoundColumnRef.
pub struct GroupingChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> GroupingChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn resolve(&self, scalar: &ScalarExpr, span: Span) -> Result<ScalarExpr> {
        if let Some(index) = self.bind_context.aggregate_info.group_items_map.get(scalar) {
            let column = &self.bind_context.aggregate_info.group_items[*index];
            let mut column_binding = if let ScalarExpr::BoundColumnRef(column_ref) = &column.scalar
            {
                column_ref.column.clone()
            } else {
                ColumnBinding {
                    database_name: None,
                    table_name: None,
                    table_index: None,
                    column_name: "group_item".to_string(),
                    index: column.index,
                    data_type: Box::new(column.scalar.data_type()?),
                    visibility: Visibility::Visible,
                }
            };

            if let Some(grouping_id) = &self.bind_context.aggregate_info.grouping_id_column {
                if grouping_id.index != column_binding.index {
                    column_binding.data_type = Box::new(column_binding.data_type.wrap_nullable());
                }
            }

            return Ok(BoundColumnRef {
                span: scalar.span(),
                column: column_binding,
            }
            .into());
        }

        match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                if self
                    .bind_context
                    .aggregate_info
                    .aggregate_functions_map
                    .contains_key(&column.column.column_name)
                {
                    // Be replaced by `WindowRewriter`.
                    return Ok(scalar.clone());
                }
                // If this is a group item, then it should have been replaced with `group_items_map`
                Err(ErrorCode::SemanticError(format!(
                    "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    &column.column.column_name
                )).set_span(span))
            }
            ScalarExpr::BoundInternalColumnRef(column) => {
                // If this is a group item, then it should have been replaced with `group_items_map`
                Err(ErrorCode::SemanticError(format!(
                    "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    &column.column.internal_column.column_name()
                )).set_span(span))
            }
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
            }
            .into()),
            ScalarExpr::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
            }
            .into()),
            ScalarExpr::NotExpr(scalar) => Ok(NotExpr {
                argument: Box::new(self.resolve(&scalar.argument, span)?),
            }
            .into()),
            ScalarExpr::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
            }
            .into()),
            ScalarExpr::FunctionCall(func) => {
                let args = func
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg, span))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments: args,
                    func_name: func.func_name.clone(),
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                span: cast.span,
                is_try: cast.is_try,
                argument: Box::new(self.resolve(&cast.argument, span)?),
                target_type: cast.target_type.clone(),
            }
            .into()),
            ScalarExpr::SubqueryExpr(_) => {
                // TODO(leiysky): check subquery in the future
                Ok(scalar.clone())
            }

            ScalarExpr::WindowFunction(win) => {
                if let Some(column) = self
                    .bind_context
                    .windows
                    .window_functions_map
                    .get(&win.display_name)
                {
                    // The exprs in `win` has already been rewrittern to `BoundColumnRef` in `WindowRewriter`.
                    // So we need to check the exprs in `bind_context.windows`
                    let window_info = &self.bind_context.windows.window_functions[*column];
                    // Just check if the exprs are in grouping items.
                    for part in window_info.partition_by_items.iter() {
                        self.resolve(&part.scalar, span)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for order in window_info.order_by_items.iter() {
                        self.resolve(&order.order_by_item.scalar, span)?;
                    }
                    // Just check if the exprs are in grouping items.
                    for arg in window_info.arguments.iter() {
                        self.resolve(&arg.scalar, span)?;
                    }

                    let column_binding = ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: win.display_name.clone(),
                        index: window_info.index,
                        data_type: Box::new(window_info.func.return_type()),
                        visibility: Visibility::Visible,
                    };
                    Ok(BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into())
                } else {
                    Err(ErrorCode::Internal("Invalid window function"))
                }
            }

            ScalarExpr::AggregateFunction(agg) => {
                if let Some(column) = self
                    .bind_context
                    .aggregate_info
                    .aggregate_functions_map
                    .get(&agg.display_name)
                {
                    let agg_func = &self.bind_context.aggregate_info.aggregate_functions[*column];
                    let column_binding = ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: agg.display_name.clone(),
                        index: agg_func.index,
                        data_type: Box::new(agg_func.scalar.data_type()?),
                        visibility: Visibility::Visible,
                    };
                    return Ok(BoundColumnRef {
                        span: None,
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::Internal("Invalid aggregate function"))
            }
        }
    }
}
