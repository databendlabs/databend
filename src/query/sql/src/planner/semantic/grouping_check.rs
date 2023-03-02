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
use crate::plans::Unnest;
use crate::BindContext;

/// Check validity of scalar expression in a grouping context.
/// The matched grouping item will be replaced with a BoundColumnRef
/// to corresponding grouping item column.
pub struct GroupingChecker<'a> {
    bind_context: &'a BindContext,
}

impl<'a> GroupingChecker<'a> {
    pub fn new(bind_context: &'a BindContext) -> Self {
        Self { bind_context }
    }

    pub fn resolve(&mut self, scalar: &ScalarExpr, span: Span) -> Result<ScalarExpr> {
        if let Some(index) = self
            .bind_context
            .aggregate_info
            .group_items_map
            .get(&format!("{:?}", scalar))
        {
            let column = &self.bind_context.aggregate_info.group_items[*index];
            let column_binding = ColumnBinding {
                database_name: None,
                table_name: None,
                column_name: "group_item".to_string(),
                index: column.index,
                data_type: Box::new(column.scalar.data_type()),
                visibility: Visibility::Visible,
            };
            return Ok(BoundColumnRef {
                column: column_binding,
            }
            .into());
        }

        match scalar {
            ScalarExpr::BoundColumnRef(column) => {
                // If this is a group item, then it should have been replaced with `group_items_map`
                Err(ErrorCode::SemanticError(format!(
                    "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                    &column.column.column_name
                )).set_span(span))
            }
            ScalarExpr::ConstantExpr(_) => Ok(scalar.clone()),
            ScalarExpr::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::NotExpr(scalar) => Ok(NotExpr {
                argument: Box::new(self.resolve(&scalar.argument, span)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.resolve(&scalar.left, span)?),
                right: Box::new(self.resolve(&scalar.right, span)?),
                return_type: scalar.return_type.clone(),
            }
            .into()),
            ScalarExpr::FunctionCall(func) => {
                let args = func
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg, span))
                    .collect::<Result<Vec<ScalarExpr>>>()?;
                Ok(FunctionCall {
                    params: func.params.clone(),
                    arguments: args,
                    func_name: func.func_name.clone(),
                    return_type: func.return_type.clone(),
                }
                .into())
            }
            ScalarExpr::CastExpr(cast) => Ok(CastExpr {
                is_try: cast.is_try,
                argument: Box::new(self.resolve(&cast.argument, span)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }
            .into()),
            ScalarExpr::Unnest(unnest) => Ok(Unnest {
                argument: Box::new(self.resolve(&unnest.argument, span)?),
                return_type: unnest.return_type.clone(),
            }
            .into()),
            ScalarExpr::SubqueryExpr(_) => {
                // TODO(leiysky): check subquery in the future
                Ok(scalar.clone())
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
                        column_name: agg.display_name.clone(),
                        index: agg_func.index,
                        data_type: Box::new(agg_func.scalar.data_type()),
                        visibility: Visibility::Visible,
                    };
                    return Ok(BoundColumnRef {
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::Internal("Invalid aggregate function"))
            }
        }
    }
}
