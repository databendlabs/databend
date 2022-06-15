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

use crate::sql::binder::ColumnBinding;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::BindContext;

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

    pub fn resolve(&mut self, scalar: &Scalar) -> Result<Scalar> {
        if let Some(index) = self
            .bind_context
            .aggregate_info
            .group_items_map
            .get(&format!("{:?}", scalar))
        {
            let column = &self.bind_context.aggregate_info.group_items[*index];
            let column_binding = ColumnBinding {
                table_name: None,
                column_name: "group_item".to_string(),
                index: column.index,
                data_type: column.scalar.data_type(),
                visible_in_unqualified_wildcard: true,
            };
            return Ok(BoundColumnRef {
                column: column_binding,
            }
            .into());
        }

        match scalar {
            Scalar::BoundColumnRef(column) => {
                // If this is a group item, then it should have been replaced with `group_items_map`
                Err(ErrorCode::SemanticError(format!("column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function", &column.column.column_name)))
            }
            Scalar::ConstantExpr(_) => Ok(scalar.clone()),
            Scalar::AndExpr(scalar) => Ok(AndExpr {
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
            Scalar::OrExpr(scalar) => Ok(OrExpr {
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
            Scalar::ComparisonExpr(scalar) => Ok(ComparisonExpr {
                op: scalar.op.clone(),
                left: Box::new(self.resolve(&scalar.left)?),
                right: Box::new(self.resolve(&scalar.right)?),
            }
            .into()),
            Scalar::FunctionCall(func) => {
                let args = func
                    .arguments
                    .iter()
                    .map(|arg| self.resolve(arg))
                    .collect::<Result<Vec<Scalar>>>()?;
                Ok(FunctionCall {
                    arguments: args,
                    func_name: func.func_name.clone(),
                    arg_types: func.arg_types.clone(),
                    return_type: func.return_type.clone(),
                }
                .into())
            }
            Scalar::CastExpr(cast) => Ok(CastExpr {
                argument: Box::new(self.resolve(&cast.argument)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }
            .into()),
            Scalar::SubqueryExpr(_) => {
                // TODO(leiysky): check subquery in the future
                Ok(scalar.clone())
            }

            Scalar::AggregateFunction(agg) => {
                if let Some(column) = self
                    .bind_context
                    .aggregate_info
                    .aggregate_functions_map
                    .get(&agg.display_name)
                {
                    let agg_func = &self.bind_context.aggregate_info.aggregate_functions[*column];
                    let column_binding = ColumnBinding {
                        table_name: None,
                        column_name: agg.display_name.clone(),
                        index: agg_func.index,
                        data_type: agg_func.scalar.data_type(),
                        visible_in_unqualified_wildcard: true,
                    };
                    return Ok(BoundColumnRef {
                        column: column_binding,
                    }
                    .into());
                }
                Err(ErrorCode::LogicalError("Invalid aggregate function"))
            }
        }
    }
}
