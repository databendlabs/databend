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
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::CrossApply;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Scalar;
use crate::sql::plans::SubqueryExpr;
use crate::sql::plans::SubqueryType;

/// Rewrite subquery into `Apply` operator
pub struct SubqueryRewriter;

impl SubqueryRewriter {
    pub fn new() -> Self {
        Self
    }

    pub fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                let mut expr = input;
                for item in plan.items.iter_mut() {
                    let (scalar, subqueries) = self.try_extract_subquery(&item.scalar)?;
                    item.scalar = scalar;
                    for subquery in subqueries {
                        expr = self.build_apply(&expr, &subquery)?;
                    }
                }

                Ok(SExpr::create_unary(s_expr.plan().clone(), expr))
            }
            RelOperator::Filter(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                let mut expr = input;
                for pred in plan.predicates.iter_mut() {
                    let (scalar, subqueries) = self.try_extract_subquery(pred)?;
                    *pred = scalar;
                    for subquery in subqueries {
                        expr = self.build_apply(&expr, &subquery)?;
                    }
                }

                Ok(SExpr::create_unary(s_expr.plan().clone(), expr))
            }
            RelOperator::Aggregate(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                let mut expr = input;
                for item in plan.group_items.iter_mut() {
                    let (scalar, subqueries) = self.try_extract_subquery(&item.scalar)?;
                    item.scalar = scalar;
                    for subquery in subqueries {
                        expr = self.build_apply(&expr, &subquery)?;
                    }
                }

                for item in plan.aggregate_functions.iter_mut() {
                    let (scalar, subqueries) = self.try_extract_subquery(&item.scalar)?;
                    item.scalar = scalar;
                    for subquery in subqueries {
                        expr = self.build_apply(&expr, &subquery)?;
                    }
                }

                Ok(SExpr::create_unary(s_expr.plan().clone(), expr))
            }

            RelOperator::LogicalInnerJoin(_) => Ok(SExpr::create_binary(
                s_expr.plan().clone(),
                self.rewrite(s_expr.child(0)?)?,
                self.rewrite(s_expr.child(1)?)?,
            )),

            RelOperator::Project(_) | RelOperator::Limit(_) | RelOperator::Sort(_) => Ok(
                SExpr::create_unary(s_expr.plan().clone(), self.rewrite(s_expr.child(0)?)?),
            ),

            RelOperator::LogicalGet(_) => Ok(s_expr.clone()),

            RelOperator::CrossApply(_)
            | RelOperator::PhysicalHashJoin(_)
            | RelOperator::Pattern(_)
            | RelOperator::PhysicalScan(_) => Err(ErrorCode::LogicalError("Invalid plan type")),
        }
    }

    pub fn build_apply(&mut self, left: &SExpr, subquery: &SubqueryExpr) -> Result<SExpr> {
        match subquery.typ {
            SubqueryType::Scalar => {
                let apply = CrossApply {
                    correlated_columns: subquery.outer_columns.clone(),
                };
                Ok(SExpr::create_binary(
                    apply.into(),
                    left.clone(),
                    subquery.subquery.clone(),
                ))
            }
            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported subquery type: {:?}",
                &subquery.typ
            ))),
        }
    }

    /// Try to extract subquery from a scalar expression. Returns replaced scalar expression
    /// and the subqueries.
    fn try_extract_subquery(&mut self, scalar: &Scalar) -> Result<(Scalar, Vec<SubqueryExpr>)> {
        match scalar {
            Scalar::BoundColumnRef(_) => Ok((scalar.clone(), vec![])),

            Scalar::ConstantExpr(_) => Ok((scalar.clone(), vec![])),

            Scalar::AndExpr(expr) => {
                let (left, result_left) = self.try_extract_subquery(&expr.left)?;
                let (right, result_right) = self.try_extract_subquery(&expr.right)?;
                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    vec![result_left, result_right].concat(),
                ))
            }

            Scalar::OrExpr(expr) => {
                let (left, result_left) = self.try_extract_subquery(&expr.left)?;
                let (right, result_right) = self.try_extract_subquery(&expr.right)?;
                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    vec![result_left, result_right].concat(),
                ))
            }

            Scalar::ComparisonExpr(expr) => {
                let (left, result_left) = self.try_extract_subquery(&expr.left)?;
                let (right, result_right) = self.try_extract_subquery(&expr.right)?;
                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    vec![result_left, result_right].concat(),
                ))
            }

            Scalar::AggregateFunction(_) => Ok((scalar.clone(), vec![])),

            Scalar::FunctionCall(func) => {
                let mut args = vec![];
                let mut s_exprs = vec![];
                for arg in func.arguments.iter() {
                    let (scalar, mut subquery) = self.try_extract_subquery(arg)?;
                    args.push(scalar);
                    s_exprs.append(&mut subquery);
                }

                let expr: Scalar = FunctionCall {
                    arguments: args,
                    func_name: func.func_name.clone(),
                    arg_types: func.arg_types.clone(),
                    return_type: func.return_type.clone(),
                }
                .into();

                Ok((expr, s_exprs))
            }

            Scalar::Cast(cast) => {
                let (scalar, s_exprs) = self.try_extract_subquery(&cast.argument)?;
                Ok((
                    CastExpr {
                        argument: Box::new(scalar),
                        from_type: cast.from_type.clone(),
                        target_type: cast.target_type.clone(),
                    }
                    .into(),
                    s_exprs,
                ))
            }

            Scalar::SubqueryExpr(subquery) => {
                // Extract the subquery and replace it with the ColumnBinding from it.
                let column = subquery
                    .output_context
                    .columns
                    .get(0)
                    .ok_or_else(|| ErrorCode::LogicalError("Invalid subquery"))?;
                let name = format!("subquery_{}", column.index);
                let column_ref = ColumnBinding {
                    table_name: None,
                    column_name: name,
                    index: column.index,
                    data_type: subquery.data_type.clone(),
                    visible_in_unqualified_wildcard: false,
                };

                Ok((BoundColumnRef { column: column_ref }.into(), vec![
                    subquery.clone()
                ]))
            }
        }
    }
}
