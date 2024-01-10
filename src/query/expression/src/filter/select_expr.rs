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

use crate::filter::select_expr_permutation::FilterPermutation;
use crate::filter::SelectOp;
use crate::types::DataType;
use crate::Expr;
use crate::Scalar;

// The `SelectExpr` is used to represent the predicates expression.
#[derive(Clone, Debug)]
pub enum SelectExpr {
    // And SelectExprs.
    And((Vec<SelectExpr>, FilterPermutation)),
    // Or SelectExprs.
    Or((Vec<SelectExpr>, FilterPermutation)),
    // Compare operations: ((Equal | NotEqual | Gt | Lt | Gte | Lte), args, data type of args).
    Compare((SelectOp, Vec<Expr>, Vec<DataType>)),
    // Other operations: for example, like, is_null, is_not_null, etc.
    Others(Expr),
    // Boolean column: (column id, data type of column).
    BooleanColumn((usize, DataType)),
    // Boolean scalar: (scalar, data type of scalar).
    BooleanScalar((Scalar, DataType)),
}

pub struct SelectExprBuildResult {
    select_expr: SelectExpr,
    has_or: bool,
    can_reorder: bool,
}

impl SelectExprBuildResult {
    fn new(select_expr: SelectExpr, has_or: bool, can_reorder: bool) -> Self {
        Self {
            select_expr,
            has_or,
            can_reorder,
        }
    }

    pub fn into(self) -> (SelectExpr, bool) {
        (self.select_expr, self.has_or)
    }
}

// If a function may be use for filter short-circuiting, we can not perform filter reorder,
// for example, for predicates `a != 0 and 3 / a > 1`，if we swap `a != 0` and `3 / a > 1`,
// there will be a divide by zero error.
fn can_reorder(func_name: &str) -> bool {
    // There may be other functions that can be used for filter short-circuiting.
    if matches!(func_name, "cast" | "div" | "divide" | "modulo") || func_name.starts_with("to_") {
        return false;
    }
    true
}

// Build `SelectExpr` from `Expr`, return the `SelectExpr` and whether the `SelectExpr` contains `Or` operation.
pub fn build_select_expr(expr: &Expr) -> SelectExprBuildResult {
    match expr {
        Expr::FunctionCall {
            function,
            args,
            generics,
            ..
        } => {
            let func_name = function.signature.name.as_str();
            match func_name {
                "and" | "and_filters" => {
                    let mut and_args = vec![];
                    let mut has_or = false;
                    let mut can_reorder = true;
                    for arg in args {
                        // Recursively flatten the AND expressions.
                        let select_expr_build_result = build_select_expr(arg);
                        has_or |= select_expr_build_result.has_or;
                        can_reorder &= select_expr_build_result.can_reorder;
                        if let SelectExpr::And((select_expr, _)) =
                            select_expr_build_result.select_expr
                        {
                            and_args.extend(select_expr);
                        } else {
                            and_args.push(select_expr_build_result.select_expr);
                        }
                    }
                    let num_exprs = and_args.len();
                    let select_expr =
                        SelectExpr::And((and_args, FilterPermutation::new(num_exprs, can_reorder)));
                    SelectExprBuildResult::new(select_expr, has_or, true)
                }
                "or" => {
                    let mut or_args = vec![];
                    let mut can_reorder = true;
                    for arg in args {
                        // Recursively flatten the OR expressions.
                        let select_expr_build_result = build_select_expr(arg);
                        can_reorder &= select_expr_build_result.can_reorder;
                        if let SelectExpr::Or((select_expr, _)) =
                            select_expr_build_result.select_expr
                        {
                            or_args.extend(select_expr);
                        } else {
                            or_args.push(select_expr_build_result.select_expr);
                        }
                    }
                    let num_exprs = or_args.len();
                    let select_expr =
                        SelectExpr::Or((or_args, FilterPermutation::new(num_exprs, can_reorder)));
                    SelectExprBuildResult::new(select_expr, true, true)
                }
                "eq" | "noteq" | "gt" | "lt" | "gte" | "lte" => {
                    let select_op = SelectOp::try_from_func_name(&function.signature.name).unwrap();
                    SelectExprBuildResult::new(
                        SelectExpr::Compare((select_op, args.clone(), generics.clone())),
                        false,
                        true,
                    )
                }
                "is_true" => build_select_expr(&args[0]),
                _ => {
                    let can_reorder = can_reorder(func_name);
                    SelectExprBuildResult::new(SelectExpr::Others(expr.clone()), false, can_reorder)
                }
            }
        }
        Expr::ColumnRef { id, data_type, .. } if matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean)) => {
            SelectExprBuildResult::new(
                SelectExpr::BooleanColumn((*id, data_type.clone())),
                false,
                true,
            )
        }
        Expr::Constant {
            scalar, data_type, ..
        } if matches!(data_type, &DataType::Boolean | &DataType::Nullable(box DataType::Boolean)) => {
            SelectExprBuildResult::new(
                SelectExpr::BooleanScalar((scalar.clone(), data_type.clone())),
                false,
                true,
            )
        }
        _ => SelectExprBuildResult::new(SelectExpr::Others(expr.clone()), false, false),
    }
}
