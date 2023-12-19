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

use crate::filter::SelectOp;
use crate::types::DataType;
use crate::Expr;
use crate::Scalar;

// The `SelectExpr` is used to represent the predicates expression.
#[derive(Clone, Debug)]
pub enum SelectExpr {
    // And SelectExprs.
    And(Vec<SelectExpr>),
    // Or SelectExprs.
    Or(Vec<SelectExpr>),
    // Compare operations: ((Equal | NotEqual | Gt | Lt | Gte | Lte), args, data type of args).
    Compare((SelectOp, Vec<Expr>, Vec<DataType>)),
    // Other operations: for example, like, is_null, is_not_null, etc.
    Others(Expr),
    // Boolean column: (column id, data type of column).
    BooleanColumn((usize, DataType)),
    // Boolean scalar: (scalar, data type of scalar).
    BooleanScalar((Scalar, DataType)),
}

// Build `SelectExpr` from `Expr`, return the `SelectExpr` and whether the `SelectExpr` contains `Or` operation.
pub fn build_select_expr(expr: &Expr) -> (SelectExpr, bool) {
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
                    for arg in args {
                        // Recursively flatten the AND expressions.
                        let (select_expr, exists_or) = build_select_expr(arg);
                        has_or |= exists_or;
                        if let SelectExpr::And(select_expr) = select_expr {
                            and_args.extend(select_expr);
                        } else {
                            and_args.push(select_expr);
                        }
                    }
                    (SelectExpr::And(and_args), has_or)
                }
                "or" => {
                    let mut or_args = vec![];
                    for arg in args {
                        // Recursively flatten the OR expressions.
                        let (select_expr, _) = build_select_expr(arg);
                        if let SelectExpr::Or(select_expr) = select_expr {
                            or_args.extend(select_expr);
                        } else {
                            or_args.push(select_expr);
                        }
                    }
                    (SelectExpr::Or(or_args), true)
                }
                "eq" | "noteq" | "gt" | "lt" | "gte" | "lte" => {
                    let select_op = SelectOp::try_from_func_name(&function.signature.name).unwrap();
                    (
                        SelectExpr::Compare((select_op, args.clone(), generics.clone())),
                        false,
                    )
                }
                "is_true" => build_select_expr(&args[0]),
                _ => (SelectExpr::Others(expr.clone()), false),
            }
        }
        Expr::ColumnRef { id, data_type, .. } if matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean)) => {
            (SelectExpr::BooleanColumn((*id, data_type.clone())), false)
        }
        Expr::Constant {
            scalar, data_type, ..
        } if matches!(data_type, &DataType::Boolean | &DataType::Nullable(box DataType::Boolean)) => {
            (
                SelectExpr::BooleanScalar((scalar.clone(), data_type.clone())),
                false,
            )
        }
        _ => (SelectExpr::Others(expr.clone()), false),
    }
}
