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

use crate::filter::like::gerenate_like_pattern;
use crate::filter::like::LikePattern;
use crate::filter::select_expr_permutation::FilterPermutation;
use crate::filter::SelectOp;
use crate::types::DataType;
use crate::Expr;
use crate::Function;
use crate::FunctionID;
use crate::Scalar;

/// The `SelectExpr` is used to represent the predicates expression.
#[derive(Clone, Debug)]
pub enum SelectExpr {
    // And SelectExprs.
    And((Vec<SelectExpr>, FilterPermutation)),
    // Or SelectExprs.
    Or((Vec<SelectExpr>, FilterPermutation)),
    // Compare operations: ((Equal | NotEqual | Gt | Lt | Gte | Lte), args, data type of args).
    Compare((SelectOp, Vec<Expr>, Vec<DataType>)),
    // Like operation: (column ref, like pattern, like str, is not like).
    Like((Expr, LikePattern, String, bool)),
    // Other operations: for example, like, is_null, is_not_null, etc.
    Others(Expr),
    // Boolean column: (column id, data type of column).
    BooleanColumn((usize, DataType)),
    // Boolean scalar: (scalar, data type of scalar).
    BooleanScalar((Scalar, DataType)),
}

#[derive(Default)]
pub struct SelectExprBuilder {
    not_function: Option<(FunctionID, Arc<Function>)>,
}

impl SelectExprBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(&mut self, expr: &Expr) -> SelectExprBuildResult {
        self.build_select_expr(expr, false)
    }

    // Build `SelectExpr` from `Expr`, return the `SelectExpr` and whether the `SelectExpr` contains `Or` operation.
    pub fn build_select_expr(&mut self, expr: &Expr, not: bool) -> SelectExprBuildResult {
        match expr {
            Expr::FunctionCall {
                id,
                function,
                args,
                generics,
                ..
            } => {
                let func_name = function.signature.name.as_str();
                if !not && matches!(func_name, "and" | "and_filters")
                    || not && matches!(func_name, "or")
                {
                    let mut and_args = vec![];
                    let mut has_or = false;
                    let mut can_reorder = true;
                    let mut can_push_down_not = true;
                    for arg in args {
                        // Recursively flatten the AND expressions.
                        let result = self.build_select_expr(arg, not);
                        has_or |= result.has_or;
                        can_reorder &= result.can_reorder;
                        can_push_down_not &= result.can_push_down_not;
                        if let SelectExpr::And((select_expr, _)) = result.select_expr {
                            and_args.extend(select_expr);
                        } else {
                            and_args.push(result.select_expr);
                        }
                    }
                    let num_exprs = and_args.len();
                    let select_expr =
                        SelectExpr::And((and_args, FilterPermutation::new(num_exprs, can_reorder)));
                    SelectExprBuildResult::new(select_expr)
                        .has_or(has_or)
                        .can_push_down_not(can_push_down_not)
                } else if !not && matches!(func_name, "or")
                    || not && matches!(func_name, "and" | "and_filters")
                {
                    let mut or_args = vec![];
                    let mut can_reorder = true;
                    let mut can_push_down_not = true;
                    for arg in args {
                        // Recursively flatten the OR expressions.
                        let result = self.build_select_expr(arg, not);
                        can_reorder &= result.can_reorder;
                        can_push_down_not &= result.can_push_down_not;
                        if let SelectExpr::Or((select_expr, _)) = result.select_expr {
                            or_args.extend(select_expr);
                        } else {
                            or_args.push(result.select_expr);
                        }
                    }
                    let num_exprs = or_args.len();
                    let select_expr =
                        SelectExpr::Or((or_args, FilterPermutation::new(num_exprs, can_reorder)));
                    SelectExprBuildResult::new(select_expr)
                        .has_or(true)
                        .can_push_down_not(can_push_down_not)
                } else {
                    match func_name {
                        "eq" | "noteq" | "gt" | "lt" | "gte" | "lte" => {
                            let select_op =
                                SelectOp::try_from_func_name(&function.signature.name).unwrap();
                            let select_op = if not { select_op.not() } else { select_op };
                            let can_reorder =
                                Self::can_reorder(&args[0]) && Self::can_reorder(&args[1]);
                            SelectExprBuildResult::new(SelectExpr::Compare((
                                select_op,
                                args.clone(),
                                generics.clone(),
                            )))
                            .can_reorder(can_reorder)
                        }
                        "not" => {
                            self.not_function = Some((id.clone(), function.clone()));
                            let result = self.build_select_expr(&args[0], not ^ true);
                            if result.can_push_down_not {
                                result
                            } else {
                                self.other_select_expr(expr, not)
                            }
                        }
                        "like" => {
                            let (column, column_data_type, scalar) = match (&args[0], &args[1]) {
                                (
                                    Expr::ColumnRef { data_type, .. },
                                    Expr::Constant { scalar, .. },
                                ) if matches!(data_type, DataType::String | DataType::Nullable(box DataType::String)) => {
                                    (&args[0], data_type, scalar)
                                }
                                (
                                    Expr::Constant { scalar, .. },
                                    Expr::ColumnRef { data_type, .. },
                                ) if matches!(data_type, DataType::String | DataType::Nullable(box DataType::String)) => {
                                    (&args[1], data_type, scalar)
                                }
                                _ => {
                                    return SelectExprBuildResult::new(SelectExpr::Others(
                                        expr.clone(),
                                    ))
                                    .can_push_down_not(false);
                                }
                            };
                            let can_reorder = Self::can_reorder(column);
                            if matches!(column_data_type, DataType::String | DataType::Nullable(box DataType::String))
                                && let Scalar::String(like_str) = scalar
                            {
                                let like_pattern = gerenate_like_pattern(like_str.as_bytes());
                                SelectExprBuildResult::new(SelectExpr::Like((
                                    column.clone(),
                                    like_pattern,
                                    like_str.clone(),
                                    not,
                                )))
                                .can_reorder(can_reorder)
                            } else {
                                SelectExprBuildResult::new(SelectExpr::Others(expr.clone()))
                                    .can_push_down_not(false)
                                    .can_reorder(can_reorder)
                            }
                        }
                        "is_true" => self.build_select_expr(&args[0], not),
                        _ => self
                            .other_select_expr(expr, not)
                            .can_reorder(Self::can_reorder(expr)),
                    }
                }
            }
            Expr::ColumnRef { id, data_type, .. } if matches!(data_type, DataType::Boolean | DataType::Nullable(box DataType::Boolean)) => {
                SelectExprBuildResult::new(SelectExpr::BooleanColumn((*id, data_type.clone())))
                    .can_push_down_not(false)
            }
            Expr::Constant {
                scalar, data_type, ..
            } if matches!(data_type, &DataType::Boolean | &DataType::Nullable(box DataType::Boolean)) =>
            {
                let scalar = if not {
                    match scalar {
                        Scalar::Null => Scalar::Null,
                        Scalar::Boolean(x) => Scalar::Boolean(!x),
                        _ => unreachable!("The constant must be a boolean scalar"),
                    }
                } else {
                    scalar.clone()
                };
                SelectExprBuildResult::new(SelectExpr::BooleanScalar((scalar, data_type.clone())))
            }
            Expr::Cast { is_try, .. } if *is_try => self.other_select_expr(expr, not),
            _ => self.other_select_expr(expr, not).can_reorder(false),
        }
    }

    // If a function may be use for filter short-circuiting, we can not perform filter reorder,
    // for example, for predicates `a != 0 and 3 / a > 1`，if we swap `a != 0` and `3 / a > 1`,
    // there will be a divide by zero error.
    pub fn can_reorder(expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { function, args, .. } => {
                let func_name = function.signature.name.as_str();
                // There may be other functions that can be used for filter short-circuiting.
                let mut can_reorder = !matches!(func_name, "cast" | "div" | "divide" | "modulo")
                    && !func_name.starts_with("to_");
                if can_reorder {
                    for arg in args {
                        can_reorder &= Self::can_reorder(arg);
                    }
                }
                can_reorder
            }
            Expr::ColumnRef { .. } | Expr::Constant { .. } => true,
            Expr::Cast { is_try, .. } if *is_try => true,
            _ => false,
        }
    }

    fn other_select_expr(&self, expr: &Expr, not: bool) -> SelectExprBuildResult {
        let can_push_down_not = !not
            || matches!(expr.data_type(), DataType::Boolean | DataType::Nullable(box DataType::Boolean));
        let expr = if not && can_push_down_not {
            self.wrap_not(expr)
        } else {
            expr.clone()
        };
        SelectExprBuildResult::new(SelectExpr::Others(expr)).can_push_down_not(can_push_down_not)
    }

    fn wrap_not(&self, expr: &Expr) -> Expr {
        let (id, function) = self.not_function.as_ref().unwrap();
        Expr::FunctionCall {
            span: None,
            id: id.clone(),
            function: function.clone(),
            generics: vec![],
            args: vec![expr.clone()],
            return_type: expr.data_type().clone(),
        }
    }
}

pub struct SelectExprBuildResult {
    select_expr: SelectExpr,
    has_or: bool,
    can_reorder: bool,
    can_push_down_not: bool,
}

impl SelectExprBuildResult {
    fn new(select_expr: SelectExpr) -> Self {
        Self {
            select_expr,
            has_or: false,
            can_reorder: true,
            can_push_down_not: true,
        }
    }

    pub fn into(self) -> (SelectExpr, bool) {
        (self.select_expr, self.has_or)
    }

    pub fn has_or(mut self, has_or: bool) -> Self {
        self.has_or = has_or;
        self
    }

    pub fn can_reorder(mut self, can_order: bool) -> Self {
        self.can_reorder = can_order;
        self
    }

    pub fn can_push_down_not(mut self, can_push_down_not: bool) -> Self {
        self.can_push_down_not = can_push_down_not;
        self
    }
}
