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

use std::collections::HashMap;

use databend_common_exception::Result;

use crate::optimizer::optimizers::operator::filter::remove_trivial_type_cast;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::FunctionCall;
use crate::plans::VisitorMut;
use crate::ScalarExpr;

// EquivalentConstantsVisitor is used to collect the equivalent relationship between Column and Scalar from the bottom to the bottom,
// replacing the Column in the upper expression with Scalar to make the Predicate easier to push down
// e.g. [b = 3 and a between b and b + 2] => [b = 3 and a between 3 and 5]
pub struct EquivalentConstantsVisitor {
    left_visitor: EquivalentConstantsVisitorInner,
    right_visitor: EquivalentConstantsVisitorInner,
}

impl Default for EquivalentConstantsVisitor {
    fn default() -> Self {
        Self {
            left_visitor: EquivalentConstantsVisitorInner::default().left_visit_order(true),
            right_visitor: EquivalentConstantsVisitorInner::default().left_visit_order(false),
        }
    }
}

impl VisitorMut<'_> for EquivalentConstantsVisitor {
    fn visit(&mut self, expr: &'_ mut ScalarExpr) -> Result<()> {
        self.left_visitor.visit(expr)?;
        self.right_visitor.visit(expr)
    }
}

#[derive(Default)]
pub struct EquivalentConstantsVisitorInner {
    eq_constants: HashMap<BoundColumnRef, ScalarExpr>,
    left_visit_order: bool,
}

impl EquivalentConstantsVisitorInner {
    fn eq_constants(mut self, eq_constants: HashMap<BoundColumnRef, ScalarExpr>) -> Self {
        self.eq_constants = eq_constants;
        self
    }

    fn left_visit_order(mut self, left_visit_order: bool) -> Self {
        self.left_visit_order = left_visit_order;
        self
    }
}

impl VisitorMut<'_> for EquivalentConstantsVisitorInner {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)?;

        if let ScalarExpr::BoundColumnRef(column) = expr {
            if let Some(eq_expr) = self.eq_constants.get(column) {
                *expr = eq_expr.clone();
            }
        }
        Ok(())
    }

    fn visit_function_call(&mut self, func: &'_ mut FunctionCall) -> Result<()> {
        match func.func_name.as_str() {
            "or" | "or_filters" => {
                for expr in &mut func.arguments {
                    let mut visitor = EquivalentConstantsVisitorInner::default()
                        .left_visit_order(self.left_visit_order);
                    visitor.visit(expr)?;
                }
            }
            "and" | "and_filters" => {
                if self.left_visit_order {
                    for expr in func.arguments.iter_mut() {
                        self.visit(expr)?;
                    }
                } else {
                    for expr in func.arguments.iter_mut().rev() {
                        self.visit(expr)?;
                    }
                }
            }
            _ => {
                for expr in &mut func.arguments {
                    let mut visitor = EquivalentConstantsVisitorInner::default()
                        .eq_constants(self.eq_constants.clone())
                        .left_visit_order(self.left_visit_order);
                    visitor.visit(expr)?;
                }
                let Some(op) = ComparisonOp::try_from_func_name(&func.func_name) else {
                    return Ok(());
                };

                let (left, right) =
                    remove_trivial_type_cast(func.arguments[0].clone(), func.arguments[1].clone());
                if left != func.arguments[0] {
                    func.arguments[0] = left;
                }
                if right != func.arguments[1] {
                    func.arguments[1] = right;
                }
                if !matches!(op, ComparisonOp::Equal) {
                    return Ok(());
                }

                match (func.arguments[0].clone(), func.arguments[1].clone()) {
                    (
                        ScalarExpr::BoundColumnRef(left_column),
                        ScalarExpr::BoundColumnRef(right_column),
                    ) => {
                        match (
                            self.eq_constants.get(&left_column).cloned(),
                            self.eq_constants.get(&right_column).cloned(),
                        ) {
                            (Some(left_eq_expr), Some(right_eq_expr)) => {
                                if left_eq_expr != right_eq_expr {
                                    self.eq_constants.remove(&left_column);
                                    self.eq_constants.remove(&right_column);
                                }
                            }
                            (Some(left_eq_expr), None) => {
                                self.eq_constants.insert(right_column, left_eq_expr);
                            }
                            (None, Some(right_eq_expr)) => {
                                self.eq_constants.insert(left_column, right_eq_expr);
                            }
                            (None, None) => (),
                        }
                    }
                    (ScalarExpr::BoundColumnRef(column), expr)
                    | (expr, ScalarExpr::BoundColumnRef(column)) => {
                        if expr.used_columns().is_empty() {
                            self.eq_constants.insert(column, expr);
                        }
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }
}
