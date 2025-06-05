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
            "or" => {
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

#[cfg(test)]
mod test {
    use databend_common_exception::Result;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::Scalar;

    use crate::optimizer::optimizers::operator::EquivalentConstantsVisitor;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;
    use crate::plans::FunctionCall;
    use crate::plans::VisitorMut;
    use crate::ColumnBinding;
    use crate::ScalarExpr;
    use crate::Visibility;

    #[test]
    fn test_equivalent_constants() -> Result<()> {
        // [a = 1 and a = b] => [a = 1 and 1 = b]
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), column_b()],
                    }),
                ],
            });
            let expect = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![number(1), column_b()],
                    }),
                ],
            });
            check(expr, expect)?;
        }
        // [a = 1 and (b > a and b < (a + 2))] => [a = 1 and (b > 1 and b < (1 + 2))]
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), number(1)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![number(1), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            let expect = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), number(1)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![number(1), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            check(expr, expect)?;
        }
        // [(b > a and b < (a + 2)) and a = 1] => [(b > 1 and b < (1 + 2)) and a = 1]
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), number(1)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![number(1), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                ],
            });
            let expect = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), number(1)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![number(1), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                ],
            });
            check(expr, expect)?;
        }
        // [a = 1 or (b > a and b < (a + 2))] => [a = 1 or (b > a and b < (a + 2))]
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), column_a()],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![column_a(), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            check(expr.clone(), expr)?;
        }
        // [a = 1 or (a = 2 and b > a and b < (a + 2))] => [a = 1 or (a = 2 and b > 2 and b < (2 + 2))]
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "eq".to_string(),
                                params: vec![],
                                arguments: vec![column_a(), number(2)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "and".to_string(),
                                params: vec![],
                                arguments: vec![
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "gt".to_string(),
                                        params: vec![],
                                        arguments: vec![column_b(), column_a()],
                                    }),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "lt".to_string(),
                                        params: vec![],
                                        arguments: vec![
                                            column_b(),
                                            ScalarExpr::FunctionCall(FunctionCall {
                                                span: None,
                                                func_name: "add".to_string(),
                                                params: vec![],
                                                arguments: vec![column_a(), number(2)],
                                            }),
                                        ],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            let expect = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "eq".to_string(),
                                params: vec![],
                                arguments: vec![column_a(), number(2)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "and".to_string(),
                                params: vec![],
                                arguments: vec![
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "gt".to_string(),
                                        params: vec![],
                                        arguments: vec![column_b(), number(2)],
                                    }),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "lt".to_string(),
                                        params: vec![],
                                        arguments: vec![
                                            column_b(),
                                            ScalarExpr::FunctionCall(FunctionCall {
                                                span: None,
                                                func_name: "add".to_string(),
                                                params: vec![],
                                                arguments: vec![number(2), number(2)],
                                            }),
                                        ],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            check(expr, expect)?;
        }
        // [a = 1 or (a = 2 and b > a and b < (a + 2))] => [a = 1 or (a = 2 and b > 2 and b < (2 + 2))] for `and_filters`
        {
            let expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and_filters".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "eq".to_string(),
                                params: vec![],
                                arguments: vec![column_a(), number(2)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), column_a()],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![column_a(), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            let expect = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![column_a(), number(1)],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "and_filters".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "eq".to_string(),
                                params: vec![],
                                arguments: vec![column_a(), number(2)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "gt".to_string(),
                                params: vec![],
                                arguments: vec![column_b(), number(2)],
                            }),
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "lt".to_string(),
                                params: vec![],
                                arguments: vec![
                                    column_b(),
                                    ScalarExpr::FunctionCall(FunctionCall {
                                        span: None,
                                        func_name: "add".to_string(),
                                        params: vec![],
                                        arguments: vec![number(2), number(2)],
                                    }),
                                ],
                            }),
                        ],
                    }),
                ],
            });
            check(expr, expect)?;
        }

        Ok(())
    }

    fn check(mut expr: ScalarExpr, expect: ScalarExpr) -> Result<()> {
        let mut visitor = EquivalentConstantsVisitor::default();
        visitor.visit(&mut expr)?;

        assert_eq!(expr, expect);
        Ok(())
    }

    fn number(value: i32) -> ScalarExpr {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Number(NumberScalar::Int32(value)),
        })
    }

    fn column_a() -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBinding {
                database_name: None,
                table_name: None,
                column_position: None,
                table_index: None,
                column_name: "a".to_string(),
                index: 0,
                data_type: Box::new(DataType::Number(NumberDataType::Int32)),
                visibility: Visibility::Visible,
                virtual_expr: None,
                is_srf: false,
            },
        })
    }

    fn column_b() -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBinding {
                database_name: None,
                table_name: None,
                column_position: None,
                table_index: None,
                column_name: "b".to_string(),
                index: 1,
                data_type: Box::new(DataType::Number(NumberDataType::Int32)),
                visibility: Visibility::Visible,
                virtual_expr: None,
                is_srf: false,
            },
        })
    }
}
