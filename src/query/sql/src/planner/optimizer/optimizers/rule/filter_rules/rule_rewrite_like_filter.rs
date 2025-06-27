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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Scalar;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOp;
use crate::ScalarExpr;

pub struct RuleRewriteLike {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleRewriteLike {
    pub fn new() -> Self {
        Self {
            id: RuleID::RewriteLike,
            // Filter
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleRewriteLike {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // Eliminate empty EvalScalar
        let filters: Filter = s_expr.plan().clone().try_into()?;
        println!("Filters s_expr is {:?}", filters);

        let mut fold = false;

        fn check_like(scalar_expr: ScalarExpr) -> (bool, ScalarExpr) {
            match &scalar_expr {
                ScalarExpr::FunctionCall(func) if func.func_name == "like" => {
                    if let (ScalarExpr::BoundColumnRef(left_col), ScalarExpr::ConstantExpr(c)) =
                        (&func.arguments[0], &func.arguments[1])
                    {
                        if matches!(
                            *left_col.column.data_type,
                            DataType::String | DataType::Variant
                        ) {
                            if let Scalar::String(v) = &c.value {
                                if v == "%" {
                                    (
                                        true,
                                        ScalarExpr::ConstantExpr(ConstantExpr {
                                            span: None,
                                            value: Scalar::Boolean(true),
                                        }),
                                    )
                                } else {
                                    (false, scalar_expr)
                                }
                            } else {
                                (false, scalar_expr)
                            }
                        } else {
                            (false, scalar_expr)
                        }
                    } else {
                        (false, scalar_expr)
                    }
                }
                ScalarExpr::FunctionCall(func) => {
                    let mut arguments = vec![];
                    let mut fold = false;
                    for arg in func.arguments.clone() {
                        let (child_folded, new_arg) = check_like(arg);
                        if child_folded {
                            fold = child_folded;
                        }
                        arguments.push(new_arg.clone());
                    }

                    if fold {
                        (
                            true,
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: func.span,
                                func_name: func.func_name.clone(),
                                params: func.params.clone(),
                                arguments,
                            }),
                        )
                    } else {
                        (
                            false,
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: func.span,
                                func_name: func.func_name.clone(),
                                params: func.params.clone(),
                                arguments,
                            }),
                        )
                    }
                }
                _ => (false, scalar_expr),
            }
        }
        let predicates = filters
            .predicates
            .into_iter()
            .map(|scalar_expr| {
                let (may_fold, scalar) = check_like(scalar_expr);
                fold = may_fold;
                scalar
            })
            .collect::<Vec<ScalarExpr>>();

        if fold {
            let filter = Filter { predicates };
            state.add_result(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(s_expr.child(0)?.clone()),
            ));
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleRewriteLike {
    fn default() -> Self {
        Self::new()
    }
}
