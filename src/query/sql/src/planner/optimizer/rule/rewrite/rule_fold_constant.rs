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
use std::sync::Arc;

use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Exchange;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::TypeCheck;

pub struct RuleFoldConstant {
    id: RuleID,
    patterns: Vec<SExpr>,
    func_ctx: FunctionContext,
}

impl RuleFoldConstant {
    pub fn new(func_ctx: FunctionContext) -> Self {
        Self {
            id: RuleID::FoldConstant,
            patterns: vec![
                // Filter
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Filter,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // EvalScalar
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::EvalScalar,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // ProjectSet
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::ProjectSet,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Exchange
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Exchange,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Join
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Join,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
                // Window
                //  \
                //   *
                SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Window,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ))),
                ),
            ],
            func_ctx,
        }
    }

    fn fold_constant(&self, scalar: &mut ScalarExpr) -> Result<()> {
        if scalar.used_columns().is_empty() {
            let expr = scalar.resolve_and_check(&HashMap::new())?;
            let (new_expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            if let Expr::Constant {
                span,
                scalar: value,
                ..
            } = new_expr
            {
                *scalar = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
                return Ok(());
            };
        }

        match scalar {
            ScalarExpr::FunctionCall(func) => {
                for arg in func.arguments.iter_mut() {
                    self.fold_constant(arg)?
                }
            }
            ScalarExpr::CastExpr(cast) => self.fold_constant(&mut cast.argument)?,
            _ => (),
        }

        Ok(())
    }
}

impl Rule for RuleFoldConstant {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let mut new_plan = s_expr.plan().clone();
        match &mut new_plan {
            RelOperator::EvalScalar(eval_scalar) => {
                for scalar in eval_scalar.items.iter_mut() {
                    self.fold_constant(&mut scalar.scalar)?;
                }
            }
            RelOperator::Filter(filter) => {
                for predicate in filter.predicates.iter_mut() {
                    self.fold_constant(predicate)?;
                }
            }
            RelOperator::ProjectSet(project_set) => {
                for srf in project_set.srfs.iter_mut() {
                    self.fold_constant(&mut srf.scalar)?;
                }
            }
            RelOperator::Exchange(Exchange::Hash(scalars)) => {
                for scalar in scalars.iter_mut() {
                    self.fold_constant(scalar)?;
                }
            }
            RelOperator::Join(join) => {
                for cond in join.left_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
                for cond in join.right_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
                for cond in join.non_equi_conditions.iter_mut() {
                    self.fold_constant(cond)?;
                }
            }
            RelOperator::Window(window) => {
                for arg in window.arguments.iter_mut() {
                    arg.scalar = self.fold_constant(&arg.scalar)?;
                }
                for part in window.partition_by.iter_mut() {
                    part.scalar = self.fold_constant(&part.scalar)?;
                }
                for o in window.order_by.iter_mut() {
                    o.order_by_item.scalar = self.fold_constant(&o.order_by_item.scalar)?;
                }
            }
            _ => unreachable!(),
        }
        if &new_plan != s_expr.plan() {
            state.add_result(s_expr.replace_plan(Arc::new(new_plan)));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
