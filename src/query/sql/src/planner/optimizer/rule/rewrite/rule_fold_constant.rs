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

use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Exchange;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

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
                    PatternPlan {
                        plan_type: RelOp::Filter,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
                // EvalScalar
                //  \
                //   *
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
                // ProjectSet
                //  \
                //   *
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::ProjectSet,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
                // Exchange
                //  \
                //   *
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Exchange,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
                // Join
                //  \
                //   *
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::Join,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ],
            func_ctx,
        }
    }

    fn fold_constant(&self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        let expr = scalar.as_expr()?;
        let old_type = expr.data_type().clone();

        let (new_expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let new_scalar = ScalarExpr::from_expr(&new_expr)?;
        let new_type = new_scalar.data_type()?;

        // Ensure constant folding does not change the type of the expression.
        if new_type == old_type {
            Ok(new_scalar)
        } else {
            Ok(wrap_cast(&new_scalar, &old_type))
        }
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
                    scalar.scalar = self.fold_constant(&scalar.scalar)?;
                }
            }
            RelOperator::Filter(filter) => {
                for predicate in filter.predicates.iter_mut() {
                    *predicate = self.fold_constant(predicate)?;
                }
            }
            RelOperator::ProjectSet(project_set) => {
                for srf in project_set.srfs.iter_mut() {
                    srf.scalar = self.fold_constant(&srf.scalar)?;
                }
            }
            RelOperator::Exchange(Exchange::Hash(scalars)) => {
                for scalar in scalars.iter_mut() {
                    *scalar = self.fold_constant(scalar)?;
                }
            }
            RelOperator::Join(join) => {
                for cond in join.left_conditions.iter_mut() {
                    *cond = self.fold_constant(cond)?;
                }
                for cond in join.right_conditions.iter_mut() {
                    *cond = self.fold_constant(cond)?;
                }
                for cond in join.non_equi_conditions.iter_mut() {
                    *cond = self.fold_constant(cond)?;
                }
            }
            _ => unreachable!(),
        }
        if &new_plan != s_expr.plan() {
            state.add_result(s_expr.replace_plan(new_plan));
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
