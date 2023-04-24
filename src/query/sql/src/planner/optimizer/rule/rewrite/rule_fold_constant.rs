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

use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

pub struct RuleFoldConstant {
    id: RuleID,
    patterns: Vec<SExpr>,
    func_ctx: FunctionContext,
}

impl RuleFoldConstant {
    pub fn new(func_ctx: FunctionContext) -> Self {
        Self {
            id: RuleID::NormalizeScalarFilter,
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
            ],
            func_ctx,
        }
    }

    fn fold_constant(&self, scalar: &ScalarExpr) -> Result<ScalarExpr> {
        let expr = scalar.as_expr()?;
        let (new_expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let new_scalar = ScalarExpr::from_expr(&new_expr)?;
        Ok(new_scalar)
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
        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut eval_scalar) => {
                let new_items = eval_scalar
                    .items
                    .iter()
                    .map(|scalar| {
                        Ok(ScalarItem {
                            scalar: self.fold_constant(&scalar.scalar)?,
                            index: scalar.index,
                        })
                    })
                    .collect::<Result<_>>()?;
                if new_items != eval_scalar.items {
                    eval_scalar.items = new_items;
                    state.add_result(SExpr::create_unary(
                        eval_scalar.into(),
                        s_expr.child(0)?.clone(),
                    ));
                }
            }
            RelOperator::Filter(mut filter) => {
                let new_predicates = filter
                    .predicates
                    .iter()
                    .map(|scalar| self.fold_constant(scalar))
                    .collect::<Result<_>>()?;
                if new_predicates != filter.predicates {
                    filter.predicates = new_predicates;
                    state.add_result(SExpr::create_unary(filter.into(), s_expr.child(0)?.clone()));
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
