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
use common_expression::FunctionContext;

use super::agg_index;
use crate::optimizer::rule::Rule;
use crate::optimizer::HeuristicOptimizer;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::BindContext;
use crate::IndexType;
use crate::MetadataRef;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    metadata: MetadataRef,
    func_ctx: FunctionContext,

    patterns: Vec<SExpr>,
}

impl RuleTryApplyAggIndex {
    pub fn new(func_ctx: FunctionContext, metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            func_ctx,
            metadata,
            patterns: vec![
                // Expression
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Scan,
                        }
                        .into(),
                    ),
                ),
                // Expression
                //     |
                //   Filter
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Filter,
                        }
                        .into(),
                        SExpr::create_leaf(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ),
                    ),
                ),
                // Expression
                //     |
                // Aggregation
                //     |
                // Expression
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                        SExpr::create_unary(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                            SExpr::create_unary(
                                PatternPlan {
                                    plan_type: RelOp::EvalScalar,
                                }
                                .into(),
                                SExpr::create_leaf(
                                    PatternPlan {
                                        plan_type: RelOp::Scan,
                                    }
                                    .into(),
                                ),
                            ),
                        ),
                    ),
                ),
                // Expression
                //     |
                // Aggregation
                //     |
                // Expression
                //     |
                //   Filter
                //     |
                //    Scan
                SExpr::create_unary(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                    SExpr::create_unary(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                        SExpr::create_unary(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                            SExpr::create_unary(
                                PatternPlan {
                                    plan_type: RelOp::EvalScalar,
                                }
                                .into(),
                                SExpr::create_unary(
                                    PatternPlan {
                                        plan_type: RelOp::Filter,
                                    }
                                    .into(),
                                    SExpr::create_leaf(
                                        PatternPlan {
                                            plan_type: RelOp::Scan,
                                        }
                                        .into(),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ],
        }
    }
}

impl Rule for RuleTryApplyAggIndex {
    fn id(&self) -> RuleID {
        self.id
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let table_id = Self::get_table_id(s_expr);
        let metadata = self.metadata.read();
        let index_plans = metadata.get_agg_indexes(table_id);
        if index_plans.is_none() {
            // No enterprise license or no index.
            return Ok(());
        }
        let index_plans = index_plans.unwrap();
        if index_plans.is_empty() {
            // No enterprise license or no index.
            return Ok(());
        }

        // The bind context is useless here.
        let optimzier = HeuristicOptimizer::new(
            self.func_ctx.clone(),
            Box::new(BindContext::new()),
            self.metadata.clone(),
        );

        if let Some(mut result) = agg_index::try_rewrite(&optimzier, s_expr, index_plans)? {
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }
}

impl RuleTryApplyAggIndex {
    fn get_table_id(s_expr: &SExpr) -> IndexType {
        match s_expr.plan() {
            RelOperator::Scan(scan) => scan.table_index,
            _ => Self::get_table_id(s_expr.child(0).unwrap()),
        }
    }
}
