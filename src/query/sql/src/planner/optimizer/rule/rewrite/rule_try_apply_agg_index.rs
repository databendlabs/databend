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

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::MetadataRef;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
    _metadata: MetadataRef,
}

impl RuleTryApplyAggIndex {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            _metadata: metadata,
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
        _s_expr: &SExpr,
        _state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        // let index_plans = self.get_index_plans();
        // if index_plans.is_empty() {
        //     // No enterprise liscense or no index.
        //     return Ok(());
        // }

        // let handler = get_agg_index_handler();

        // // if let Some(mut new_expr) =

        // Ok(())
        todo!("agg index")
    }
}

// impl RuleTryApplyAggIndex {
//     fn get_index_plans(&self) -> Vec<SExpr> {
//         todo!("agg index")
//     }
// }
