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

use super::agg_index;
use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;

pub struct RuleTryApplyAggIndex {
    id: RuleID,
    metadata: MetadataRef,

    patterns: Vec<SExpr>,
}

impl RuleTryApplyAggIndex {
    fn sorted_patterns() -> Vec<SExpr> {
        vec![
            // Expression
            //     |
            //    Sort
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Scan,
                        }
                        .into(),
                    ))),
                )),
            ),
            // Expression
            //     |
            //    Sort
            //     |
            //   Filter
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Filter,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_leaf(Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ))),
                    )),
                )),
            ),
            // Expression
            //     |
            //    Sort
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_unary(
                            Arc::new(
                                PatternPlan {
                                    plan_type: RelOp::Aggregate,
                                }
                                .into(),
                            ),
                            Arc::new(SExpr::create_unary(
                                Arc::new(
                                    PatternPlan {
                                        plan_type: RelOp::EvalScalar,
                                    }
                                    .into(),
                                ),
                                Arc::new(SExpr::create_leaf(Arc::new(
                                    PatternPlan {
                                        plan_type: RelOp::Scan,
                                    }
                                    .into(),
                                ))),
                            )),
                        )),
                    )),
                )),
            ),
            // Expression
            //     |
            //    Sort
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_unary(
                            Arc::new(
                                PatternPlan {
                                    plan_type: RelOp::Aggregate,
                                }
                                .into(),
                            ),
                            Arc::new(SExpr::create_unary(
                                Arc::new(
                                    PatternPlan {
                                        plan_type: RelOp::EvalScalar,
                                    }
                                    .into(),
                                ),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(
                                        PatternPlan {
                                            plan_type: RelOp::Filter,
                                        }
                                        .into(),
                                    ),
                                    Arc::new(SExpr::create_leaf(Arc::new(
                                        PatternPlan {
                                            plan_type: RelOp::Scan,
                                        }
                                        .into(),
                                    ))),
                                )),
                            )),
                        )),
                    )),
                )),
            ),
        ]
    }

    fn normal_patterns() -> Vec<SExpr> {
        vec![
            // Expression
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Scan,
                    }
                    .into(),
                ))),
            ),
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Filter,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_leaf(Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Scan,
                        }
                        .into(),
                    ))),
                )),
            ),
            // Expression
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //    Scan
            SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_unary(
                            Arc::new(
                                PatternPlan {
                                    plan_type: RelOp::EvalScalar,
                                }
                                .into(),
                            ),
                            Arc::new(SExpr::create_leaf(Arc::new(
                                PatternPlan {
                                    plan_type: RelOp::Scan,
                                }
                                .into(),
                            ))),
                        )),
                    )),
                )),
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
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::EvalScalar,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Aggregate,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Aggregate,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_unary(
                            Arc::new(
                                PatternPlan {
                                    plan_type: RelOp::EvalScalar,
                                }
                                .into(),
                            ),
                            Arc::new(SExpr::create_unary(
                                Arc::new(
                                    PatternPlan {
                                        plan_type: RelOp::Filter,
                                    }
                                    .into(),
                                ),
                                Arc::new(SExpr::create_leaf(Arc::new(
                                    PatternPlan {
                                        plan_type: RelOp::Scan,
                                    }
                                    .into(),
                                ))),
                            )),
                        )),
                    )),
                )),
            ),
        ]
    }

    fn patterns() -> Vec<SExpr> {
        let mut patterns = Self::normal_patterns();
        patterns.extend(Self::sorted_patterns());
        patterns
    }
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::TryApplyAggIndex,
            metadata,
            patterns: Self::patterns(),
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
        let (table_index, table_name) = self.get_table(s_expr);
        let metadata = self.metadata.read();
        let index_plans = metadata.get_agg_indexes(&table_name);
        if index_plans.is_none() {
            // No enterprise license or no index.
            return Ok(());
        }
        let index_plans = index_plans.unwrap();
        if index_plans.is_empty() {
            // No enterprise license or no index.
            return Ok(());
        }

        let base_columns = metadata.columns_by_table_index(table_index);

        if let Some(mut result) =
            agg_index::try_rewrite(table_index, &base_columns, s_expr, index_plans)?
        {
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }
}

impl RuleTryApplyAggIndex {
    fn get_table(&self, s_expr: &SExpr) -> (IndexType, String) {
        match s_expr.plan() {
            RelOperator::Scan(scan) => {
                let metadata = self.metadata.read();
                let table = metadata.table(scan.table_index);
                (
                    scan.table_index,
                    format!("{}.{}.{}", table.catalog(), table.database(), table.name()),
                )
            }
            _ => self.get_table(s_expr.child(0).unwrap()),
        }
    }
}
