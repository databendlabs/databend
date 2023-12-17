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

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::PatternPlan;
use crate::plans::RelOp;

// Split `Aggregate` into `FinalAggregate` and `PartialAggregate`
pub struct RuleSplitAggregate {
    id: RuleID,
    patterns: Vec<SExpr>,
}

impl RuleSplitAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::SplitAggregate,
            //  Aggregate
            //  \
            //   *
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Aggregate,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ))),
            )],
        }
    }
}

impl Rule for RuleSplitAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut agg: Aggregate = s_expr.plan().clone().try_into()?;
        if agg.mode != AggregateMode::Initial {
            return Ok(());
        }

        agg.mode = AggregateMode::Final;
        let mut partial = agg.clone();
        partial.mode = AggregateMode::Partial;
        let result = SExpr::create_unary(
            Arc::new(agg.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(partial.into()),
                Arc::new(s_expr.child(0)?.clone()),
            )),
        );
        state.add_result(result);
        Ok(())
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }
}
