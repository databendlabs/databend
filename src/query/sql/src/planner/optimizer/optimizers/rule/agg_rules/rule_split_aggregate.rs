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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::RelOp;

// Split `Aggregate` into `FinalAggregate` and `PartialAggregate`
pub struct RuleSplitAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleSplitAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::SplitAggregate,
            //  Aggregate
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Aggregate,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleSplitAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut agg = s_expr.plan().as_any().downcast_ref::<Aggregate>().unwrap();
        if agg.mode != AggregateMode::Initial {
            return Ok(());
        }

        agg.mode = AggregateMode::Final;
        let mut partial = agg.clone();
        partial.mode = AggregateMode::Partial;
        let result =
            SExpr::create_unary(agg, SExpr::create_unary(partial, s_expr.child(0)?.clone()));
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleSplitAggregate {
    fn default() -> Self {
        Self::new()
    }
}
