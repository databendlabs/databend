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

use std::cmp;
use std::sync::Arc;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;

/// Input:  Limit
///           \
///          Aggregate
///             \
///              *
///
/// Output: Limit
///           \
///          Aggregate(padding limit)
///             \
///               *
pub struct RulePushDownLimitAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimitAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitAggregate,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::Leaf],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownLimitAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(mut count) = limit.limit {
            count += limit.offset;
            let agg = s_expr.child(0)?;
            let mut agg_limit: Aggregate = agg.plan().clone().try_into()?;

            agg_limit.limit = Some(agg_limit.limit.map_or(count, |c| cmp::max(c, count)));
            let agg = SExpr::create_unary(
                Arc::new(RelOperator::Aggregate(agg_limit)),
                Arc::new(agg.child(0)?.clone()),
            );

            let mut result = s_expr.replace_children(vec![Arc::new(agg)]);
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
