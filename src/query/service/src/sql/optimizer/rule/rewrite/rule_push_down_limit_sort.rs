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

use std::cmp;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Limit;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::RelOp::Pattern;
use crate::sql::plans::RelOp::Sort;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Sort as logsort;

/// Input:  Limit
///           \
///          Sort
///             \
///              *
///
/// Output: Limit
///           \
///          Sort(padding limit)
///             \
///               *
pub struct RulePushDownLimitSort {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownLimitSort {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitSort,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Limit,
                }
                .into(),
                SExpr::create_unary(
                    PatternPlan { plan_type: Sort }.into(),
                    SExpr::create_leaf(PatternPlan { plan_type: Pattern }.into()),
                ),
            ),
        }
    }
}

impl Rule for RulePushDownLimitSort {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(mut count) = limit.limit {
            count += limit.offset;
            let sort = s_expr.child(0)?;
            let mut sort_limit: logsort = sort.plan().clone().try_into()?;
            sort_limit.limit = Some(sort_limit.limit.map_or(count, |c| cmp::max(c, count)));
            let sort = SExpr::create_unary(RelOperator::Sort(sort_limit), sort.child(0)?.clone());
            state.add_result(s_expr.replace_children(vec![sort]));
        }
        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
