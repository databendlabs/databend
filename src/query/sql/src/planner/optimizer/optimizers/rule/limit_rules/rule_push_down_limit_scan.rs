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

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::Scan;

/// Input:  Limit
///           \
///          Scan
///
/// Output:
///         Limit
///           \
///           Scan(padding limit)
pub struct RulePushDownLimitScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimitScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimitScan,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownLimitScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit = s_expr.plan().as_any().downcast_ref::<Limit>().unwrap();
        let Some(mut count) = limit.limit else {
            return Ok(());
        };
        count += limit.offset;

        let child = s_expr.child(0)?;
        let mut get = child
            .plan()
            .as_any()
            .downcast_ref::<Scan>()
            .unwrap()
            .clone();
        get.limit = Some(get.limit.map_or(count, |c| cmp::max(c, count)));
        let get = SExpr::create_leaf(get);

        let mut result = s_expr.replace_children(vec![Arc::new(get)]);
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownLimitScan {
    fn default() -> Self {
        Self::new()
    }
}
