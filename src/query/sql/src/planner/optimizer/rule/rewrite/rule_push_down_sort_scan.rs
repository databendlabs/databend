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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Sort;

/// Input:  Sort
///           \
///          Scan
///
/// Output:
///         Sort
///           \
///           Scan(padding order_by and limit)
pub struct RulePushDownSortScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownSortScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownSortScan,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Sort,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownSortScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let child = s_expr.child(0)?;
        let mut get: Scan = child.plan().clone().try_into()?;
        if get.order_by.is_none() {
            get.order_by = Some(sort.items);
        }
        if let Some(limit) = sort.limit {
            get.limit = Some(get.limit.map_or(limit, |c| cmp::max(c, limit)));
        }
        let get = SExpr::create_leaf(Arc::new(RelOperator::Scan(get)));

        let mut result = s_expr.replace_children(vec![Arc::new(get)]);
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
