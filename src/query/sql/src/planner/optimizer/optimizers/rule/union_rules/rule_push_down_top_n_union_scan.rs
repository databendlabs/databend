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

use crate::match_op;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::optimizers::rule::scan_order::push_down_scan_order;
use crate::plans::RelOperator;

/// Matches: TopN -> [EvalScalar ->] UnionAll
///
/// Install the TopN sort keys as the `order_by` of every branch scan of the
/// union, translated into the symbols each branch outputs them as. The
/// physical plan then registers one runtime TopN filter per branch scan, all
/// sharing the boundary published by the TopN above the union.
///
/// Only the ordering is pushed down, not the limit: the scans still return
/// all rows that survive the runtime filter, and the TopN keeps the result
/// exact. See [`push_down_scan_order`] for the operators crossed and the
/// branches skipped.
pub struct RulePushDownTopNUnionScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownTopNUnionScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownTopNUnionScan,
            matchers: vec![
                match_op!(TopN -> UnionAll[*, *]),
                match_op!(TopN -> EvalScalar -> UnionAll[*, *]),
            ],
        }
    }
}

impl Rule for RulePushDownTopNUnionScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let RelOperator::TopN(top_n) = s_expr.plan() else {
            return Ok(());
        };

        let Some(input) = push_down_scan_order(s_expr.unary_child(), &top_n.items) else {
            return Ok(());
        };
        let mut result = s_expr.replace_children([Arc::new(input)]);
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownTopNUnionScan {
    fn default() -> Self {
        Self::new()
    }
}
