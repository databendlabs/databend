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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::DummyTableScan;

pub struct RulePushDownLimit {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownLimit {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownLimit,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RulePushDownLimit {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;

        if let Some(limit_val) = limit.limit &&  limit_val == 0{
            let dummy_scan = DummyTableScan;
            let result = SExpr::create_leaf(Arc::new(RelOperator::DummyTableScan(dummy_scan)));
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}