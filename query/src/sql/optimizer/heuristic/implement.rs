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

use common_exception::Result;
use lazy_static::lazy_static;

use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::RuleSet;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::SExpr;

lazy_static! {
    static ref DEFAULT_IMPLEMENT_RULES: Vec<RuleID> =
        vec![RuleID::ImplementGet, RuleID::ImplementHashJoin];
}

pub struct HeuristicImplementor {
    implement_rule_list: RuleSet,
}

impl HeuristicImplementor {
    pub fn new() -> Self {
        HeuristicImplementor {
            implement_rule_list: RuleSet::create_with_ids(DEFAULT_IMPLEMENT_RULES.clone()).unwrap(),
        }
    }

    pub fn implement(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        for rule in self.implement_rule_list.iter() {
            if s_expr.match_pattern(rule.pattern()) {
                rule.apply(s_expr, state)?;
                break;
            }
        }
        Ok(())
    }
}
