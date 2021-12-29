// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::rule::factory::RuleFactory;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::RulePtr;

/// Set of `Rule`
pub struct RuleSet {
    rules: HashMap<RuleID, RulePtr>,
}

impl RuleSet {
    pub fn create() -> Self {
        RuleSet {
            rules: HashMap::new(),
        }
    }

    pub fn create_with_ids(ids: Vec<RuleID>) -> Result<Self> {
        let factory = RuleFactory::create();
        let mut rule_set = Self::create();
        for id in ids {
            if rule_set.contains(&id) {
                return Err(ErrorCode::LogicalError(format!(
                    "Duplicated Rule: {}",
                    id.name()
                )));
            }
            rule_set.insert(factory.create_rule(id)?);
        }

        Ok(rule_set)
    }

    pub fn insert(&mut self, rule: RulePtr) {
        self.rules.insert(rule.id(), rule);
    }

    pub fn contains(&self, id: &RuleID) -> bool {
        self.rules.contains_key(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &RulePtr> {
        self.rules.values()
    }
}
