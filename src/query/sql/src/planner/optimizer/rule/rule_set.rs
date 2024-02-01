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

use std::hash::Hash;
use std::hash::Hasher;

use num_traits::FromPrimitive;
use roaring::RoaringBitmap;

use crate::optimizer::rule::RuleID;

/// Set of `Rule`
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RuleSet {
    rules: RoaringBitmap,
}

impl RuleSet {
    pub fn create() -> Self {
        RuleSet {
            rules: RoaringBitmap::new(),
        }
    }

    pub fn create_with_ids(ids: Vec<RuleID>) -> Self {
        let mut rule_set = Self::create();
        for id in ids {
            rule_set.rules.insert(id as u32);
        }
        rule_set
    }

    pub fn insert(&mut self, id: RuleID) {
        self.rules.insert(id as u32);
    }

    pub fn contains(&self, id: &RuleID) -> bool {
        self.rules.contains(*id as u32)
    }

    pub fn remove(&mut self, id: &RuleID) {
        self.rules.remove(*id as u32);
    }

    pub fn intersect(&self, other: &RuleSet) -> RuleSet {
        let mut rule_set = Self::create();
        rule_set.rules = self.rules.clone() & other.rules.clone();
        rule_set
    }

    pub fn iter(&self) -> impl Iterator<Item = RuleID> + '_ {
        self.rules.iter().map(|v| RuleID::from_u32(v).unwrap())
    }
}

/// A bitmap to store information about applied rules
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AppliedRules {
    rules: RuleSet,
}

impl AppliedRules {
    pub fn set(&mut self, id: &RuleID, v: bool) {
        if v {
            self.rules.insert(*id);
        } else {
            self.rules.remove(id);
        }
    }

    pub fn get(&self, id: &RuleID) -> bool {
        self.rules.contains(id)
    }

    pub fn clear(&mut self) {
        self.rules = RuleSet::create();
    }
}

impl Hash for AppliedRules {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rules.iter().for_each(|id| id.hash(state))
    }
}
