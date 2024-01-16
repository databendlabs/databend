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

use crate::optimizer::RuleID;
use crate::optimizer::RuleSet;

pub fn get_explore_rule_set(optimized: bool) -> RuleSet {
    if optimized {
        rule_set_dphyp()
    } else {
        rule_set_rs_l1()
    }
}

/// The join order has been optimized by dphyp, therefore we will not change the join order
/// and only attempt to exchange the order of build and probe.
fn rule_set_dphyp() -> RuleSet {
    RuleSet::create_with_ids(vec![RuleID::EagerAggregation])
}

/// Get rule set of join order RS-L1, which will only generate left-deep trees.
/// Read paper "The Complexity of Transformation-Based Join Enumeration" for more details.
fn rule_set_rs_l1() -> RuleSet {
    RuleSet::create_with_ids(vec![
        RuleID::CommuteJoinBaseTable,
        RuleID::LeftExchangeJoin,
        RuleID::EagerAggregation,
        RuleID::CommuteJoin,
    ])
}
