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

use roaring::RoaringBitmap;

use crate::optimizer::RuleID;

pub fn calc_explore_rule_set(enable_bushy_join: bool) -> RoaringBitmap {
    if enable_bushy_join {
        calc_join_rule_set_rs_b2()
    } else {
        calc_join_rule_set_rs_l1()
    }
}

/// Get rule set of join order RS-B2, which may generate bushy trees.
/// Read paper "The Complexity of Transformation-Based Join Enumeration" for more details.
fn calc_join_rule_set_rs_b2() -> RoaringBitmap {
    (RuleID::CommuteJoin as u32..RuleID::CommuteJoinBaseTable as u32).collect::<RoaringBitmap>()
}

/// Get rule set of join order RS-L1, which will only generate left-deep trees.
/// Read paper "The Complexity of Transformation-Based Join Enumeration" for more details.
fn calc_join_rule_set_rs_l1() -> RoaringBitmap {
    (RuleID::CommuteJoinBaseTable as u32..RuleID::RightExchangeJoin as u32)
        .collect::<RoaringBitmap>()
}
