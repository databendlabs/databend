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

use std::fmt;

use pb::txn_condition::ConditionResult;

use crate::protobuf as pb;

impl fmt::Display for ConditionResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let x = match self {
            ConditionResult::Eq => "==",
            ConditionResult::Gt => ">",
            ConditionResult::Ge => ">=",
            ConditionResult::Lt => "<",
            ConditionResult::Le => "<=",
            ConditionResult::Ne => "!=",
        };
        write!(f, "{}", x)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_condition_result() {
        assert_eq!(format!("{}", ConditionResult::Eq), "==");
        assert_eq!(format!("{}", ConditionResult::Gt), ">");
        assert_eq!(format!("{}", ConditionResult::Ge), ">=");
        assert_eq!(format!("{}", ConditionResult::Lt), "<");
        assert_eq!(format!("{}", ConditionResult::Le), "<=");
        assert_eq!(format!("{}", ConditionResult::Ne), "!=");
    }
}
