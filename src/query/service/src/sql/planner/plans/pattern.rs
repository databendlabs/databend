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

use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PatternPlan {
    pub plan_type: RelOp,
}

impl Operator for PatternPlan {
    fn rel_op(&self) -> RelOp {
        self.plan_type.clone()
    }

    fn is_physical(&self) -> bool {
        false
    }

    fn is_logical(&self) -> bool {
        false
    }

    fn is_pattern(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        None
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        None
    }
}
