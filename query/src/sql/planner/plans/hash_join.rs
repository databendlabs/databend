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

use super::JoinType;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

#[derive(Clone, Debug)]
pub struct PhysicalHashJoin {
    pub build_keys: Vec<Scalar>,
    pub probe_keys: Vec<Scalar>,
    pub join_type: JoinType,
}

impl Operator for PhysicalHashJoin {
    fn plan_type(&self) -> RelOp {
        RelOp::PhysicalHashJoin
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        Some(self)
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        None
    }
}

impl PhysicalPlan for PhysicalHashJoin {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        todo!()
    }
}
