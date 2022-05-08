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

use std::any::Any;

use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::BasePlan;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::PlanType;
use crate::sql::plans::Scalar;

#[derive(Clone)]
pub struct LogicalInnerJoin {
    pub left_conditions: Vec<Scalar>,
    pub right_conditions: Vec<Scalar>,
}

impl BasePlan for LogicalInnerJoin {
    fn plan_type(&self) -> PlanType {
        PlanType::LogicalInnerJoin
    }

    fn is_physical(&self) -> bool {
        false
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        None
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        Some(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl LogicalPlan for LogicalInnerJoin {
    fn compute_relational_prop(&self, _expression: &SExpr) -> RelationalProperty {
        todo!()
    }
}
