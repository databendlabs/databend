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

mod logical_get;
mod logical_project;
mod physical_project;
mod physical_scan;

pub use logical_get::LogicalGet;
pub use logical_project::LogicalProject;
pub use logical_project::ProjectItem;
pub use physical_project::PhysicalProject;
pub use physical_scan::PhysicalScan;

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::optimizer::SExpr;

pub trait LogicalPlan {
    fn compute_relational_prop(&self, expression: &SExpr) -> RelationalProperty;

    fn as_plan(&self) -> Plan;
}

pub trait PhysicalPlan {
    fn compute_physical_prop(&self, expression: &SExpr) -> PhysicalProperty;

    fn compute_required_prop(&self, input_prop: &RequiredProperty) -> RequiredProperty;

    fn as_plan(&self) -> Plan;
}

/// Relational operator
#[derive(Clone, PartialEq, Debug)]
pub enum Plan {
    // Logical operators
    LogicalGet(LogicalGet),
    LogicalProject(LogicalProject),

    // Physical operators
    PhysicalScan(PhysicalScan),
    PhysicalProject(PhysicalProject),

    // Pattern
    Pattern,
}

impl Plan {
    // Check if two plans are in same variant
    pub fn kind_eq(&self, other: &Plan) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }

    pub fn is_logical(&self) -> bool {
        matches!(self, Plan::LogicalGet(_) | Plan::LogicalProject(_))
    }

    pub fn is_physical(&self) -> bool {
        matches!(self, Plan::PhysicalScan(_) | Plan::PhysicalProject(_))
    }

    pub fn as_logical_plan(&self) -> Option<&dyn LogicalPlan> {
        match self {
            Plan::LogicalGet(plan) => Some(plan),
            Plan::LogicalProject(plan) => Some(plan),
            _ => None,
        }
    }

    pub fn as_physical_plan(&self) -> Option<&dyn PhysicalPlan> {
        match self {
            Plan::PhysicalScan(plan) => Some(plan),
            Plan::PhysicalProject(plan) => Some(plan),
            _ => None,
        }
    }

    pub fn compute_relational_prop(&self, expression: &SExpr) -> Option<RelationalProperty> {
        self.as_logical_plan()
            .map(|plan| plan.compute_relational_prop(expression))
    }
}
