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

mod aggregate;
mod expression;
mod filter;
mod having;
mod logical_get;
mod pattern;
mod physical_scan;
mod project;
mod scalar;

use std::any::Any;

pub use aggregate::AggregatePlan;
use enum_dispatch::enum_dispatch;
pub use expression::ExpressionPlan;
pub use filter::FilterPlan;
pub use having::HavingPlan;
pub use logical_get::LogicalGet;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use project::ProjectItem;
pub use project::ProjectPlan;
pub use scalar::Scalar;

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;

#[enum_dispatch]
pub trait BasePlan: Any {
    fn plan_type(&self) -> PlanType;

    fn is_physical(&self) -> bool;

    fn is_logical(&self) -> bool;

    fn is_pattern(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan>;

    fn as_logical(&self) -> Option<&dyn LogicalPlan>;

    fn as_any(&self) -> &dyn Any;
}

pub trait LogicalPlan {
    fn compute_relational_prop(&self, expression: &SExpr) -> RelationalProperty;
}

pub trait PhysicalPlan {
    fn compute_physical_prop(&self, expression: &SExpr) -> PhysicalProperty;
}

/// Relational operator
#[derive(Clone, PartialEq, Debug)]
pub enum PlanType {
    // Logical operators
    LogicalGet,

    // Physical operators
    PhysicalScan,

    // Operators that are both logical and physical
    Project,
    Expression,
    Filter,
    Aggregate,
    Having,

    // Pattern
    Pattern,
}

#[enum_dispatch(BasePlan)]
#[derive(Clone)]
pub enum BasePlanImpl {
    LogicalGet(LogicalGet),
    PhysicalScan(PhysicalScan),
    Project(ProjectPlan),
    Expression(ExpressionPlan),
    Filter(FilterPlan),
    Aggregate(AggregatePlan),
    Having(HavingPlan),
    Pattern(PatternPlan),
}
