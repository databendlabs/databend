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
mod apply;
mod eval_scalar;
mod explain;
mod filter;
mod hash_join;
mod limit;
mod logical_get;
mod logical_join;
mod max_one_row;
mod pattern;
mod physical_scan;
mod project;
mod scalar;
mod sort;

pub use aggregate::AggregatePlan;
pub use apply::CrossApply;
use common_exception::Result;
use enum_dispatch::enum_dispatch;
pub use eval_scalar::EvalScalar;
pub use eval_scalar::ScalarItem;
pub use explain::ExplainPlan;
pub use filter::FilterPlan;
pub use hash_join::PhysicalHashJoin;
pub use limit::LimitPlan;
pub use logical_get::LogicalGet;
pub use logical_join::JoinType;
pub use logical_join::LogicalJoin;
pub use max_one_row::Max1Row;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use project::Project;
pub use scalar::*;
pub use sort::SortItem;
pub use sort::SortPlan;

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;

#[enum_dispatch]
pub trait Operator {
    fn plan_type(&self) -> PlanType;

    fn is_physical(&self) -> bool;

    fn is_logical(&self) -> bool;

    fn is_pattern(&self) -> bool {
        false
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan>;

    fn as_physical(&self) -> Option<&dyn PhysicalPlan>;
}

pub trait LogicalPlan {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty>;
}

pub trait PhysicalPlan {
    fn compute_physical_prop(&self, expression: &SExpr) -> PhysicalProperty;
}

/// Relational operator
#[derive(Clone, PartialEq, Debug)]
pub enum PlanType {
    // Logical operators
    LogicalGet,
    LogicalJoin,

    // Physical operators
    PhysicalScan,
    PhysicalHashJoin,

    // Operators that are both logical and physical
    Project,
    EvalScalar,
    Filter,
    Aggregate,
    Sort,
    Limit,
    CrossApply,
    Max1Row,

    // Pattern
    Pattern,

    // Explain
    Explain,
}

/// Relational operators
#[enum_dispatch(Operator)]
#[derive(Clone, Debug)]
pub enum RelOperator {
    LogicalGet(LogicalGet),
    LogicalJoin(LogicalJoin),

    PhysicalScan(PhysicalScan),
    PhysicalHashJoin(PhysicalHashJoin),

    Project(Project),
    EvalScalar(EvalScalar),
    Filter(FilterPlan),
    Aggregate(AggregatePlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    CrossApply(CrossApply),
    Max1Row(Max1Row),

    Pattern(PatternPlan),

    Explain(ExplainPlan),
}
