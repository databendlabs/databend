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

use common_exception::Result;
use enum_dispatch::enum_dispatch;

use super::aggregate::AggregatePlan;
use super::apply::CrossApply;
use super::eval_scalar::EvalScalar;
use super::filter::FilterPlan;
use super::hash_join::PhysicalHashJoin;
use super::limit::LimitPlan;
use super::logical_get::LogicalGet;
use super::logical_join::LogicalInnerJoin;
use super::max_one_row::Max1Row;
use super::pattern::PatternPlan;
use super::physical_scan::PhysicalScan;
use super::project::Project;
use super::sort::SortPlan;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;

#[enum_dispatch]
pub trait Operator {
    fn plan_type(&self) -> RelOp;

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
pub enum RelOp {
    // Logical operators
    LogicalGet,
    LogicalInnerJoin,

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
}

/// Relational operators
#[enum_dispatch(Operator)]
#[derive(Clone, Debug)]
pub enum RelOperator {
    LogicalGet(LogicalGet),
    LogicalInnerJoin(LogicalInnerJoin),

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
}
