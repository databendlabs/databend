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

use common_exception::ErrorCode;
use common_exception::Result;

use super::aggregate::Aggregate;
use super::eval_scalar::EvalScalar;
use super::filter::Filter;
use super::hash_join::PhysicalHashJoin;
use super::limit::Limit;
use super::logical_get::LogicalGet;
use super::logical_join::LogicalInnerJoin;
use super::pattern::PatternPlan;
use super::physical_scan::PhysicalScan;
use super::project::Project;
use super::sort::Sort;
use super::union_all::UnionAll;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::Exchange;

pub trait Operator {
    fn rel_op(&self) -> RelOp;

    fn is_physical(&self) -> bool;

    fn is_logical(&self) -> bool;

    fn is_pattern(&self) -> bool {
        false
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator>;

    fn as_physical(&self) -> Option<&dyn PhysicalOperator>;
}

pub trait LogicalOperator {
    fn derive_relational_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<RelationalProperty>;
}

pub trait PhysicalOperator {
    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty>;

    fn compute_required_prop_child<'a>(
        &self,
        rel_expr: &RelExpr<'a>,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty>;
}

/// Relational operator
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
    Exchange,
    UnionAll,

    // Pattern
    Pattern,
}

/// Relational operators
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RelOperator {
    LogicalGet(LogicalGet),
    LogicalInnerJoin(LogicalInnerJoin),

    PhysicalScan(PhysicalScan),
    PhysicalHashJoin(PhysicalHashJoin),

    Project(Project),
    EvalScalar(EvalScalar),
    Filter(Filter),
    Aggregate(Aggregate),
    Sort(Sort),
    Limit(Limit),
    Exchange(Exchange),
    UnionAll(UnionAll),

    Pattern(PatternPlan),
}

impl Operator for RelOperator {
    fn rel_op(&self) -> RelOp {
        match self {
            RelOperator::LogicalGet(rel_op) => rel_op.rel_op(),
            RelOperator::LogicalInnerJoin(rel_op) => rel_op.rel_op(),
            RelOperator::PhysicalScan(rel_op) => rel_op.rel_op(),
            RelOperator::PhysicalHashJoin(rel_op) => rel_op.rel_op(),
            RelOperator::Project(rel_op) => rel_op.rel_op(),
            RelOperator::EvalScalar(rel_op) => rel_op.rel_op(),
            RelOperator::Filter(rel_op) => rel_op.rel_op(),
            RelOperator::Aggregate(rel_op) => rel_op.rel_op(),
            RelOperator::Sort(rel_op) => rel_op.rel_op(),
            RelOperator::Limit(rel_op) => rel_op.rel_op(),
            RelOperator::Pattern(rel_op) => rel_op.rel_op(),
            RelOperator::Exchange(rel_op) => rel_op.rel_op(),
            RelOperator::UnionAll(rel_op) => rel_op.rel_op(),
        }
    }

    fn is_physical(&self) -> bool {
        match self {
            RelOperator::LogicalGet(rel_op) => rel_op.is_physical(),
            RelOperator::LogicalInnerJoin(rel_op) => rel_op.is_physical(),
            RelOperator::PhysicalScan(rel_op) => rel_op.is_physical(),
            RelOperator::PhysicalHashJoin(rel_op) => rel_op.is_physical(),
            RelOperator::Project(rel_op) => rel_op.is_physical(),
            RelOperator::EvalScalar(rel_op) => rel_op.is_physical(),
            RelOperator::Filter(rel_op) => rel_op.is_physical(),
            RelOperator::Aggregate(rel_op) => rel_op.is_physical(),
            RelOperator::Sort(rel_op) => rel_op.is_physical(),
            RelOperator::Limit(rel_op) => rel_op.is_physical(),
            RelOperator::Pattern(rel_op) => rel_op.is_physical(),
            RelOperator::Exchange(rel_op) => rel_op.is_physical(),
            RelOperator::UnionAll(rel_op) => rel_op.is_physical(),
        }
    }

    fn is_logical(&self) -> bool {
        match self {
            RelOperator::LogicalGet(rel_op) => rel_op.is_logical(),
            RelOperator::LogicalInnerJoin(rel_op) => rel_op.is_logical(),
            RelOperator::PhysicalScan(rel_op) => rel_op.is_logical(),
            RelOperator::PhysicalHashJoin(rel_op) => rel_op.is_logical(),
            RelOperator::Project(rel_op) => rel_op.is_logical(),
            RelOperator::EvalScalar(rel_op) => rel_op.is_logical(),
            RelOperator::Filter(rel_op) => rel_op.is_logical(),
            RelOperator::Aggregate(rel_op) => rel_op.is_logical(),
            RelOperator::Sort(rel_op) => rel_op.is_logical(),
            RelOperator::Limit(rel_op) => rel_op.is_logical(),
            RelOperator::Pattern(rel_op) => rel_op.is_logical(),
            RelOperator::Exchange(rel_op) => rel_op.is_logical(),
            RelOperator::UnionAll(rel_op) => rel_op.is_logical(),
        }
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        match self {
            RelOperator::LogicalGet(rel_op) => rel_op.as_logical(),
            RelOperator::LogicalInnerJoin(rel_op) => rel_op.as_logical(),
            RelOperator::PhysicalScan(rel_op) => rel_op.as_logical(),
            RelOperator::PhysicalHashJoin(rel_op) => rel_op.as_logical(),
            RelOperator::Project(rel_op) => rel_op.as_logical(),
            RelOperator::EvalScalar(rel_op) => rel_op.as_logical(),
            RelOperator::Filter(rel_op) => rel_op.as_logical(),
            RelOperator::Aggregate(rel_op) => rel_op.as_logical(),
            RelOperator::Sort(rel_op) => rel_op.as_logical(),
            RelOperator::Limit(rel_op) => rel_op.as_logical(),
            RelOperator::Pattern(rel_op) => rel_op.as_logical(),
            RelOperator::Exchange(rel_op) => rel_op.as_logical(),
            RelOperator::UnionAll(rel_op) => rel_op.as_logical(),
        }
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        match self {
            RelOperator::LogicalGet(rel_op) => rel_op.as_physical(),
            RelOperator::LogicalInnerJoin(rel_op) => rel_op.as_physical(),
            RelOperator::PhysicalScan(rel_op) => rel_op.as_physical(),
            RelOperator::PhysicalHashJoin(rel_op) => rel_op.as_physical(),
            RelOperator::Project(rel_op) => rel_op.as_physical(),
            RelOperator::EvalScalar(rel_op) => rel_op.as_physical(),
            RelOperator::Filter(rel_op) => rel_op.as_physical(),
            RelOperator::Aggregate(rel_op) => rel_op.as_physical(),
            RelOperator::Sort(rel_op) => rel_op.as_physical(),
            RelOperator::Limit(rel_op) => rel_op.as_physical(),
            RelOperator::Pattern(rel_op) => rel_op.as_physical(),
            RelOperator::Exchange(rel_op) => rel_op.as_physical(),
            RelOperator::UnionAll(rel_op) => rel_op.as_physical(),
        }
    }
}

impl From<LogicalGet> for RelOperator {
    fn from(v: LogicalGet) -> Self {
        Self::LogicalGet(v)
    }
}

impl TryFrom<RelOperator> for LogicalGet {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::LogicalGet(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to LogicalGet",
            ))
        }
    }
}

impl From<LogicalInnerJoin> for RelOperator {
    fn from(v: LogicalInnerJoin) -> Self {
        Self::LogicalInnerJoin(v)
    }
}

impl TryFrom<RelOperator> for LogicalInnerJoin {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::LogicalInnerJoin(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to LogicalInnerJoin",
            ))
        }
    }
}

impl From<PhysicalScan> for RelOperator {
    fn from(v: PhysicalScan) -> Self {
        Self::PhysicalScan(v)
    }
}

impl TryFrom<RelOperator> for PhysicalScan {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::PhysicalScan(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to PhysicalScan",
            ))
        }
    }
}

impl From<PhysicalHashJoin> for RelOperator {
    fn from(v: PhysicalHashJoin) -> Self {
        Self::PhysicalHashJoin(v)
    }
}

impl TryFrom<RelOperator> for PhysicalHashJoin {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::PhysicalHashJoin(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to PhysicalHashJoin",
            ))
        }
    }
}

impl From<Project> for RelOperator {
    fn from(v: Project) -> Self {
        Self::Project(v)
    }
}

impl TryFrom<RelOperator> for Project {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Project(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Project",
            ))
        }
    }
}

impl From<EvalScalar> for RelOperator {
    fn from(v: EvalScalar) -> Self {
        Self::EvalScalar(v)
    }
}

impl TryFrom<RelOperator> for EvalScalar {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::EvalScalar(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to EvalScalar",
            ))
        }
    }
}

impl From<Filter> for RelOperator {
    fn from(v: Filter) -> Self {
        Self::Filter(v)
    }
}

impl TryFrom<RelOperator> for Filter {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Filter(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Filter",
            ))
        }
    }
}

impl From<Aggregate> for RelOperator {
    fn from(v: Aggregate) -> Self {
        Self::Aggregate(v)
    }
}

impl TryFrom<RelOperator> for Aggregate {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Aggregate(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Aggregate",
            ))
        }
    }
}

impl From<Sort> for RelOperator {
    fn from(v: Sort) -> Self {
        Self::Sort(v)
    }
}

impl TryFrom<RelOperator> for Sort {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Sort(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Sort",
            ))
        }
    }
}

impl From<Limit> for RelOperator {
    fn from(v: Limit) -> Self {
        Self::Limit(v)
    }
}

impl TryFrom<RelOperator> for Limit {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Limit(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Limit",
            ))
        }
    }
}

impl From<PatternPlan> for RelOperator {
    fn from(v: PatternPlan) -> Self {
        Self::Pattern(v)
    }
}

impl TryFrom<RelOperator> for PatternPlan {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Pattern(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Pattern",
            ))
        }
    }
}

impl From<Exchange> for RelOperator {
    fn from(v: Exchange) -> Self {
        Self::Exchange(v)
    }
}

impl TryFrom<RelOperator> for Exchange {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Exchange(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to Exchange",
            ))
        }
    }
}

impl From<UnionAll> for RelOperator {
    fn from(v: UnionAll) -> Self {
        Self::UnionAll(v)
    }
}

impl TryFrom<RelOperator> for UnionAll {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::UnionAll(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot downcast RelOperator to UnionAll",
            ))
        }
    }
}
