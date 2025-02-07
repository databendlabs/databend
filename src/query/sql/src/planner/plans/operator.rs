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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use super::MutationSource;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::r_cte_scan::RecursiveCteScan;
use crate::plans::Aggregate;
use crate::plans::AsyncFunction;
use crate::plans::CacheScan;
use crate::plans::ConstantTableScan;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::Exchange;
use crate::plans::ExpressionScan;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::Limit;
use crate::plans::Mutation;
use crate::plans::OptimizeCompactBlock;
use crate::plans::ProjectSet;
use crate::plans::Scan;
use crate::plans::Sort;
use crate::plans::Udf;
use crate::plans::UnionAll;
use crate::plans::Window;

pub trait Operator {
    /// Get relational operator kind
    fn rel_op(&self) -> RelOp;

    /// Get arity of this operator
    fn arity(&self) -> usize {
        1
    }

    /// Derive relational property
    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty::default()))
    }

    /// Derive physical property
    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    /// Derive statistics information
    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo::default()))
    }

    /// Compute required property for child with index `child_index`
    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Ok(required.clone())
    }

    /// Enumerate all possible combinations of required property for children
    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![RequiredProperty::default(); self.arity()]])
    }
}

/// Relational operator
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RelOp {
    Scan,
    Join,
    EvalScalar,
    Filter,
    Aggregate,
    Sort,
    Limit,
    Exchange,
    UnionAll,
    DummyTableScan,
    Window,
    ProjectSet,
    ConstantTableScan,
    ExpressionScan,
    CacheScan,
    Udf,
    Udaf,
    AsyncFunction,
    RecursiveCteScan,
    MergeInto,
    CompactBlock,
    MutationSource,

    // Pattern
    Pattern,
}

/// Relational operators
#[derive(Educe)]
#[educe(
    PartialEq(bound = false, attrs = "#[recursive::recursive]"),
    Eq,
    Hash(bound = false, attrs = "#[recursive::recursive]"),
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
pub enum RelOperator {
    Scan(Scan),
    Join(Join),
    EvalScalar(EvalScalar),
    Filter(Filter),
    Aggregate(Aggregate),
    Sort(Sort),
    Limit(Limit),
    Exchange(Exchange),
    UnionAll(UnionAll),
    DummyTableScan(DummyTableScan),
    Window(Window),
    ProjectSet(ProjectSet),
    ConstantTableScan(ConstantTableScan),
    ExpressionScan(ExpressionScan),
    CacheScan(CacheScan),
    Udf(Udf),
    RecursiveCteScan(RecursiveCteScan),
    AsyncFunction(AsyncFunction),
    Mutation(Mutation),
    CompactBlock(OptimizeCompactBlock),
    MutationSource(MutationSource),
}

impl Operator for RelOperator {
    fn rel_op(&self) -> RelOp {
        match self {
            RelOperator::Scan(rel_op) => rel_op.rel_op(),
            RelOperator::Join(rel_op) => rel_op.rel_op(),
            RelOperator::EvalScalar(rel_op) => rel_op.rel_op(),
            RelOperator::Filter(rel_op) => rel_op.rel_op(),
            RelOperator::Aggregate(rel_op) => rel_op.rel_op(),
            RelOperator::Sort(rel_op) => rel_op.rel_op(),
            RelOperator::Limit(rel_op) => rel_op.rel_op(),
            RelOperator::Exchange(rel_op) => rel_op.rel_op(),
            RelOperator::UnionAll(rel_op) => rel_op.rel_op(),
            RelOperator::DummyTableScan(rel_op) => rel_op.rel_op(),
            RelOperator::ProjectSet(rel_op) => rel_op.rel_op(),
            RelOperator::Window(rel_op) => rel_op.rel_op(),
            RelOperator::ConstantTableScan(rel_op) => rel_op.rel_op(),
            RelOperator::ExpressionScan(rel_op) => rel_op.rel_op(),
            RelOperator::CacheScan(rel_op) => rel_op.rel_op(),
            RelOperator::Udf(rel_op) => rel_op.rel_op(),
            RelOperator::RecursiveCteScan(rel_op) => rel_op.rel_op(),
            RelOperator::AsyncFunction(rel_op) => rel_op.rel_op(),
            RelOperator::Mutation(rel_op) => rel_op.rel_op(),
            RelOperator::CompactBlock(rel_op) => rel_op.rel_op(),
            RelOperator::MutationSource(rel_op) => rel_op.rel_op(),
        }
    }

    fn arity(&self) -> usize {
        match self {
            RelOperator::Scan(rel_op) => rel_op.arity(),
            RelOperator::Join(rel_op) => rel_op.arity(),
            RelOperator::EvalScalar(rel_op) => rel_op.arity(),
            RelOperator::Filter(rel_op) => rel_op.arity(),
            RelOperator::Aggregate(rel_op) => rel_op.arity(),
            RelOperator::Sort(rel_op) => rel_op.arity(),
            RelOperator::Limit(rel_op) => rel_op.arity(),
            RelOperator::Exchange(rel_op) => rel_op.arity(),
            RelOperator::UnionAll(rel_op) => rel_op.arity(),
            RelOperator::DummyTableScan(rel_op) => rel_op.arity(),
            RelOperator::Window(rel_op) => rel_op.arity(),
            RelOperator::ProjectSet(rel_op) => rel_op.arity(),
            RelOperator::ConstantTableScan(rel_op) => rel_op.arity(),
            RelOperator::ExpressionScan(rel_op) => rel_op.arity(),
            RelOperator::CacheScan(rel_op) => rel_op.arity(),
            RelOperator::Udf(rel_op) => rel_op.arity(),
            RelOperator::RecursiveCteScan(rel_op) => rel_op.arity(),
            RelOperator::AsyncFunction(rel_op) => rel_op.arity(),
            RelOperator::Mutation(rel_op) => rel_op.arity(),
            RelOperator::CompactBlock(rel_op) => rel_op.arity(),
            RelOperator::MutationSource(rel_op) => rel_op.arity(),
        }
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        match self {
            RelOperator::Scan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Join(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::EvalScalar(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Filter(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Aggregate(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Sort(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Limit(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Exchange(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::UnionAll(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::DummyTableScan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::ProjectSet(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Window(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::ConstantTableScan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::ExpressionScan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::CacheScan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Udf(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::RecursiveCteScan(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::AsyncFunction(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::Mutation(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::CompactBlock(rel_op) => rel_op.derive_relational_prop(rel_expr),
            RelOperator::MutationSource(rel_op) => rel_op.derive_relational_prop(rel_expr),
        }
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        match self {
            RelOperator::Scan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Join(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::EvalScalar(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Filter(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Aggregate(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Sort(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Limit(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Exchange(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::UnionAll(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::DummyTableScan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::ProjectSet(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Window(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::ConstantTableScan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::ExpressionScan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::CacheScan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Udf(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::RecursiveCteScan(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::AsyncFunction(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::Mutation(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::CompactBlock(rel_op) => rel_op.derive_physical_prop(rel_expr),
            RelOperator::MutationSource(rel_op) => rel_op.derive_physical_prop(rel_expr),
        }
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        match self {
            RelOperator::Scan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Join(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::EvalScalar(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Filter(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Aggregate(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Sort(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Limit(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Exchange(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::UnionAll(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::DummyTableScan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::ProjectSet(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Window(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::ConstantTableScan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::ExpressionScan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::CacheScan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Udf(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::RecursiveCteScan(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::AsyncFunction(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::Mutation(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::CompactBlock(rel_op) => rel_op.derive_stats(rel_expr),
            RelOperator::MutationSource(rel_op) => rel_op.derive_stats(rel_expr),
        }
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        match self {
            RelOperator::Scan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Join(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::EvalScalar(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Filter(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Aggregate(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Sort(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Limit(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Exchange(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::UnionAll(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::DummyTableScan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Window(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::ProjectSet(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::ConstantTableScan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::ExpressionScan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::CacheScan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Udf(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::RecursiveCteScan(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::AsyncFunction(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::Mutation(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::CompactBlock(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
            RelOperator::MutationSource(rel_op) => {
                rel_op.compute_required_prop_child(ctx, rel_expr, child_index, required)
            }
        }
    }

    fn compute_required_prop_children(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        match self {
            RelOperator::Scan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Join(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::EvalScalar(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Filter(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Aggregate(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Sort(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Limit(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Exchange(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::UnionAll(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::DummyTableScan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Window(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::ProjectSet(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::ConstantTableScan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::ExpressionScan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::CacheScan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Udf(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::RecursiveCteScan(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::AsyncFunction(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::Mutation(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::CompactBlock(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
            RelOperator::MutationSource(rel_op) => {
                rel_op.compute_required_prop_children(ctx, rel_expr, required)
            }
        }
    }
}

impl From<Scan> for RelOperator {
    fn from(v: Scan) -> Self {
        Self::Scan(v)
    }
}

impl TryFrom<RelOperator> for Scan {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Scan(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Scan",
                value.rel_op()
            )))
        }
    }
}

impl From<Join> for RelOperator {
    fn from(v: Join) -> Self {
        Self::Join(v)
    }
}

impl TryFrom<RelOperator> for Join {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Join(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Join",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to EvalScalar",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Filter",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Aggregate",
                value.rel_op()
            )))
        }
    }
}

impl From<Window> for RelOperator {
    fn from(v: Window) -> Self {
        Self::Window(v)
    }
}

impl TryFrom<RelOperator> for Window {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Window(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Window",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Sort",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Limit",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Exchange",
                value.rel_op()
            )))
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
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to UnionAll",
                value.rel_op()
            )))
        }
    }
}

impl From<DummyTableScan> for RelOperator {
    fn from(v: DummyTableScan) -> Self {
        Self::DummyTableScan(v)
    }
}

impl TryFrom<RelOperator> for DummyTableScan {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::DummyTableScan(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to DummyTableScan",
                value.rel_op()
            )))
        }
    }
}

impl From<ProjectSet> for RelOperator {
    fn from(value: ProjectSet) -> Self {
        Self::ProjectSet(value)
    }
}

impl TryFrom<RelOperator> for ProjectSet {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> std::result::Result<Self, Self::Error> {
        if let RelOperator::ProjectSet(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to ProjectSet",
                value.rel_op()
            )))
        }
    }
}

impl From<ConstantTableScan> for RelOperator {
    fn from(value: ConstantTableScan) -> Self {
        Self::ConstantTableScan(value)
    }
}

impl From<ExpressionScan> for RelOperator {
    fn from(value: ExpressionScan) -> Self {
        Self::ExpressionScan(value)
    }
}

impl TryFrom<RelOperator> for ConstantTableScan {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> std::result::Result<Self, Self::Error> {
        if let RelOperator::ConstantTableScan(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to ConstantTableScan",
                value.rel_op()
            )))
        }
    }
}

impl From<Udf> for RelOperator {
    fn from(value: Udf) -> Self {
        Self::Udf(value)
    }
}

impl TryFrom<RelOperator> for Udf {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> std::result::Result<Self, Self::Error> {
        if let RelOperator::Udf(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to Udf",
                value.rel_op()
            )))
        }
    }
}

impl TryFrom<RelOperator> for RecursiveCteScan {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> std::result::Result<Self, Self::Error> {
        if let RelOperator::RecursiveCteScan(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to RecursiveCteScan",
                value.rel_op()
            )))
        }
    }
}
impl From<AsyncFunction> for RelOperator {
    fn from(value: AsyncFunction) -> Self {
        Self::AsyncFunction(value)
    }
}

impl TryFrom<RelOperator> for AsyncFunction {
    type Error = ErrorCode;

    fn try_from(value: RelOperator) -> std::result::Result<Self, Self::Error> {
        if let RelOperator::AsyncFunction(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to AsyncFunction",
                value.rel_op()
            )))
        }
    }
}

impl From<Mutation> for RelOperator {
    fn from(v: Mutation) -> Self {
        Self::Mutation(v)
    }
}

impl TryFrom<RelOperator> for Mutation {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::Mutation(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to MergeInto",
                value.rel_op()
            )))
        }
    }
}

impl From<OptimizeCompactBlock> for RelOperator {
    fn from(v: OptimizeCompactBlock) -> Self {
        Self::CompactBlock(v)
    }
}

impl TryFrom<RelOperator> for OptimizeCompactBlock {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::CompactBlock(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to OptimizeCompactBlock",
                value.rel_op()
            )))
        }
    }
}

impl From<MutationSource> for RelOperator {
    fn from(v: MutationSource) -> Self {
        Self::MutationSource(v)
    }
}

impl TryFrom<RelOperator> for MutationSource {
    type Error = ErrorCode;
    fn try_from(value: RelOperator) -> Result<Self> {
        if let RelOperator::MutationSource(value) = value {
            Ok(value)
        } else {
            Err(ErrorCode::Internal(format!(
                "Cannot downcast {:?} to MutationSource",
                value.rel_op()
            )))
        }
    }
}
