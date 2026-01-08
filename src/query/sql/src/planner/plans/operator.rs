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
use enum_as_inner::EnumAsInner;

use super::MutationSource;
use super::SubqueryExpr;
use crate::ScalarExpr;
use crate::impl_match_rel_op;
use crate::impl_try_from_rel_operator;
use crate::match_rel_op;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
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
use crate::plans::MaterializedCTE;
use crate::plans::MaterializedCTERef;
use crate::plans::Mutation;
use crate::plans::OptimizeCompactBlock as CompactBlock;
use crate::plans::ProjectSet;
use crate::plans::Scan;
use crate::plans::SecureFilter;
use crate::plans::Sort;
use crate::plans::Udf;
use crate::plans::UnionAll;
use crate::plans::Window;
use crate::plans::r_cte_scan::RecursiveCteScan;
use crate::plans::sequence::Sequence;

pub trait Operator {
    /// Get relational operator kind
    fn rel_op(&self) -> RelOp;

    /// Get arity of this operator
    fn arity(&self) -> usize {
        1
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(std::iter::empty())
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
    SecureFilter,
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
    MaterializedCTE,
    MaterializeCTERef,
    Sequence,
}

/// Relational operators
#[derive(Educe, EnumAsInner)]
#[educe(
    PartialEq(bound = false, attrs = "#[stacksafe::stacksafe]"),
    Eq,
    Hash(bound = false, attrs = "#[stacksafe::stacksafe]"),
    Clone(bound = false, attrs = "#[stacksafe::stacksafe]"),
    Debug(bound = false, attrs = "#[stacksafe::stacksafe]")
)]
pub enum RelOperator {
    Scan(Scan),
    Join(Join),
    EvalScalar(EvalScalar),
    Filter(Filter),
    SecureFilter(SecureFilter),
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
    CompactBlock(CompactBlock),
    MutationSource(MutationSource),
    MaterializedCTE(MaterializedCTE),
    MaterializedCTERef(MaterializedCTERef),
    Sequence(Sequence),
}

impl RelOperator {
    pub fn has_subquery(&self) -> bool {
        let mut iter = self.scalar_expr_iter();
        iter.any(|expr| expr.has_subquery())
    }

    pub fn support_lazy_materialize(&self) -> bool {
        !matches!(
            self,
            RelOperator::Join(_)
                | RelOperator::CacheScan(_)
                | RelOperator::UnionAll(_)
                | RelOperator::DummyTableScan(_)
                | RelOperator::ExpressionScan(_)
                | RelOperator::ConstantTableScan(_)
        )
    }

    pub fn collect_subquery(&self) -> Vec<SubqueryExpr> {
        let mut subqueries = Vec::new();
        for scalar in self.scalar_expr_iter() {
            scalar.collect_subquery(&mut subqueries);
        }
        subqueries
    }
}

impl Operator for RelOperator {
    fn rel_op(&self) -> RelOp {
        match_rel_op!(self, rel_op)
    }

    fn arity(&self) -> usize {
        match_rel_op!(self, arity)
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        match_rel_op!(self, scalar_expr_iter)
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        match_rel_op!(self, derive_relational_prop(rel_expr))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        match_rel_op!(self, derive_physical_prop(rel_expr))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        match_rel_op!(self, derive_stats(rel_expr))
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        match_rel_op!(
            self,
            compute_required_prop_child(ctx, rel_expr, child_index, required)
        )
    }

    fn compute_required_prop_children(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        match_rel_op!(
            self,
            compute_required_prop_children(ctx, rel_expr, required)
        )
    }
}

impl_try_from_rel_operator! {
    Scan,
    Join,
    EvalScalar,
    Filter,
    SecureFilter,
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
    RecursiveCteScan,
    AsyncFunction,
    Mutation,
    CompactBlock,
    MutationSource,
    MaterializedCTE,
    MaterializedCTERef,
    Sequence
}
