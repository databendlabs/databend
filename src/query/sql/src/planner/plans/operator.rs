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
use crate::ColumnSet;
use crate::ScalarExpr;
use crate::impl_match_rel_op;
use crate::impl_try_from_rel_operator;
use crate::match_rel_op;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatContext;
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
use crate::plans::Sort;
use crate::plans::TopN;
use crate::plans::Udf;
use crate::plans::UnionAll;
use crate::plans::Window;
use crate::plans::WindowGroup;
use crate::plans::r_cte_scan::RecursiveCteScan;
use crate::plans::sequence::Sequence;

pub(crate) fn derive_outer_columns<'a, I>(
    mut outer_columns: ColumnSet,
    available_columns: &ColumnSet,
    scalar_exprs: I,
) -> ColumnSet
where
    I: IntoIterator<Item = &'a ScalarExpr>,
{
    let mut scalar_columns = ColumnSet::new();
    for scalar in scalar_exprs {
        scalar.collect_used_columns(&mut scalar_columns);
    }
    scalar_columns.retain(|column| !available_columns.contains(column));
    outer_columns.extend(scalar_columns);
    outer_columns
}

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

    fn derive_outer_columns(
        &self,
        outer_columns: ColumnSet,
        available_columns: &ColumnSet,
    ) -> ColumnSet {
        derive_outer_columns(outer_columns, available_columns, self.scalar_expr_iter())
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
    fn derive_stats(&self, _rel_expr: &RelExpr, _stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
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
    TopN,
    Exchange,
    UnionAll,
    DummyTableScan,
    Window,
    WindowGroup,
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
    TopN(TopN),
    Exchange(Exchange),
    UnionAll(UnionAll),
    DummyTableScan(DummyTableScan),
    Window(Window),
    WindowGroup(WindowGroup),
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
            RelOperator::CacheScan(_)
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

    fn derive_stats(&self, rel_expr: &RelExpr, stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
        match_rel_op!(self, derive_stats(rel_expr, stat_ctx))
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
    Aggregate,
    Sort,
    Limit,
    TopN,
    Exchange,
    UnionAll,
    DummyTableScan,
    Window,
    WindowGroup,
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

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::optimizer::ir::SExpr;
    use crate::plans::BoundColumnRef;
    use crate::plans::ScalarItem;
    use crate::plans::Scan;
    use crate::plans::SortItem;
    use crate::plans::WindowFuncType;
    use crate::plans::WindowPartition;

    fn column(index: usize) -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(DataType::Number(NumberDataType::Int32)),
                Visibility::Visible,
            )
            .build(),
        })
    }

    fn column_set(indices: &[usize]) -> ColumnSet {
        indices.iter().copied().map(Symbol::new).collect()
    }

    fn scan(columns: &[usize]) -> SExpr {
        SExpr::create_leaf(Scan {
            columns: column_set(columns),
            ..Default::default()
        })
    }

    fn assert_outer_columns(name: &str, s_expr: &SExpr) -> Result<()> {
        let property = RelExpr::with_s_expr(s_expr).derive_relational_prop()?;
        assert_eq!(property.outer_columns, column_set(&[0]), "{name}");
        Ok(())
    }

    #[test]
    fn test_relational_properties_track_scalar_outer_columns() -> Result<()> {
        let aggregate = Aggregate {
            group_items: vec![ScalarItem {
                scalar: column(0),
                index: Symbol::new(2),
            }],
            ..Default::default()
        };
        assert_outer_columns("aggregate", &SExpr::create_unary(aggregate, scan(&[1])))?;

        let window = Window {
            span: None,
            index: Symbol::new(2),
            function: WindowFuncType::RowNumber,
            arguments: vec![],
            partition_by: vec![ScalarItem {
                scalar: column(0),
                index: Symbol::new(3),
            }],
            order_by: vec![],
            frame: Default::default(),
            limit: None,
            top: None,
        };
        assert_outer_columns("window", &SExpr::create_unary(window, scan(&[1])))?;

        let union = UnionAll {
            left_outputs: vec![(Symbol::new(2), Some(column(0)))],
            right_outputs: vec![(Symbol::new(2), None)],
            cte_scan_names: vec![],
            logical_recursive_cte_id: None,
            output_indexes: vec![Symbol::new(2)],
        };
        assert_outer_columns(
            "union",
            &SExpr::create_binary(union, scan(&[1]), scan(&[1])),
        )?;

        let sort = Sort {
            items: vec![],
            limit: None,
            after_exchange: None,
            pre_projection: None,
            window_partition: Some(WindowPartition {
                partition_by: vec![ScalarItem {
                    scalar: column(0),
                    index: Symbol::new(3),
                }],
                top: None,
                func: WindowFuncType::RowNumber,
            }),
        };
        assert_outer_columns("sort", &SExpr::create_unary(sort, scan(&[1, 3])))?;

        let top_n = TopN {
            items: vec![SortItem {
                index: Symbol::new(0),
                asc: true,
                nulls_first: false,
            }],
            limit: 1,
            offset: 0,
            lazy_columns: ColumnSet::new(),
            after_exchange: None,
        };
        assert_outer_columns("top_n", &SExpr::create_unary(top_n, scan(&[1])))?;

        Ok(())
    }
}
