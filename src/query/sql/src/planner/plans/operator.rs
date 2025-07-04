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
use super::SubqueryExpr;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
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
use crate::plans::WindowFuncType;
use crate::ScalarExpr;

pub type OperatorRef = Arc<dyn Operator>;

pub trait Operator: Send + Sync + 'static {
    /// Get relational operator kind
    fn rel_op(&self) -> RelOp;

    /// Get arity of this operator
    fn arity(&self) -> usize {
        1
    }

    fn as_any(&self) -> &dyn std::any::Any;

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr>> {
        Box::new(std::iter::empty())
    }

    fn has_subquery(&self) -> bool {
        let mut iter = self.scalar_expr_iter();
        iter.any(|expr| expr.has_subquery())
    }

    fn collect_subquery(&self) -> Vec<SubqueryExpr> {
        let mut subqueries = Vec::new();
        for scalar in self.scalar_expr_iter() {
            scalar.collect_subquery(&mut subqueries);
        }
        subqueries
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

    fn try_downcast_ref(&self) -> Option<&Self> {
        self.as_any().downcast_ref::<Self>()
    }

    fn try_downcast_mut(&mut self) -> Option<&mut Self> {
        self.as_any().downcast_mut::<Self>()
    }
}

/// Relational operator
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
    AsyncFunction,
    RecursiveCteScan,
    MergeInto,
    CompactBlock,
    MutationSource,
    // Pattern
}

#[macro_export]
macro_rules! with_match_rel_op {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Scan => Scan,
                Join => Join,
                EvalScalar => EvalScalar,
                Filter => Filter,
                Aggregate => Aggregate,
                Sort => Sort,
                Limit => Limit,
                Exchange => Exchange,
                UnionAll => UnionAll,
                DummyTableScan => DummyTableScan,
                Window => Window,
                ProjectSet => ProjectSet,
                ConstantTableScan => ConstantTableScan,
                ExpressionScan => ExpressionScan,
                CacheScan => CacheScan,
                Udf => Udf,
                Udaf => Udaf,
                AsyncFunction => AsyncFunction,
                RecursiveCteScan => RecursiveCteScan,
                MergeInto => MergeInto,
                CompactBlock => CompactBlock,
                MutationSource => MutationSource,
            ],
            $($tail)*
        }
    }
}
