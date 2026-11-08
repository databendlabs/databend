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

use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use crate::Symbol;
use crate::plans::Operator;

/// Operator, state and cache policy for a plan-tree stage. Logical and physical
/// kinds implement this contract independently; rewrites keep their operator type
/// and reset only the state prescribed by that stage.
pub trait ExprKind: Sized + Send + Sync {
    type Operator: Operator + Clone + Debug + Eq + Hash + Send + Sync;
    type State: Clone + Eq + Hash + Send + Sync;
    const NAME: &'static str;

    fn rewritten_state(state: &Self::State) -> Self::State;
    fn rel_expr(expr: &Expr<Self>) -> crate::optimizer::ir::RelExpr<'_>;
    fn relational_cache(
        state: &Self::State,
    ) -> &Arc<OnceLock<Arc<crate::optimizer::ir::RelationalProperty>>>;
    fn statistics_cache(state: &Self::State)
    -> &Arc<OnceLock<Arc<crate::optimizer::ir::StatInfo>>>;
    fn validate_types(expr: &Expr<Self>, metadata: &crate::MetadataRef) -> Result<()>;
    fn validate_column_scope(
        expr: &Expr<Self>,
        metadata: &crate::MetadataRef,
        ambient: &crate::ColumnSet,
    ) -> Result<()>;
}

#[derive(Educe)]
#[educe(
    PartialEq(bound = false, attrs = "#[recursive::recursive]"),
    Eq,
    Hash(bound = false, attrs = "#[recursive::recursive]"),
    Clone(bound = false, attrs = "#[recursive::recursive]")
)]
pub struct Expr<K: ExprKind> {
    pub plan: Arc<K::Operator>,
    pub children: Vec<Arc<Self>>,
    pub(crate) state: K::State,
}

impl<K: ExprKind> Expr<K> {
    pub fn plan(&self) -> &K::Operator {
        &self.plan
    }

    pub fn children(&self) -> impl Iterator<Item = &Self> {
        self.children.iter().map(|v| v.as_ref())
    }

    pub fn child(&self, n: usize) -> Result<&Self> {
        self.children
            .get(n)
            .map(|v| v.as_ref())
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid children index: {}", n)))
    }

    pub fn unary_child(&self) -> &Self {
        debug_assert_eq!(self.children.len(), 1);
        &self.children[0]
    }

    pub fn unary_child_arc(&self) -> Arc<Self> {
        assert_eq!(self.children.len(), 1);
        self.children[0].clone()
    }

    pub fn left_child(&self) -> &Self {
        debug_assert_eq!(self.children.len(), 2);
        &self.children[0]
    }

    pub fn left_child_arc(&self) -> Arc<Self> {
        assert_eq!(self.children.len(), 2);
        self.children[0].clone()
    }

    pub fn right_child(&self) -> &Self {
        debug_assert_eq!(self.children.len(), 2);
        &self.children[1]
    }

    pub fn right_child_arc(&self) -> Arc<Self> {
        assert_eq!(self.children.len(), 2);
        self.children[1].clone()
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn replace_children(&self, children: impl IntoIterator<Item = Arc<Self>>) -> Self {
        Self {
            plan: self.plan.clone(),
            children: children.into_iter().collect(),
            state: K::rewritten_state(&self.state),
        }
    }

    pub fn derive_relational_prop(&self) -> Result<Arc<crate::optimizer::ir::RelationalProperty>> {
        use crate::plans::Operator;
        let prop = K::relational_cache(&self.state)
            .get_or_try_init(|| self.plan.derive_relational_prop(&K::rel_expr(self)))?;
        Ok(prop.clone())
    }

    pub fn derive_cardinality(
        &self,
        ctx: &crate::optimizer::ir::StatContext,
    ) -> Result<Arc<crate::optimizer::ir::StatInfo>> {
        use crate::plans::Operator;
        let stats = K::statistics_cache(&self.state)
            .get_or_try_init(|| self.plan.derive_stats(&K::rel_expr(self), ctx))?;
        Ok(stats.clone())
    }
}

#[derive(Clone, Default)]
pub struct ScanRequiredColumns {
    pub columns: BTreeSet<Symbol>,
    pub inverted_index: Option<InvertedIndexInfo>,
    pub vector_index: Option<VectorIndexInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Left,
    Right,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }

    pub fn child<K: ExprKind>(self, s_expr: &Expr<K>) -> Arc<Expr<K>> {
        match self {
            Side::Left => s_expr.left_child_arc(),
            Side::Right => s_expr.right_child_arc(),
        }
    }
}

impl<K: ExprKind> Expr<K> {
    pub fn build_side_child(&self) -> &Expr<K> {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[1]
    }

    pub fn probe_side_child(&self) -> &Expr<K> {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[0]
    }

    #[recursive::recursive]
    pub fn get_udfs(&self) -> Result<HashSet<&String>> {
        let mut udfs = HashSet::new();
        let iter = self.plan.scalar_expr_iter();
        for scalar in iter {
            for udf in scalar.get_udf_names()? {
                udfs.insert(udf);
            }
        }

        for child in &self.children {
            let udf = child.get_udfs()?;
            udf.iter().for_each(|udf| {
                udfs.insert(*udf);
            })
        }
        Ok(udfs)
    }
}

impl<K: ExprKind> Expr<K> {
    pub fn replace_left_child(&self, left: impl Into<Arc<Self>>) -> Self {
        assert_eq!(self.children.len(), 2);
        Self {
            plan: self.plan.clone(),
            state: K::rewritten_state(&self.state),
            children: vec![left.into(), self.children[1].clone()],
        }
    }

    pub fn replace_right_child(&self, right: impl Into<Arc<Self>>) -> Self {
        assert_eq!(self.children.len(), 2);
        Self {
            plan: self.plan.clone(),
            state: K::rewritten_state(&self.state),
            children: vec![self.children[0].clone(), right.into()],
        }
    }

    pub fn replace_side_child(&self, side: Side, child: impl Into<Arc<Self>>) -> Self {
        match side {
            Side::Left => self.replace_left_child(child),
            Side::Right => self.replace_right_child(child),
        }
    }

    pub fn replace_plan(&self, plan: impl Into<Arc<K::Operator>>) -> Self {
        Self {
            plan: plan.into(),
            state: K::rewritten_state(&self.state),
            children: self.children.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::ir::AsyncExprVisitor;
    use crate::optimizer::ir::ExprVisitor;
    use crate::optimizer::ir::PExpr;
    use crate::optimizer::ir::SExpr;
    use crate::optimizer::ir::StatContext;
    use crate::optimizer::ir::VisitAction;
    use crate::optimizer::optimizers::rule::RuleID;
    use crate::plans::DummyTableScan;
    use crate::plans::Join;
    use crate::plans::Limit;
    use crate::plans::RelOp;

    struct ReplaceDummy<K: ExprKind>(Expr<K>);
    impl<K: ExprKind> ExprVisitor<K> for ReplaceDummy<K> {
        fn visit(&mut self, expr: &Expr<K>) -> Result<VisitAction<K>> {
            Ok(if expr.plan().rel_op() == RelOp::DummyTableScan {
                VisitAction::Replace(self.0.clone())
            } else {
                VisitAction::Continue
            })
        }
    }
    #[async_trait::async_trait]
    impl<K: ExprKind> AsyncExprVisitor<K> for ReplaceDummy<K> {
        async fn visit(&mut self, expr: &Expr<K>) -> Result<VisitAction<K>> {
            ExprVisitor::visit(self, expr)
        }
    }

    async fn check_traversal<K: ExprKind>(tree: &Expr<K>, empty: Expr<K>) -> Result<Expr<K>> {
        let stats = StatContext::without_settings(Default::default());
        tree.derive_relational_prop()?;
        assert_eq!(tree.derive_cardinality(&stats)?.cardinality, 1.0);
        let replaced = tree.accept(&mut ReplaceDummy(empty.clone()))?.unwrap();
        assert!(K::relational_cache(&replaced.state).get().is_none());
        assert!(K::statistics_cache(&replaced.state).get().is_none());
        assert_eq!(replaced.derive_cardinality(&stats)?.cardinality, 0.0);
        let asynchronous = tree.accept_async(&mut ReplaceDummy(empty)).await?.unwrap();
        assert!(replaced == asynchronous);
        assert_eq!(tree.derive_cardinality(&stats)?.cardinality, 1.0);
        assert_eq!(tree.unary_child().plan().rel_op(), RelOp::DummyTableScan);
        Ok(replaced)
    }

    #[tokio::test]
    async fn logical_and_physical_traversal_reset_cached_state() -> Result<()> {
        let limit = Limit {
            before_exchange: false,
            limit: Some(0),
            offset: 0,
            lazy_columns: Default::default(),
        };
        let parent = Limit {
            limit: None,
            ..limit.clone()
        };
        let mut logical = SExpr::create(
            parent.clone(),
            vec![Arc::new(SExpr::create_leaf(DummyTableScan::new()))],
            Some(7),
            None,
            None,
        );
        let mut physical = PExpr::create(
            parent,
            vec![Arc::new(PExpr::create_leaf(DummyTableScan::new()))],
            Some(7),
            None,
            None,
        );
        logical.set_applied_rule(&RuleID::EliminateEvalScalar);
        physical.set_applied_rule(&RuleID::EliminateEvalScalar);
        let rewritten = check_traversal(
            &logical,
            SExpr::create_unary(limit.clone(), SExpr::create_leaf(DummyTableScan::new())),
        )
        .await?;
        assert_eq!(rewritten.original_group(), None);
        assert!(rewritten.applied_rule(&RuleID::EliminateEvalScalar));
        assert_eq!(logical.original_group(), Some(7));
        let rewritten = check_traversal(
            &physical,
            PExpr::create_unary(limit, PExpr::create_leaf(DummyTableScan::new())),
        )
        .await?;
        assert_eq!(rewritten.original_group(), None);
        assert!(rewritten.applied_rule(&RuleID::EliminateEvalScalar));
        assert_eq!(physical.original_group(), Some(7));
        Ok(())
    }

    fn check_binary<K: ExprKind>(tree: Expr<K>) {
        let left = tree.left_child_arc();
        let right = tree.right_child_arc();
        let replacement = Arc::new(left.replace_plan(left.plan.clone()));
        let replaced = tree.replace_left_child(replacement.clone());
        assert!(Arc::ptr_eq(&replaced.left_child_arc(), &replacement));
        assert!(Arc::ptr_eq(&replaced.right_child_arc(), &right));
        let replaced = tree.replace_side_child(Side::Right, replacement.clone());
        assert!(Arc::ptr_eq(&replaced.left_child_arc(), &left));
        assert!(Arc::ptr_eq(&replaced.right_child_arc(), &replacement));
        assert!(tree.child(2).is_err());
        assert!(tree == tree.clone());
    }

    #[test]
    fn binary_rewrites_preserve_the_other_child() {
        check_binary(SExpr::create_binary(
            Join::default(),
            SExpr::create_leaf(DummyTableScan::new()),
            SExpr::create_leaf(DummyTableScan::new()),
        ));
        check_binary(PExpr::create_binary(
            Join::default(),
            PExpr::create_leaf(DummyTableScan::new()),
            PExpr::create_leaf(DummyTableScan::new()),
        ));
    }
}
