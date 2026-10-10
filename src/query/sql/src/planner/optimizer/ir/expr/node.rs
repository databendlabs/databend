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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use crate::IndexType;
use crate::Symbol;
use crate::plans::Exchange;
use crate::plans::Operator;
use crate::plans::RelOperator;

/// Stage-specific operator and state types for a shared recursive expression.
/// Constructors and invalidation remain stage-owned; relational utilities can be
/// shared by opting into `RelExprKind`.
pub trait ExprKind {
    type Operator: Clone + Debug + Eq + Hash;
    type State: Clone + Eq + Hash;
}

/// Opt-in support for rewriting the recursive shape; invalidation stays stage-owned.
pub trait RewriteExprKind: ExprKind {
    fn rewritten_state(state: &Self::State) -> Self::State;
}

/// Shared relational utilities apply only while a stage uses RelOperator. A future
/// physical operator can provide its own utilities without changing the tree skeleton.
pub trait RelExprKind: RewriteExprKind<Operator = crate::plans::RelOperator> + Sized {
    const NAME: &'static str;
    fn rel_expr(expr: &Expr<Self>) -> crate::optimizer::ir::RelExpr<'_>;
    fn relational_cache(
        state: &Self::State,
    ) -> &Arc<std::sync::OnceLock<Arc<crate::optimizer::ir::RelationalProperty>>>;
    fn statistics_cache(
        state: &Self::State,
    ) -> &Arc<std::sync::OnceLock<Arc<crate::optimizer::ir::StatInfo>>>;
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
}

impl<K: RewriteExprKind> Expr<K> {
    pub fn replace_children(&self, children: impl IntoIterator<Item = Arc<Self>>) -> Self {
        Self {
            plan: self.plan.clone(),
            children: children.into_iter().collect(),
            state: K::rewritten_state(&self.state),
        }
    }
}

impl<K: RelExprKind> Expr<K> {
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

impl<K: RelExprKind> Expr<K> {
    pub fn build_side_child(&self) -> &Expr<K> {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[1]
    }

    pub fn probe_side_child(&self) -> &Expr<K> {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[0]
    }

    #[recursive::recursive]
    pub fn support_lazy_materialize(&self) -> bool {
        self.plan.support_lazy_materialize()
            && self
                .children
                .iter()
                .all(|child| child.support_lazy_materialize())
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

    #[recursive::recursive]
    pub fn get_udfs_col_ids(&self) -> Result<BTreeSet<Symbol>> {
        let mut udf_ids = BTreeSet::new();
        if let RelOperator::Udf(udf) = self.plan.as_ref() {
            for item in udf.items.iter() {
                udf_ids.insert(item.index);
            }
        }
        for child in &self.children {
            let udfs = child.get_udfs_col_ids()?;
            udf_ids.extend(udfs);
        }
        Ok(udf_ids)
    }

    // Add column index to Scan nodes that match the given table index
    pub fn add_column_index_to_scans(&self, table_index: IndexType, column_index: Symbol) -> Self {
        let mut required_columns = BTreeMap::new();
        required_columns.insert(table_index, ScanRequiredColumns {
            columns: BTreeSet::from([column_index]),
            inverted_index: None,
            vector_index: None,
        });
        self.add_column_indexes_to_scans(&required_columns)
    }

    // Add column indexes to Scan nodes that match the given table indexes.
    pub fn add_column_indexes_to_scans(
        &self,
        required_columns: &BTreeMap<IndexType, ScanRequiredColumns>,
    ) -> Self {
        struct Visitor<'a> {
            required_columns: &'a BTreeMap<IndexType, ScanRequiredColumns>,
        }

        impl<K: RelExprKind> super::visitor::ExprVisitor<K> for Visitor<'_> {
            fn visit(&mut self, expr: &Expr<K>) -> Result<super::visitor::VisitAction<K>> {
                if let Some(p) = expr.plan.as_ref().as_scan() {
                    if let Some(required_columns) = self.required_columns.get(&p.table_index) {
                        let mut p = p.clone();
                        p.columns.extend(required_columns.columns.iter().copied());
                        if required_columns.inverted_index.is_some() {
                            p.inverted_index = required_columns.inverted_index.clone();
                        }
                        if required_columns.vector_index.is_some() {
                            p.vector_index = required_columns.vector_index.clone();
                        }
                        let expr = expr.replace_plan(p);
                        return Ok(super::visitor::VisitAction::Replace(expr));
                    } else {
                        return Ok(super::visitor::VisitAction::SkipChildren);
                    }
                }
                Ok(super::visitor::VisitAction::Continue)
            }
        }

        let mut visitor = Visitor { required_columns };
        let expr = self.accept(&mut visitor);
        if let Ok(Some(expr)) = expr {
            return expr;
        }
        self.clone()
    }

    #[recursive::recursive]
    pub fn has_merge_exchange(&self) -> bool {
        if let RelOperator::Exchange(Exchange::Merge) = self.plan.as_ref() {
            return true;
        }
        self.children.iter().any(|child| child.has_merge_exchange())
    }

    pub fn get_data_distribution(&self) -> Result<Option<Exchange>> {
        struct DataDistributionVisitor {
            result: Option<Exchange>,
        }
        impl<K: RelExprKind> super::visitor::ExprVisitor<K> for DataDistributionVisitor {
            fn visit(&mut self, expr: &Expr<K>) -> Result<super::visitor::VisitAction<K>> {
                match expr.plan.as_ref() {
                    RelOperator::Exchange(exchange) => {
                        self.result = Some(exchange.clone());
                        Ok(super::visitor::VisitAction::Stop)
                    }

                    RelOperator::Join(_) => {
                        let child = expr.probe_side_child();
                        self.result = child.get_data_distribution()?;
                        Ok(super::visitor::VisitAction::Stop)
                    }
                    _ => {
                        if expr.arity() > 0 {
                            Ok(super::visitor::VisitAction::Continue)
                        } else {
                            Ok(super::visitor::VisitAction::Stop)
                        }
                    }
                }
            }
        }

        let mut visitor = DataDistributionVisitor { result: None };
        let _ = self.accept(&mut visitor);
        Ok(visitor.result)
    }
}

impl<K: RewriteExprKind<Operator = crate::plans::RelOperator>> Expr<K> {
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

    pub fn replace_plan(&self, plan: impl Into<Arc<RelOperator>>) -> Self {
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

    // The skeleton does not require the physical stage to retain logical cache/rule
    // fields, or even the same operator type.
    struct IndependentStage;

    impl ExprKind for IndependentStage {
        type Operator = &'static str;
        type State = u64;
    }

    impl RewriteExprKind for IndependentStage {
        fn rewritten_state(_: &Self::State) -> Self::State {
            99
        }
    }

    struct ReplaceScan;

    impl crate::optimizer::ir::ExprVisitor<IndependentStage> for ReplaceScan {
        fn visit(
            &mut self,
            expr: &Expr<IndependentStage>,
        ) -> Result<crate::optimizer::ir::VisitAction<IndependentStage>> {
            use crate::optimizer::ir::VisitAction;
            if *expr.plan() == "scan" {
                Ok(VisitAction::Replace(Expr {
                    plan: Arc::new("new_scan"),
                    children: vec![],
                    state: 7,
                }))
            } else {
                Ok(VisitAction::Continue)
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::optimizer::ir::AsyncExprVisitor<IndependentStage> for ReplaceScan {
        async fn visit(
            &mut self,
            expr: &Expr<IndependentStage>,
        ) -> Result<crate::optimizer::ir::VisitAction<IndependentStage>> {
            crate::optimizer::ir::ExprVisitor::visit(self, expr)
        }
    }

    #[tokio::test]
    async fn shared_traversal_uses_stage_invalidation() -> Result<()> {
        let tree = Expr::<IndependentStage> {
            plan: Arc::new("filter"),
            children: vec![Arc::new(Expr {
                plan: Arc::new("scan"),
                children: vec![],
                state: 11,
            })],
            state: 23,
        };
        let replaced = tree.accept(&mut ReplaceScan)?.unwrap();
        assert_eq!(replaced.state, 99);
        assert_eq!(*replaced.child(0)?.plan(), "new_scan");
        assert_eq!(replaced.child(0)?.state, 7);
        let asynchronous = tree.accept_async(&mut ReplaceScan).await?.unwrap();
        assert!(replaced == asynchronous);
        assert_eq!(tree.state, 23);
        assert_eq!(*tree.child(0)?.plan(), "scan");
        Ok(())
    }

    #[test]
    fn tree_access_does_not_depend_on_stage_state() -> Result<()> {
        let leaf = Arc::new(Expr::<IndependentStage> {
            plan: Arc::new("scan"),
            children: vec![],
            state: 11,
        });
        let tree = Expr::<IndependentStage> {
            plan: Arc::new("filter"),
            children: vec![leaf.clone()],
            state: 23,
        };
        assert_eq!(*tree.plan(), "filter");
        assert_eq!(tree.arity(), 1);
        assert_eq!(tree.child(0)?.state, 11);
        assert!(tree.child(1).is_err());
        assert!(Arc::ptr_eq(&leaf, &tree.unary_child_arc()));
        let cloned = tree.clone();
        assert!(tree == cloned);
        assert!(Arc::ptr_eq(&tree.children[0], &cloned.children[0]));
        Ok(())
    }
}
