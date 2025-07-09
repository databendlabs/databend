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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use crate::optimizer::ir::property::RelExpr;
use crate::optimizer::ir::property::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::Exchange;
use crate::plans::Operator;
use crate::plans::OperatorRef;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::plans::WindowFuncType;
use crate::IndexType;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Educe)]
#[educe(
    PartialEq(bound = false, attrs = "#[recursive::recursive]"),
    Eq,
    Hash(bound = false, attrs = "#[recursive::recursive]"),
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
pub struct SExpr {
    pub plan: OperatorRef,
    pub(crate) children: Vec<Arc<SExpr>>,

    pub(crate) original_group: Option<IndexType>,

    /// A cache of relational property of current `SExpr`, will
    /// be lazily computed as soon as `RelExpr::derive_relational_prop`
    /// is invoked on current `SExpr`.
    ///
    /// Since `SExpr` is `Send + Sync`, we use `Mutex` to protect
    /// the cache.
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) rel_prop: Arc<Mutex<Option<Arc<RelationalProperty>>>>,

    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) stat_info: Arc<Mutex<Option<Arc<StatInfo>>>>,

    /// A bitmap to record applied rules on current SExpr, to prevent
    /// redundant transformations.
    pub(crate) applied_rules: AppliedRules,
}

impl SExpr {
    pub fn create<P: Operator>(
        plan: P,
        children: impl Into<Vec<Arc<SExpr>>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        let plan = Arc::new(plan) as OperatorRef;
        SExpr {
            plan,
            children: children.into(),
            original_group,
            rel_prop: Arc::new(Mutex::new(rel_prop)),
            stat_info: Arc::new(Mutex::new(stat_info)),
            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_with_plan(
        plan: OperatorRef,
        children: impl Into<Vec<Arc<SExpr>>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        SExpr {
            plan,
            children: children.into(),
            original_group,
            rel_prop: Arc::new(Mutex::new(rel_prop)),
            stat_info: Arc::new(Mutex::new(stat_info)),
            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary<P: Operator>(plan: P, child: impl Into<Arc<SExpr>>) -> Self {
        Self::create(plan, [child.into()], None, None, None)
    }

    pub fn create_binary<P: Operator>(
        plan: P,
        left_child: impl Into<Arc<SExpr>>,
        right_child: impl Into<Arc<SExpr>>,
    ) -> Self {
        Self::create(
            plan,
            [left_child.into(), right_child.into()],
            None,
            None,
            None,
        )
    }

    pub fn create_leaf<P: Operator>(plan: P) -> Self {
        Self::create(plan, [], None, None, None)
    }

    pub fn build_unary<P: Operator>(self, plan: P) -> Self {
        debug_assert_eq!(plan.arity(), 1);
        Self::create_unary(plan, self)
    }

    pub fn ref_build_unary<P: Operator>(self: &Arc<SExpr>, plan: P) -> Self {
        debug_assert_eq!(plan.arity(), 1);
        Self::create_unary(plan, self.clone())
    }

    pub fn plan(&self) -> &OperatorRef {
        &self.plan
    }

    pub fn plan_rel_op(&self) -> RelOp {
        self.plan.rel_op()
    }

    pub fn children(&self) -> impl Iterator<Item = &SExpr> {
        self.children.iter().map(|v| v.as_ref())
    }

    pub fn child(&self, n: usize) -> Result<&SExpr> {
        self.children
            .get(n)
            .map(|v| v.as_ref())
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid children index: {}", n)))
    }

    pub fn unary_child(&self) -> &SExpr {
        assert_eq!(self.children.len(), 1);
        &self.children[0]
    }

    pub fn left_child(&self) -> &SExpr {
        assert_eq!(self.children.len(), 2);
        &self.children[0]
    }

    pub fn right_child(&self) -> &SExpr {
        assert_eq!(self.children.len(), 2);
        &self.children[1]
    }

    pub fn build_side_child(&self) -> &SExpr {
        assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[1]
    }

    pub fn probe_side_child(&self) -> &SExpr {
        assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[0]
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.original_group
    }

    /// Replace children with given new `children`.
    /// Note that this method will keep the `applied_rules` of
    /// current `SExpr` unchanged.
    pub fn replace_children(&self, children: impl IntoIterator<Item = Arc<SExpr>>) -> Self {
        Self {
            plan: self.plan.clone(),
            original_group: None,
            rel_prop: Arc::new(Mutex::new(None)),
            stat_info: Arc::new(Mutex::new(None)),
            applied_rules: self.applied_rules.clone(),
            children: children.into_iter().collect(),
        }
    }

    pub fn replace_plan<P: Operator>(&self, plan: P) -> Self {
        Self {
            plan: Arc::new(plan) as OperatorRef,
            original_group: None,
            rel_prop: Arc::new(Mutex::new(None)),
            stat_info: Arc::new(Mutex::new(None)),
            applied_rules: self.applied_rules.clone(),
            children: self.children.clone(),
        }
    }

    /// Record the applied rule id in current SExpr
    pub(crate) fn set_applied_rule(&mut self, rule_id: &RuleID) {
        self.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current SExpr
    pub(crate) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.applied_rules.get(rule_id)
    }

    /// Check if contain subquery
    #[recursive::recursive]
    pub(crate) fn has_subquery(&self) -> bool {
        self.plan.has_subquery() || self.children.iter().any(|child| child.has_subquery())
    }

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

    // Add column index to Scan nodes that match the given table index
    pub fn add_column_index_to_scans(
        &self,
        table_index: IndexType,
        column_index: IndexType,
        inverted_index: &Option<InvertedIndexInfo>,
    ) -> SExpr {
        #[recursive::recursive]
        fn add_column_index_to_scans_recursive(
            s_expr: &SExpr,
            column_index: IndexType,
            table_index: IndexType,
            inverted_index: &Option<InvertedIndexInfo>,
        ) -> SExpr {
            let mut s_expr = s_expr.clone();
            s_expr.plan = if let Some(mut p) = s_expr.plan().as_any().downcast_mut::<Scan>() {
                if p.table_index == table_index {
                    p.columns.insert(column_index);
                    if inverted_index.is_some() {
                        p.inverted_index = inverted_index.clone();
                    }
                }
                Arc::new(p.clone())
            } else {
                s_expr.plan
            };

            if s_expr.children.is_empty() {
                s_expr
            } else {
                let mut children = Vec::with_capacity(s_expr.children.len());
                for child in s_expr.children.iter() {
                    children.push(Arc::new(add_column_index_to_scans_recursive(
                        child,
                        column_index,
                        table_index,
                        inverted_index,
                    )));
                }

                s_expr.children = children;

                s_expr
            }
        }

        add_column_index_to_scans_recursive(self, column_index, table_index, inverted_index)
    }

    // The method will clear the applied rules of current SExpr and its children.
    #[recursive::recursive]
    pub fn clear_applied_rules(&mut self) {
        self.applied_rules.clear();
        let children = self
            .children()
            .map(|child| {
                let mut child = child.clone();
                child.clear_applied_rules();
                Arc::new(child)
            })
            .collect::<Vec<_>>();
        self.children = children;
    }

    #[recursive::recursive]
    pub fn has_merge_exchange(&self) -> bool {
        if let Some(Exchange::Merge) = self.plan.as_any().downcast_ref::<Exchange>() {
            return true;
        }
        self.children.iter().any(|child| child.has_merge_exchange())
    }

    pub fn derive_relational_prop(&self) -> Result<Arc<RelationalProperty>> {
        if let Some(rel_prop) = self.rel_prop.lock().unwrap().as_ref() {
            return Ok(rel_prop.clone());
        }
        let rel_prop = self
            .plan
            .derive_relational_prop(&RelExpr::SExpr { expr: self })?;
        *self.rel_prop.lock().unwrap() = Some(rel_prop.clone());
        Ok(rel_prop)
    }

    pub fn get_data_distribution(&self) -> Result<Option<Exchange>> {
        match self.plan.rel_op() {
            crate::plans::RelOp::Scan
            | crate::plans::RelOp::DummyTableScan
            | crate::plans::RelOp::ConstantTableScan
            | crate::plans::RelOp::ExpressionScan
            | crate::plans::RelOp::CacheScan
            | crate::plans::RelOp::RecursiveCteScan => Ok(None),

            crate::plans::RelOp::Join => self.probe_side_child().get_data_distribution(),

            crate::plans::RelOp::Exchange => {
                let exchange = self.plan.as_any().downcast_ref::<Exchange>().unwrap();
                Ok(Some(exchange.clone()))
            }

            crate::plans::RelOp::EvalScalar
            | crate::plans::RelOp::Filter
            | crate::plans::RelOp::Aggregate
            | crate::plans::RelOp::Sort
            | crate::plans::RelOp::Limit
            | crate::plans::RelOp::UnionAll
            | crate::plans::RelOp::Window
            | crate::plans::RelOp::ProjectSet
            | crate::plans::RelOp::Udf
            | crate::plans::RelOp::AsyncFunction
            | crate::plans::RelOp::Mutation
            | crate::plans::RelOp::CompactBlock
            | crate::plans::RelOp::MutationSource => self.child(0)?.get_data_distribution(),
        }
    }
}
