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
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;

use crate::optimizer::ir::property::RelExpr;
use crate::optimizer::ir::property::RelationalProperty;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::VisitAction;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::Exchange;
use crate::plans::Operator;
use crate::plans::RelOperator;
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
    pub plan: Arc<RelOperator>,
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
    pub fn create(
        plan: impl Into<Arc<RelOperator>>,
        children: impl Into<Vec<Arc<SExpr>>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        let plan = plan.into();
        let children = children.into();
        SExpr {
            plan,
            children,
            original_group,
            rel_prop: Arc::new(Mutex::new(rel_prop)),
            stat_info: Arc::new(Mutex::new(stat_info)),
            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary(plan: impl Into<Arc<RelOperator>>, child: impl Into<Arc<SExpr>>) -> Self {
        Self::create(plan.into(), [child.into()], None, None, None)
    }

    pub fn create_binary(
        plan: impl Into<Arc<RelOperator>>,
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

    pub fn create_leaf(plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, [], None, None, None)
    }

    pub fn build_unary(self, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, [self.into()], None, None, None)
    }

    pub fn ref_build_unary(self: &Arc<SExpr>, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, [self.clone()], None, None, None)
    }

    pub fn plan(&self) -> &RelOperator {
        &self.plan
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
        debug_assert_eq!(self.children.len(), 1);
        &self.children[0]
    }

    pub fn left_child(&self) -> &SExpr {
        debug_assert_eq!(self.children.len(), 2);
        &self.children[0]
    }

    pub fn right_child(&self) -> &SExpr {
        debug_assert_eq!(self.children.len(), 2);
        &self.children[1]
    }

    pub fn build_side_child(&self) -> &SExpr {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
        &self.children[1]
    }

    pub fn probe_side_child(&self) -> &SExpr {
        debug_assert_eq!(self.plan.rel_op(), crate::plans::RelOp::Join);
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

    pub fn replace_plan(&self, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self {
            plan: plan.into(),
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

    // Add column index to Scan nodes that match the given table index
    pub fn add_column_index_to_scans(
        &self,
        table_index: IndexType,
        column_index: IndexType,
        inverted_index: &Option<InvertedIndexInfo>,
        vector_index: &Option<VectorIndexInfo>,
    ) -> SExpr {
        struct Visitor<'a> {
            table_index: IndexType,
            column_index: IndexType,
            inverted_index: &'a Option<InvertedIndexInfo>,
            vector_index: &'a Option<VectorIndexInfo>,
        }

        impl<'a> SExprVisitor for Visitor<'a> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if let Some(p) = expr.plan.as_ref().as_scan() {
                    if p.table_index == self.table_index {
                        let mut p = p.clone();
                        p.columns.insert(self.column_index);
                        if self.inverted_index.is_some() {
                            p.inverted_index = self.inverted_index.clone();
                        }
                        if self.vector_index.is_some() {
                            p.vector_index = self.vector_index.clone();
                        }
                        let expr = expr.replace_plan(p);
                        return Ok(VisitAction::Replace(expr));
                    } else {
                        return Ok(VisitAction::SkipChildren);
                    }
                }
                Ok(VisitAction::Continue)
            }
        }

        let mut visitor = Visitor {
            table_index,
            column_index,
            inverted_index,
            vector_index,
        };
        let expr = self.accept(&mut visitor);
        if let Ok(Some(expr)) = expr {
            return expr;
        }
        self.clone()
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
        if let RelOperator::Exchange(Exchange::Merge) = self.plan.as_ref() {
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
        struct DataDistributionVisitor {
            result: Option<Exchange>,
        }
        impl SExprVisitor for DataDistributionVisitor {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                match expr.plan.as_ref() {
                    RelOperator::Exchange(exchange) => {
                        self.result = Some(exchange.clone());
                        Ok(VisitAction::Stop)
                    }

                    RelOperator::Join(_) => {
                        let child = expr.probe_side_child();
                        self.result = child.get_data_distribution()?;
                        Ok(VisitAction::Stop)
                    }
                    _ => {
                        if expr.arity() > 0 {
                            Ok(VisitAction::Continue)
                        } else {
                            Ok(VisitAction::Stop)
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
