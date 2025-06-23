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
use crate::plans::RelOperator;
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
        plan: Arc<RelOperator>,
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

    pub fn create_unary(plan: Arc<RelOperator>, child: impl Into<Arc<SExpr>>) -> Self {
        Self::create(plan, [child.into()], None, None, None)
    }

    pub fn create_binary(
        plan: Arc<RelOperator>,
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

    pub fn create_leaf(plan: Arc<RelOperator>) -> Self {
        Self::create(plan, [], None, None, None)
    }

    pub fn build_unary(self, plan: Arc<RelOperator>) -> Self {
        debug_assert_eq!(plan.arity(), 1);
        Self::create(plan, [self.into()], None, None, None)
    }

    pub fn ref_build_unary(self: &Arc<SExpr>, plan: Arc<RelOperator>) -> Self {
        debug_assert_eq!(plan.arity(), 1);
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

    pub fn replace_plan(&self, plan: Arc<RelOperator>) -> Self {
        Self {
            plan,
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

    //
    #[recursive::recursive]
    pub fn get_udfs(&self) -> Result<HashSet<&String>> {
        let mut udfs = HashSet::new();

        match self.plan.as_ref() {
            RelOperator::Scan(scan) => {
                let Scan {
                    push_down_predicates,
                    prewhere,
                    agg_index,
                    ..
                } = scan;

                if let Some(push_down_predicates) = push_down_predicates {
                    for push_down_predicate in push_down_predicates {
                        push_down_predicate.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
                if let Some(prewhere) = prewhere {
                    for predicate in &prewhere.predicates {
                        predicate.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
                if let Some(agg_index) = agg_index {
                    for predicate in &agg_index.predicates {
                        predicate.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                    for selection in &agg_index.selection {
                        selection.scalar.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
            }
            RelOperator::Exchange(exchange) => {
                if let Exchange::Hash(hash) = exchange {
                    for hash in hash {
                        hash.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                }
            }
            RelOperator::Join(op) => {
                for equi_condition in op.equi_conditions.iter() {
                    equi_condition.left.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                    equi_condition
                        .right
                        .get_udf_names()?
                        .iter()
                        .for_each(|udf| {
                            udfs.insert(*udf);
                        });
                }
                for non in &op.non_equi_conditions {
                    non.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::EvalScalar(op) => {
                for item in &op.items {
                    item.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Filter(op) => {
                for predicate in &op.predicates {
                    predicate.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Aggregate(op) => {
                for group_items in &op.group_items {
                    group_items.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
                for agg_func in &op.aggregate_functions {
                    agg_func.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Window(op) => {
                match &op.function {
                    WindowFuncType::Aggregate(agg) => {
                        for arg in agg.exprs() {
                            arg.get_udf_names()?.iter().for_each(|udf| {
                                udfs.insert(*udf);
                            });
                        }
                    }
                    WindowFuncType::LagLead(lag_lead) => {
                        // udfs_pad(&mut udfs, f, &lag_lead.arg)?;
                        lag_lead.arg.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                        if let Some(default) = &lag_lead.default {
                            default.get_udf_names()?.iter().for_each(|udf| {
                                udfs.insert(*udf);
                            });
                        }
                    }
                    WindowFuncType::NthValue(nth) => {
                        nth.arg.get_udf_names()?.iter().for_each(|udf| {
                            udfs.insert(*udf);
                        });
                    }
                    _ => {}
                }
                for arg in &op.arguments {
                    arg.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
                for order_by in &op.order_by {
                    order_by
                        .order_by_item
                        .scalar
                        .get_udf_names()?
                        .iter()
                        .for_each(|udf| {
                            udfs.insert(*udf);
                        });
                }
                for partition_by in &op.partition_by {
                    partition_by.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::ProjectSet(op) => {
                for srf in &op.srfs {
                    srf.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Udf(udf) => {
                for item in &udf.items {
                    item.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::AsyncFunction(async_func) => {
                for item in &async_func.items {
                    item.scalar.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::MutationSource(mutation_source) => {
                for predicate in &mutation_source.predicates {
                    predicate.get_udf_names()?.iter().for_each(|udf| {
                        udfs.insert(*udf);
                    });
                }
            }
            RelOperator::Limit(_)
            | RelOperator::UnionAll(_)
            | RelOperator::Sort(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::Mutation(_)
            | RelOperator::MaterializedCTE(_)
            | RelOperator::CTEConsumer(_)
            | RelOperator::CompactBlock(_) => {}
        };
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
            s_expr.plan = if let RelOperator::Scan(mut p) = (*s_expr.plan).clone() {
                if p.table_index == table_index {
                    p.columns.insert(column_index);
                    if inverted_index.is_some() {
                        p.inverted_index = inverted_index.clone();
                    }
                }
                Arc::new(p.into())
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
}
