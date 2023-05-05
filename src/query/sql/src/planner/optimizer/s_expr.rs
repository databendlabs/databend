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
use std::sync::Mutex;

use common_exception::ErrorCode;
use common_exception::Result;
use educe::Educe;

use super::RelationalProperty;
use crate::optimizer::rule::AppliedRules;
use crate::optimizer::rule::RuleID;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::WindowFuncType;
use crate::IndexType;
use crate::ScalarExpr;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct SExpr {
    pub(crate) plan: RelOperator,
    pub(crate) children: Arc<Vec<SExpr>>,

    pub(crate) original_group: Option<IndexType>,

    /// A cache of relational property of current `SExpr`, will
    /// be lazily computed as soon as `RelExpr::derive_relational_prop`
    /// is invoked on current `SExpr`.
    ///
    /// Since `SExpr` is `Send + Sync`, we use `Mutex` to protect
    /// the cache.
    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub(crate) rel_prop: Arc<Mutex<Option<RelationalProperty>>>,

    #[educe(Hash(ignore), PartialEq(ignore), Eq(ignore))]
    pub(crate) stat_info: Arc<Mutex<Option<StatInfo>>>,

    /// A bitmap to record applied rules on current SExpr, to prevent
    /// redundant transformations.
    pub(crate) applied_rules: AppliedRules,
}

impl SExpr {
    pub fn create(
        plan: RelOperator,
        children: Vec<SExpr>,
        original_group: Option<IndexType>,
        rel_prop: Option<RelationalProperty>,
        stat_info: Option<StatInfo>,
    ) -> Self {
        SExpr {
            plan,
            children: Arc::new(children),
            original_group,
            rel_prop: Arc::new(Mutex::new(rel_prop)),
            stat_info: Arc::new(Mutex::new(stat_info)),
            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary(plan: RelOperator, child: SExpr) -> Self {
        Self::create(plan, vec![child], None, None, None)
    }

    pub fn create_binary(plan: RelOperator, left_child: SExpr, right_child: SExpr) -> Self {
        Self::create(plan, vec![left_child, right_child], None, None, None)
    }

    pub fn create_leaf(plan: RelOperator) -> Self {
        Self::create(plan, vec![], None, None, None)
    }

    pub fn create_pattern_leaf() -> Self {
        Self::create(
            PatternPlan {
                plan_type: RelOp::Pattern,
            }
            .into(),
            vec![],
            None,
            None,
            None,
        )
    }

    pub fn plan(&self) -> &RelOperator {
        &self.plan
    }

    pub fn children(&self) -> &[SExpr] {
        &self.children
    }

    pub fn child(&self, n: usize) -> Result<&SExpr> {
        self.children
            .get(n)
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid children index: {}", n)))
    }

    pub fn arity(&self) -> usize {
        self.children.len()
    }

    pub fn is_pattern(&self) -> bool {
        matches!(self.plan.rel_op(), RelOp::Pattern)
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.original_group
    }

    pub fn match_pattern(&self, pattern: &SExpr) -> bool {
        if pattern.plan.rel_op() != RelOp::Pattern {
            // Pattern is plan
            if self.plan.rel_op() != pattern.plan.rel_op() {
                return false;
            }

            if self.arity() != pattern.arity() {
                // Check if current expression has same arity with current pattern
                return false;
            }

            for (e, p) in self.children.iter().zip(pattern.children.iter()) {
                // Check children
                if !e.match_pattern(p) {
                    return false;
                }
            }
        };

        true
    }

    /// Replace children with given new `children`.
    /// Note that this method will keep the `applied_rules` of
    /// current `SExpr` unchanged.
    pub fn replace_children(&self, children: Vec<SExpr>) -> Self {
        Self {
            plan: self.plan.clone(),
            original_group: None,
            rel_prop: Arc::new(Mutex::new(None)),
            stat_info: Arc::new(Mutex::new(None)),
            applied_rules: self.applied_rules.clone(),
            children: Arc::new(children),
        }
    }

    pub fn replace_plan(&self, plan: RelOperator) -> Self {
        Self {
            plan,
            original_group: self.original_group,
            rel_prop: self.rel_prop.clone(),
            stat_info: self.stat_info.clone(),
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
    pub(crate) fn contain_subquery(&self) -> bool {
        if !find_subquery(&self.plan) {
            return self.children.iter().any(|child| child.contain_subquery());
        }
        true
    }

    // Add (table_index, column_index) into `Scan` node recursively.
    pub fn add_internal_column_index(
        expr: &SExpr,
        table_index: IndexType,
        column_index: IndexType,
    ) -> SExpr {
        fn add_internal_column_index_into_child(
            s_expr: &SExpr,
            column_index: IndexType,
            table_index: IndexType,
        ) -> SExpr {
            let mut s_expr = s_expr.clone();
            if let RelOperator::Scan(p) = &mut s_expr.plan {
                if p.table_index == table_index {
                    p.columns.insert(column_index);
                }
            }

            if s_expr.children.is_empty() {
                s_expr
            } else {
                let mut children = Vec::with_capacity(s_expr.children.len());
                for child in s_expr.children.as_ref() {
                    children.push(add_internal_column_index_into_child(
                        child,
                        column_index,
                        table_index,
                    ));
                }

                s_expr.children = Arc::new(children);

                s_expr
            }
        }

        add_internal_column_index_into_child(expr, column_index, table_index)
    }
}

fn find_subquery(rel_op: &RelOperator) -> bool {
    match rel_op {
        RelOperator::Scan(_)
        | RelOperator::Limit(_)
        | RelOperator::Exchange(_)
        | RelOperator::UnionAll(_)
        | RelOperator::Sort(_)
        | RelOperator::DummyTableScan(_)
        | RelOperator::RuntimeFilterSource(_)
        | RelOperator::Pattern(_) => false,
        RelOperator::Join(op) => {
            op.left_conditions.iter().any(find_subquery_in_expr)
                || op.right_conditions.iter().any(find_subquery_in_expr)
                || op.non_equi_conditions.iter().any(find_subquery_in_expr)
        }
        RelOperator::EvalScalar(op) => op
            .items
            .iter()
            .any(|expr| find_subquery_in_expr(&expr.scalar)),
        RelOperator::Filter(op) => op.predicates.iter().any(find_subquery_in_expr),
        RelOperator::Aggregate(op) => {
            op.group_items
                .iter()
                .any(|expr| find_subquery_in_expr(&expr.scalar))
                || op
                    .aggregate_functions
                    .iter()
                    .any(|expr| find_subquery_in_expr(&expr.scalar))
        }
        RelOperator::Window(op) => {
            op.order_by
                .iter()
                .any(|o| find_subquery_in_expr(&o.order_by_item.scalar))
                || op
                    .partition_by
                    .iter()
                    .any(|expr| find_subquery_in_expr(&expr.scalar))
                || match &op.function {
                    WindowFuncType::Aggregate(agg) => agg.args.iter().any(find_subquery_in_expr),
                    _ => false,
                }
        }
        RelOperator::ProjectSet(op) => op
            .srfs
            .iter()
            .any(|expr| find_subquery_in_expr(&expr.scalar)),
    }
}

fn find_subquery_in_expr(expr: &ScalarExpr) -> bool {
    match expr {
        ScalarExpr::BoundColumnRef(_) | ScalarExpr::ConstantExpr(_) => false,
        ScalarExpr::WindowFunction(expr) => {
            let flag = match &expr.func {
                WindowFuncType::Aggregate(agg) => agg.args.iter().any(find_subquery_in_expr),
                _ => false,
            };
            flag || expr.partition_by.iter().any(find_subquery_in_expr)
                || expr.order_by.iter().any(|o| find_subquery_in_expr(&o.expr))
        }
        ScalarExpr::AggregateFunction(expr) => expr.args.iter().any(find_subquery_in_expr),
        ScalarExpr::FunctionCall(expr) => expr.arguments.iter().any(find_subquery_in_expr),
        ScalarExpr::CastExpr(expr) => find_subquery_in_expr(&expr.argument),
        ScalarExpr::SubqueryExpr(_) => true,
    }
}
