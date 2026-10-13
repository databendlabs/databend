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
use std::sync::OnceLock;

use educe::Educe;

use crate::IndexType;
use crate::optimizer::ir::Expr;
use crate::optimizer::ir::ExprKind;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelExprKind;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RewriteExprKind;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::RelOperator;

/// Physical expression, with stage-specific state on a shared recursive tree.
pub type PExpr = Expr<Physical>;

pub struct Physical;

impl ExprKind for Physical {
    type Operator = RelOperator;
    type State = PhysicalState;
}

#[derive(Educe)]
#[educe(PartialEq, Eq, Hash, Clone)]
pub struct PhysicalState {
    pub(crate) original_group: Option<IndexType>,
    /// Shared, lazily populated caches; excluded from expression identity.
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) rel_prop: Arc<OnceLock<Arc<RelationalProperty>>>,
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) stat_info: Arc<OnceLock<Arc<StatInfo>>>,
    pub(crate) applied_rules: AppliedRules,
}

impl RewriteExprKind for Physical {
    fn rewritten_state(state: &Self::State) -> Self::State {
        Self::State {
            original_group: None,
            rel_prop: Default::default(),
            stat_info: Default::default(),
            applied_rules: state.applied_rules.clone(),
        }
    }
}

impl RelExprKind for Physical {
    const NAME: &'static str = "PExpr";
    fn rel_expr(expr: &Expr<Self>) -> RelExpr<'_> {
        RelExpr::with_p_expr(expr)
    }
    fn relational_cache(state: &Self::State) -> &Arc<OnceLock<Arc<RelationalProperty>>> {
        &state.rel_prop
    }
    fn statistics_cache(state: &Self::State) -> &Arc<OnceLock<Arc<StatInfo>>> {
        &state.stat_info
    }
}

// Preserve diagnostic output rather than exposing the implementation's state wrapper.
impl std::fmt::Debug for Expr<Physical> {
    #[recursive::recursive]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PExpr")
            .field("plan", &self.plan)
            .field("children", &self.children)
            .field("original_group", &self.state.original_group)
            .field("rel_prop", &self.state.rel_prop)
            .field("stat_info", &self.state.stat_info)
            .field("applied_rules", &self.state.applied_rules)
            .finish()
    }
}

impl PExpr {
    pub fn create(
        plan: impl Into<Arc<RelOperator>>,
        children: Vec<Arc<PExpr>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        PExpr {
            plan: plan.into(),
            children,
            state: PhysicalState {
                original_group,
                rel_prop: Arc::new(match rel_prop {
                    Some(rel_prop) => OnceLock::from(rel_prop),
                    None => OnceLock::new(),
                }),
                stat_info: Arc::new(match stat_info {
                    Some(stat_info) => OnceLock::from(stat_info),
                    None => OnceLock::new(),
                }),
                applied_rules: AppliedRules::default(),
            },
        }
    }

    pub fn create_unary(plan: impl Into<Arc<RelOperator>>, child: impl Into<Arc<PExpr>>) -> Self {
        Self::create(plan.into(), vec![child.into()], None, None, None)
    }

    pub fn create_binary(
        plan: impl Into<Arc<RelOperator>>,
        left_child: impl Into<Arc<PExpr>>,
        right_child: impl Into<Arc<PExpr>>,
    ) -> Self {
        Self::create(
            plan,
            vec![left_child.into(), right_child.into()],
            None,
            None,
            None,
        )
    }

    pub fn create_leaf(plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![], None, None, None)
    }

    pub fn build_unary(self, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![self.into()], None, None, None)
    }

    pub fn ref_build_unary(self: &Arc<PExpr>, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![self.clone()], None, None, None)
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.state.original_group
    }

    /// Record the applied rule id in current PExpr
    pub(crate) fn set_applied_rule(&mut self, rule_id: &RuleID) {
        self.state.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current PExpr
    pub(crate) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.state.applied_rules.get(rule_id)
    }

    // The method will clear the applied rules of current PExpr and its children.
    #[recursive::recursive]
    pub fn clear_applied_rules(&mut self) {
        self.state.applied_rules.clear();
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
}
