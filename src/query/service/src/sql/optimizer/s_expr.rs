// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::rule::AppliedRules;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::plans::Operator;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::RelOperator;
use crate::sql::IndexType;

/// `SExpr` is abbreviation of single expression, which is a tree of relational operators.
#[derive(Clone, Debug)]
pub struct SExpr {
    pub(super) plan: RelOperator,
    pub(super) children: Vec<SExpr>,

    pub(super) original_group: Option<IndexType>,

    /// A bitmap to record applied rules on current SExpr, to prevent
    /// redundant transformations.
    pub(super) applied_rules: AppliedRules,
}

impl SExpr {
    pub fn create(
        plan: RelOperator,
        children: Vec<SExpr>,
        original_group: Option<IndexType>,
    ) -> Self {
        SExpr {
            plan,
            children,
            original_group,

            applied_rules: AppliedRules::default(),
        }
    }

    pub fn create_unary(plan: RelOperator, child: SExpr) -> Self {
        Self::create(plan, vec![child], None)
    }

    pub fn create_binary(plan: RelOperator, left_child: SExpr, right_child: SExpr) -> Self {
        Self::create(plan, vec![left_child, right_child], None)
    }

    pub fn create_leaf(plan: RelOperator) -> Self {
        Self::create(plan, vec![], None)
    }

    pub fn create_pattern_leaf() -> Self {
        Self::create(
            PatternPlan {
                plan_type: RelOp::Pattern,
            }
            .into(),
            vec![],
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
            .ok_or_else(|| ErrorCode::LogicalError(format!("Invalid children index: {}", n)))
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

    pub fn replace_children(&self, children: Vec<SExpr>) -> Self {
        Self {
            plan: self.plan.clone(),
            original_group: self.original_group,
            applied_rules: self.applied_rules.clone(),
            children,
        }
    }

    /// Record the applied rule id in current SExpr
    pub(super) fn apply_rule(&mut self, rule_id: &RuleID) {
        self.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current SExpr
    pub(super) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.applied_rules.get(rule_id)
    }
}
