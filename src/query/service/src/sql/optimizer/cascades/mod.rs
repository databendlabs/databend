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

mod explore_rules;
mod implement_rules;

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;

use super::cost::Cost;
use super::cost::CostContext;
use super::cost::CostModel;
use super::cost::DefaultCostModel;
use crate::sql::optimizer::cascades::explore_rules::get_explore_rule_set;
use crate::sql::optimizer::cascades::implement_rules::get_implement_rule_set;
use crate::sql::optimizer::format::display_memo;
use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::memo::Memo;
use crate::sql::optimizer::rule::RuleSet;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Operator;
use crate::sql::IndexType;

/// A cascades-style search engine to enumerate possible alternations of a relational expression and
/// find the optimal one.
pub struct CascadesOptimizer {
    memo: Memo,
    explore_rules: RuleSet,
    implement_rules: RuleSet,

    cost_model: Box<dyn CostModel>,

    /// group index -> best cost context
    best_cost_map: HashMap<IndexType, CostContext>,
}

impl CascadesOptimizer {
    pub fn create() -> Self {
        CascadesOptimizer {
            memo: Memo::create(),
            explore_rules: get_explore_rule_set(),
            implement_rules: get_implement_rule_set(),
            cost_model: Box::new(DefaultCostModel),
            best_cost_map: HashMap::new(),
        }
    }

    fn init(&mut self, expression: SExpr) -> Result<()> {
        self.memo.init(expression)?;

        Ok(())
    }

    pub fn optimize(mut self, s_expr: SExpr) -> Result<SExpr> {
        self.init(s_expr)?;

        let root_index = self
            .memo
            .root()
            .ok_or_else(|| {
                ErrorCode::LogicalError("Root group cannot be None after initialization")
            })?
            .group_index;

        self.explore_group(root_index)?;

        self.implement_group(root_index)?;

        self.optimize_group(root_index)?;

        tracing::debug!("Memo: \n{}", display_memo(&self.memo));

        self.find_optimal_plan(root_index)
    }

    fn explore_group(&mut self, group_index: IndexType) -> Result<()> {
        let group = self.memo.group(group_index)?;
        for m_expr in group.m_exprs.clone() {
            self.explore_expr(&m_expr)?;
        }

        Ok(())
    }

    fn explore_expr(&mut self, m_expr: &MExpr) -> Result<()> {
        for child in m_expr.children.iter() {
            self.explore_group(*child)?;
        }

        let mut state = TransformState::new();
        for rule in self.explore_rules.iter() {
            m_expr.apply_rule(&self.memo, rule, &mut state)?;
        }
        self.insert_from_transform_state(m_expr.group_index, state)?;

        Ok(())
    }

    fn implement_group(&mut self, group_index: IndexType) -> Result<()> {
        let group = self.memo.group(group_index)?;
        for m_expr in group.m_exprs.clone() {
            self.implement_expr(&m_expr)?;
        }

        Ok(())
    }

    fn implement_expr(&mut self, m_expr: &MExpr) -> Result<()> {
        for child in m_expr.children.iter() {
            self.implement_group(*child)?;
        }

        let mut state = TransformState::new();
        for rule in self.implement_rules.iter() {
            m_expr.apply_rule(&self.memo, rule, &mut state)?;
        }
        self.insert_from_transform_state(m_expr.group_index, state)?;

        Ok(())
    }

    fn insert_from_transform_state(
        &mut self,
        group_index: IndexType,
        state: TransformState,
    ) -> Result<()> {
        for result in state.results() {
            self.insert_expression(group_index, result)?;
        }

        Ok(())
    }

    fn insert_expression(&mut self, group_index: IndexType, expression: &SExpr) -> Result<()> {
        self.memo.insert(Some(group_index), expression.clone())?;

        Ok(())
    }

    fn find_optimal_plan(&self, group_index: IndexType) -> Result<SExpr> {
        let group = self.memo.group(group_index)?;
        let cost_context = self.best_cost_map.get(&group_index).ok_or_else(|| {
            ErrorCode::LogicalError(format!("Cannot find CostContext of group: {group_index}"))
        })?;

        let m_expr = group.m_exprs.get(cost_context.expr_index).ok_or_else(|| {
            ErrorCode::LogicalError(format!(
                "Cannot find best expression of group: {group_index}"
            ))
        })?;

        let children = m_expr
            .children
            .iter()
            .map(|index| self.find_optimal_plan(*index))
            .collect::<Result<Vec<_>>>()?;

        let result = SExpr::create(m_expr.plan.clone(), children, None);

        Ok(result)
    }

    fn optimize_group(&mut self, group_index: IndexType) -> Result<()> {
        let group = self.memo.group(group_index)?.clone();
        for m_expr in group.m_exprs.iter() {
            if m_expr.plan.is_physical() {
                self.optimize_m_expr(m_expr)?;
            }
        }

        Ok(())
    }

    fn optimize_m_expr(&mut self, m_expr: &MExpr) -> Result<()> {
        let mut cost = Cost::from(0);
        for child in m_expr.children.iter() {
            self.optimize_group(*child)?;
            let cost_context = self.best_cost_map.get(child).ok_or_else(|| {
                ErrorCode::LogicalError(format!("Cannot find CostContext of group: {child}"))
            })?;

            cost = cost + cost_context.cost;
        }

        let op_cost = self.cost_model.compute_cost(&self.memo, m_expr)?;
        cost = cost + op_cost;

        let cost_context = CostContext {
            cost,
            group_index: m_expr.group_index,
            expr_index: m_expr.index,
        };

        match self.best_cost_map.entry(m_expr.group_index) {
            Entry::Vacant(entry) => {
                entry.insert(cost_context);
            }
            Entry::Occupied(mut entry) => {
                // Replace the cost context of the group if current context is lower
                if cost < entry.get().cost {
                    entry.insert(cost_context);
                }
            }
        }

        Ok(())
    }
}
