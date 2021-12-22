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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::cascades::explore_rules::get_explore_rule_set;
use crate::sql::optimizer::cascades::implement_rules::get_implement_rule_set;
use crate::sql::optimizer::group::Group;
use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::memo::Memo;
use crate::sql::optimizer::optimize_context::OptimizeContext;
use crate::sql::optimizer::rule::RulePtr;
use crate::sql::optimizer::rule::RuleSet;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::IndexType;

/// A cascades-style search engine to enumerate possible alternations of a relational expression and
/// find the optimal one.
///
/// NOTICE: we don't support cost-based optimization and lower bound searching for now.
pub struct CascadesOptimizer {
    optimize_context: OptimizeContext,
    memo: Memo,
    explore_rules: RuleSet,
    implement_rules: RuleSet,
}

impl CascadesOptimizer {
    pub fn create(optimize_context: OptimizeContext) -> Self {
        CascadesOptimizer {
            optimize_context,
            memo: Memo::create(),
            explore_rules: get_explore_rule_set(),
            implement_rules: get_implement_rule_set(),
        }
    }

    fn init(&mut self, expression: SExpr) -> Result<()> {
        self.memo.init(expression)?;

        Ok(())
    }

    pub fn optimize(&mut self, expression: SExpr) -> Result<SExpr> {
        self.init(expression)?;

        self.explore_group(self.memo.root().unwrap().group_index())?;

        self.implement_group(self.memo.root().unwrap().group_index())?;

        self.find_optimal_plan()
    }

    fn explore_group(&mut self, group_index: IndexType) -> Result<()> {
        let group = self.memo.group(group_index);
        let expressions: Vec<MExpr> = group.iter().cloned().collect();
        for m_expr in expressions {
            self.explore_expr(m_expr)?;
        }

        Ok(())
    }

    fn explore_expr(&mut self, m_expr: MExpr) -> Result<()> {
        for child in m_expr.children() {
            self.explore_group(*child)?;
        }

        let mut state = TransformState::create();
        for rule in self.explore_rules.iter() {
            m_expr.apply_rule(&self.memo, rule, &mut state)?;
        }
        self.insert_from_transform_state(m_expr.group_index(), state)?;

        Ok(())
    }

    fn implement_group(&mut self, group_index: IndexType) -> Result<()> {
        let group = self.memo.group(group_index);
        let expressions: Vec<MExpr> = group.iter().cloned().collect();
        for m_expr in expressions {
            self.implement_expr(m_expr)?;
        }

        Ok(())
    }

    fn implement_expr(&mut self, m_expr: MExpr) -> Result<()> {
        for child in m_expr.children() {
            self.implement_group(*child)?;
        }

        let mut state = TransformState::create();
        for rule in self.implement_rules.iter() {
            m_expr.apply_rule(&self.memo, rule, &mut state)?;
        }
        self.insert_from_transform_state(m_expr.group_index(), state)?;

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

    #[allow(dead_code)]
    fn apply_rule(&self, m_expr: &MExpr, rule: &RulePtr, state: &mut TransformState) -> Result<()> {
        m_expr.apply_rule(&self.memo, rule, state)?;

        Ok(())
    }

    fn find_optimal_plan(&self) -> Result<SExpr> {
        println!("{:?}", self.memo);

        let root_group = self.memo.root().unwrap();

        let required_prop = self.optimize_context.required_prop().clone();

        self.optimize_group(root_group, &required_prop)
    }

    /// We don't have cost mechanism for evaluate cost of plans, so we just extract
    /// first physical plan in a group that satisfies given RequiredProperty.
    fn optimize_group(&self, group: &Group, required_prop: &RequiredProperty) -> Result<SExpr> {
        for m_expr in group.iter() {
            if m_expr.plan().is_physical() {
                let plan = m_expr.plan().clone();

                // Chek properties
                let physical = plan.as_physical_plan().unwrap();
                let relational_prop = group.relational_prop().unwrap();
                let dummy_physical_prop = PhysicalProperty::default();
                let required_prop = physical.compute_required_prop(required_prop);

                if !required_prop.provided_by(relational_prop, &dummy_physical_prop) {
                    continue;
                }

                let children = self.optimize_m_expr(m_expr, &required_prop)?;
                let result = SExpr::create(plan, children, None);
                return Ok(result);
            }
        }

        Err(ErrorCode::LogicalError("Cannot find an appropriate plan"))
    }

    fn optimize_m_expr(
        &self,
        m_expr: &MExpr,
        required_prop: &RequiredProperty,
    ) -> Result<Vec<SExpr>> {
        let mut children = vec![];
        for child in m_expr.children() {
            let group = self.memo.group(*child);
            children.push(self.optimize_group(group, required_prop)?);
        }

        Ok(children)
    }
}
