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

mod implement;
mod rule_list;

use common_exception::Result;

use crate::sql::optimizer::heuristic::implement::HeuristicImplementor;
use crate::sql::optimizer::heuristic::rule_list::RuleList;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::SExpr;

/// A heuristic query optimizer. It will apply specific transformation rules in order and
/// implement the logical plans with default implementation rules.
pub struct HeuristicOptimizer {
    rules: RuleList,
    implementor: HeuristicImplementor,
}

impl HeuristicOptimizer {
    pub fn create() -> Result<Self> {
        Ok(HeuristicOptimizer {
            rules: RuleList::create(vec![])?,
            implementor: HeuristicImplementor::new(),
        })
    }

    pub fn optimize(&mut self, expression: SExpr) -> Result<SExpr> {
        let result = self.optimize_expression(&expression)?;
        Ok(result)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(self.optimize_expression(expr)?);
        }
        let optimized_expr = SExpr::create(s_expr.plan().clone(), optimized_children, None);
        let result = self.apply_transform_rules(&optimized_expr, &self.rules)?;

        Ok(result)
    }

    fn apply_transform_rules(&self, s_expr: &SExpr, rule_list: &RuleList) -> Result<SExpr> {
        let mut result = s_expr.clone();

        for rule in rule_list.iter() {
            let mut state = TransformState::new();
            rule.apply(&result, &mut state)?;
            if !state.results().is_empty() {
                result = state.results()[0].clone();
            }
        }

        // Implement expression with Implementor
        let mut state = TransformState::new();
        self.implementor.implement(s_expr, &mut state)?;
        if !state.results().is_empty() {
            result = state.results()[0].clone();
        }

        Ok(result)
    }
}
