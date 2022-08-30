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

mod decorrelate;
mod implement;
mod prune_columns;
mod rule_list;
mod subquery_rewriter;
mod where_optimizer;

use std::sync::Arc;

use common_exception::Result;
use once_cell::sync::Lazy;

use super::rule::RuleID;
use super::ColumnSet;
use crate::sessions::QueryContext;
use crate::sql::optimizer::heuristic::decorrelate::decorrelate_subquery;
use crate::sql::optimizer::heuristic::implement::HeuristicImplementor;
pub use crate::sql::optimizer::heuristic::rule_list::RuleList;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

pub static DEFAULT_REWRITE_RULES: Lazy<Vec<RuleID>> = Lazy::new(|| {
    vec![
        RuleID::NormalizeDisjunctiveFilter,
        RuleID::NormalizeScalarFilter,
        RuleID::EliminateFilter,
        RuleID::EliminateEvalScalar,
        RuleID::EliminateProject,
        RuleID::MergeFilter,
        RuleID::MergeEvalScalar,
        RuleID::MergeProject,
        RuleID::PushDownLimitProject,
        RuleID::PushDownLimitSort,
        RuleID::PushDownLimitOuterJoin,
        RuleID::PushDownLimitScan,
        RuleID::PushDownSortScan,
        RuleID::PushDownFilterEvalScalar,
        RuleID::PushDownFilterProject,
        RuleID::PushDownFilterJoin,
        RuleID::SplitAggregate,
        RuleID::PushDownFilterScan,
    ]
});

/// A heuristic query optimizer. It will apply specific transformation rules in order and
/// implement the logical plans with default implementation rules.
pub struct HeuristicOptimizer {
    rules: RuleList,
    implementor: HeuristicImplementor,

    _ctx: Arc<QueryContext>,
    bind_context: Box<BindContext>,
    metadata: MetadataRef,
}

impl HeuristicOptimizer {
    pub fn new(
        ctx: Arc<QueryContext>,
        bind_context: Box<BindContext>,
        metadata: MetadataRef,
        rules: RuleList,
    ) -> Self {
        HeuristicOptimizer {
            rules,
            implementor: HeuristicImplementor::new(),

            _ctx: ctx,
            bind_context,
            metadata,
        }
    }

    fn pre_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let result = decorrelate_subquery(self.metadata.clone(), s_expr)?;
        Ok(result)
    }

    fn post_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let pruner = prune_columns::ColumnPruner::new(self.metadata.clone());
        let require_columns: ColumnSet =
            self.bind_context.columns.iter().map(|c| c.index).collect();
        let s_expr = pruner.prune_columns(&s_expr, require_columns)?;

        let where_opt = where_optimizer::WhereOptimizer::new(self.metadata.clone());
        where_opt.where_optimize(s_expr)
    }

    pub fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let pre_optimized = self.pre_optimize(s_expr)?;
        let optimized = self.optimize_expression(&pre_optimized)?;
        let post_optimized = self.post_optimize(optimized)?;
        // let mut result = self.implement_expression(&post_optimized)?;

        Ok(post_optimized)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(self.optimize_expression(expr)?);
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_transform_rules(&optimized_expr, &self.rules)?;

        Ok(result)
    }

    #[allow(dead_code)]
    fn implement_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut implemented_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            implemented_children.push(self.implement_expression(expr)?);
        }
        let implemented_expr = SExpr::create(s_expr.plan().clone(), implemented_children, None);
        // Implement expression with Implementor
        let mut state = TransformState::new();
        self.implementor.implement(&implemented_expr, &mut state)?;
        let result = if !state.results().is_empty() {
            state.results()[0].clone()
        } else {
            implemented_expr
        };
        Ok(result)
    }

    // Return `None` if no rules matched
    fn apply_transform_rules(&self, s_expr: &SExpr, rule_list: &RuleList) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        for rule in rule_list.iter() {
            let mut state = TransformState::new();
            if s_expr.match_pattern(rule.pattern()) && !s_expr.applied_rule(&rule.id()) {
                s_expr.apply_rule(&rule.id());
                rule.apply(&s_expr, &mut state)?;
                if !state.results().is_empty() {
                    // Recursive optimize the result
                    let result = &state.results()[0];
                    let optimized_result = self.optimize_expression(result)?;

                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}
