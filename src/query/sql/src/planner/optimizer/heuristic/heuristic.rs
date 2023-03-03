// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::prune_unused_columns::UnusedColumnPruner;
use crate::optimizer::heuristic::decorrelate::decorrelate_subquery;
use crate::optimizer::heuristic::prewhere_optimization::PrewhereOptimizer;
use crate::optimizer::rule::RulePtr;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::optimizer::RULE_FACTORY;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::BindContext;
use crate::MetadataRef;

pub static DEFAULT_REWRITE_RULES: Lazy<Vec<RuleID>> = Lazy::new(|| {
    vec![
        RuleID::NormalizeDisjunctiveFilter,
        RuleID::NormalizeScalarFilter,
        RuleID::EliminateFilter,
        RuleID::EliminateEvalScalar,
        RuleID::MergeFilter,
        RuleID::MergeEvalScalar,
        RuleID::PushDownFilterUnion,
        RuleID::PushDownFilterAggregate,
        RuleID::PushDownLimitUnion,
        RuleID::RulePushDownLimitExpression,
        RuleID::PushDownLimitSort,
        RuleID::PushDownLimitAggregate,
        RuleID::PushDownLimitOuterJoin,
        RuleID::PushDownLimitScan,
        RuleID::PushDownFilterSort,
        RuleID::PushDownFilterEvalScalar,
        RuleID::PushDownFilterJoin,
        RuleID::FoldCountAggregate,
        RuleID::SplitAggregate,
        RuleID::PushDownFilterScan,
        RuleID::PushDownSortScan,
    ]
});

/// A heuristic query optimizer. It will apply specific transformation rules in order and
/// implement the logical plans with default implementation rules.
pub struct HeuristicOptimizer {
    _ctx: Arc<dyn TableContext>,
    bind_context: Box<BindContext>,
    metadata: MetadataRef,
}

impl HeuristicOptimizer {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        bind_context: Box<BindContext>,
        metadata: MetadataRef,
    ) -> Self {
        HeuristicOptimizer {
            _ctx: ctx,
            bind_context,
            metadata,
        }
    }

    fn pre_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr;
        if s_expr.contain_subquery() {
            s_expr = decorrelate_subquery(self.metadata.clone(), s_expr)?;
        }

        // always pruner the unused columns before and after optimization
        let pruner = UnusedColumnPruner::new(self.metadata.clone());
        let require_columns: ColumnSet =
            self.bind_context.columns.iter().map(|c| c.index).collect();
        pruner.remove_unused_columns(&s_expr, require_columns)
    }

    fn post_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let prewhere_optimizer = PrewhereOptimizer::new(self.metadata.clone());
        let s_expr = prewhere_optimizer.prewhere_optimize(s_expr)?;

        let pruner = UnusedColumnPruner::new(self.metadata.clone());
        let require_columns: ColumnSet =
            self.bind_context.columns.iter().map(|c| c.index).collect();
        pruner.remove_unused_columns(&s_expr, require_columns)
    }

    pub fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let pre_optimized = self.pre_optimize(s_expr)?;
        let optimized = self.optimize_expression(&pre_optimized)?;
        let post_optimized = self.post_optimize(optimized)?;

        // do it again, some rules may be missed after the post_optimized
        // for example: push down sort + limit (topn) to scan
        // TODO: if we push down the filter to scan, we need to remove the filter plan
        let optimized = self.optimize_expression(&post_optimized)?;
        let post_optimized = self.post_optimize(optimized)?;

        Ok(post_optimized)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(self.optimize_expression(expr)?);
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_transform_rules(&optimized_expr)?;

        Ok(result)
    }

    fn calc_operator_rule_set(&self, operator: &RelOperator) -> roaring::RoaringBitmap {
        unsafe { operator.transformation_candidate_rules() & (&RULE_FACTORY.transformation_rules) }
    }

    fn get_rule(&self, rule_id: u32) -> Result<RulePtr> {
        unsafe {
            RULE_FACTORY.create_rule(
                DEFAULT_REWRITE_RULES[rule_id as usize],
                Some(self.metadata.clone()),
            )
        }
    }

    /// Try to apply the rules to the expression.
    /// Return the final result that no rule can be applied.
    fn apply_transform_rules(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        let rule_set = self.calc_operator_rule_set(&s_expr.plan);

        for rule_id in rule_set.iter() {
            let rule = self.get_rule(rule_id)?;
            let mut state = TransformResult::new();
            if s_expr.match_pattern(rule.pattern()) && !s_expr.applied_rule(&rule.id()) {
                s_expr.set_applied_rule(&rule.id());
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
