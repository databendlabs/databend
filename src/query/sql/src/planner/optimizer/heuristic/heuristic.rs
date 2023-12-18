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
use std::sync::LazyLock;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;

use crate::optimizer::heuristic::decorrelate::decorrelate_subquery;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::MetadataRef;

pub static DEFAULT_REWRITE_RULES: LazyLock<Vec<RuleID>> = LazyLock::new(|| {
    vec![
        RuleID::NormalizeDisjunctiveFilter,
        RuleID::NormalizeScalarFilter,
        RuleID::NormalizeAggregate,
        RuleID::EliminateFilter,
        RuleID::MergeFilter,
        RuleID::InferFilter,
        RuleID::MergeEvalScalar,
        RuleID::PushDownFilterUnion,
        RuleID::PushDownFilterAggregate,
        RuleID::PushDownLimitUnion,
        RuleID::PushDownLimitExpression,
        RuleID::PushDownLimitSort,
        RuleID::PushDownLimitAggregate,
        RuleID::PushDownLimitOuterJoin,
        RuleID::PushDownLimitScan,
        RuleID::PushDownFilterSort,
        RuleID::PushDownFilterEvalScalar,
        RuleID::PushDownFilterJoin,
        RuleID::PushDownFilterProjectSet,
        RuleID::FoldCountAggregate,
        RuleID::TryApplyAggIndex,
        RuleID::SplitAggregate,
        RuleID::PushDownFilterScan,
        RuleID::PushDownPrewhere, /* PushDownPrwhere should be after all rules except PushDownFilterScan */
        RuleID::PushDownSortScan, // PushDownSortScan should be after PushDownPrewhere
    ]
});

pub static RESIDUAL_RULES: LazyLock<Vec<RuleID>> =
    LazyLock::new(|| vec![RuleID::EliminateEvalScalar, RuleID::CommuteJoin]);

/// A heuristic query optimizer. It will apply specific transformation rules in order and
/// implement the logical plans with default implementation rules.
pub struct HeuristicOptimizer {
    _func_ctx: FunctionContext,
    metadata: MetadataRef,
}

impl HeuristicOptimizer {
    pub fn new(func_ctx: FunctionContext, metadata: MetadataRef) -> Self {
        HeuristicOptimizer {
            _func_ctx: func_ctx,
            metadata,
        }
    }

    pub fn pre_optimize(&self, s_expr: SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr;
        if s_expr.contain_subquery() {
            s_expr = decorrelate_subquery(self.metadata.clone(), s_expr)?;
        }
        Ok(s_expr)
    }

    pub fn optimize(&self, s_expr: SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let pre_optimized = self.pre_optimize(s_expr)?;
        self.optimize_expression(&pre_optimized, rules)
    }

    pub fn optimize_expression(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(Arc::new(self.optimize_expression(expr, rules)?));
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_transform_rules(&optimized_expr, rules)?;

        Ok(result)
    }

    /// Try to apply the rules to the expression.
    /// Return the final result that no rule can be applied.
    fn apply_transform_rules(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();

        for rule_id in rules {
            let rule = RuleFactory::create_rule(*rule_id, self.metadata.clone())?;
            let mut state = TransformResult::new();
            if rule
                .patterns()
                .iter()
                .any(|pattern| s_expr.match_pattern(pattern))
                && !s_expr.applied_rule(&rule.id())
            {
                s_expr.set_applied_rule(&rule.id());
                rule.apply(&s_expr, &mut state)?;
                if !state.results().is_empty() {
                    // Recursive optimize the result
                    let result = &state.results()[0];
                    let optimized_result = self.optimize_expression(result, rules)?;
                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}
