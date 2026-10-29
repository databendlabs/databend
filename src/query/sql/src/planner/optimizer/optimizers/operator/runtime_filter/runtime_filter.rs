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

use databend_common_exception::Result;

use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::RuleFactory;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::RulePtr;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::JoinRuntimeFilter;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScanRuntimeFilter;
use crate::plans::as_runtime_filter_predicate;
use crate::plans::runtime_filter_predicate;

/// Filter push-down rules that move runtime filter predicates towards scans.
const PUSH_DOWN_RULES: [RuleID; 8] = [
    RuleID::PushDownFilterJoin,
    RuleID::PushDownFilterEvalScalar,
    RuleID::PushDownFilterUnion,
    RuleID::PushDownFilterAggregate,
    RuleID::PushDownFilterWindow,
    RuleID::PushDownFilterSort,
    RuleID::PushDownFilterProjectSet,
    RuleID::PushDownFilterUdf,
];

/// Plan join runtime filters with the filter push-down rules.
///
/// For every eligible equi condition of a join, this pass records a
/// [`JoinRuntimeFilter`] on the join and places the predicate
/// `__runtime_filter(id)(probe_key)` above the probe child. The ordinary
/// filter push-down rules then move the predicate down, rewriting it through
/// projections, unions, aggregates, windows and join equivalences. A predicate
/// that reaches a `Scan` becomes a [`ScanRuntimeFilter`] of that scan. A
/// predicate that cannot be pushed further is removed: runtime filters only
/// act at scans.
///
/// Joins are processed top-down. Pushing a predicate through a lower join can
/// rewrite that join's conditions, so a join creates its own runtime filters
/// only after the filters of the joins above it are placed.
///
/// The physical hash join decides which filters are actually built.
pub struct RuntimeFilterOptimizer {
    opt_ctx: Arc<OptimizerContext>,
}

impl RuntimeFilterOptimizer {
    pub const NAME: &'static str = "RuntimeFilterOptimizer";

    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self { opt_ctx }
    }

    pub fn optimize_sync(&self, s_expr: SExpr) -> Result<SExpr> {
        let mut rules = Vec::with_capacity(PUSH_DOWN_RULES.len());
        for rule_id in PUSH_DOWN_RULES {
            let rule = RuleFactory::create_rule(rule_id, self.opt_ctx.clone())?;
            if !self.opt_ctx.is_optimizer_disabled(&rule.name()) {
                rules.push(rule);
            }
        }
        RuntimeFilterPlanner {
            opt_ctx: &self.opt_ctx,
            rules,
        }
        .optimize_expr(s_expr)
    }
}

struct RuntimeFilterPlanner<'a> {
    opt_ctx: &'a OptimizerContext,
    rules: Vec<RulePtr>,
}

impl RuntimeFilterPlanner<'_> {
    #[recursive::recursive]
    fn optimize_expr(&self, s_expr: SExpr) -> Result<SExpr> {
        let s_expr = self.add_join_runtime_filters(s_expr)?;
        let mut children = Vec::with_capacity(s_expr.arity());
        for child in s_expr.children() {
            children.push(Arc::new(self.optimize_expr(child.clone())?));
        }
        Ok(s_expr.replace_children(children))
    }

    fn add_join_runtime_filters(&self, s_expr: SExpr) -> Result<SExpr> {
        let RelOperator::Join(join) = s_expr.plan() else {
            return Ok(s_expr);
        };
        if !supported_join_type(&join.join_type) || !join.runtime_filters.is_empty() {
            return Ok(s_expr);
        }

        let mut join = join.clone();
        let mut predicates = vec![];
        for condition in join.equi_conditions.iter() {
            // A runtime filter drops NULL probe keys, which a null-safe equality can match.
            if condition.is_null_equal {
                continue;
            }
            let (probe_key, build_key) = condition.canonical_keys();
            if probe_key.used_columns().len() != 1 {
                continue;
            }
            let id = self
                .opt_ctx
                .get_metadata()
                .write()
                .next_join_runtime_filter_key();
            join.runtime_filters.push(JoinRuntimeFilter {
                id,
                probe_key: probe_key.clone(),
                build_key: build_key.clone(),
            });
            predicates.push(runtime_filter_predicate(id, probe_key.clone()));
        }
        if predicates.is_empty() {
            return Ok(s_expr);
        }

        let probe_side = self.push_down(predicates, s_expr.child(0)?.clone())?;
        let build_side = s_expr.child(1)?.clone();
        Ok(s_expr
            .replace_plan(Arc::new(RelOperator::Join(join)))
            .replace_children([Arc::new(probe_side), Arc::new(build_side)]))
    }

    /// Push runtime filter `predicates` into `child` and return the rewritten child.
    #[recursive::recursive]
    fn push_down(&self, predicates: Vec<ScalarExpr>, child: SExpr) -> Result<SExpr> {
        match child.plan() {
            RelOperator::Scan(scan) => {
                let mut scan = scan.clone();
                for predicate in predicates.iter() {
                    if let Some((id, probe_key)) = as_runtime_filter_predicate(predicate) {
                        scan.runtime_filters.push(ScanRuntimeFilter {
                            id,
                            probe_key: probe_key.clone(),
                        });
                    }
                }
                return Ok(child.replace_plan(Arc::new(RelOperator::Scan(scan))));
            }
            // Runtime filters commute with an ordinary filter.
            RelOperator::Filter(_) => {
                let input = self.push_down(predicates, child.child(0)?.clone())?;
                return Ok(child.replace_children([Arc::new(input)]));
            }
            _ => {}
        }

        let input = SExpr::create_unary(
            Arc::new(RelOperator::Filter(Filter { predicates })),
            Arc::new(child.clone()),
        );
        for rule in self.rules.iter() {
            for (index, matcher) in rule.matchers().iter().enumerate() {
                if !matcher.matches(&input) {
                    continue;
                }
                let mut state = TransformResult::new();
                rule.apply_matcher(index, &input, &mut state)?;
                return match state.results().first() {
                    Some(result) => self.continue_push_down(result.clone()),
                    // The rule cannot move the predicates below this operator.
                    None => Ok(child),
                };
            }
        }
        // No rule moves a filter below this operator.
        Ok(child)
    }

    /// Continue pushing the runtime filters that a rule placed below an operator.
    ///
    /// A push-down rule leaves the predicates it cannot move in a filter at the
    /// root of its result; those runtime filters are removed. Predicates moved
    /// below the operator are in filters on its direct children.
    fn continue_push_down(&self, result: SExpr) -> Result<SExpr> {
        let (remaining, operator) = match result.plan() {
            RelOperator::Filter(filter) => {
                let (_, others) = split_runtime_filters(&filter.predicates);
                (others, result.child(0)?.clone())
            }
            _ => (vec![], result),
        };

        let mut children = Vec::with_capacity(operator.arity());
        for child in operator.children() {
            let child = match child.plan() {
                RelOperator::Filter(filter) => {
                    let (runtime_filters, others) = split_runtime_filters(&filter.predicates);
                    if runtime_filters.is_empty() {
                        child.clone()
                    } else {
                        let input = self.push_down(runtime_filters, child.child(0)?.clone())?;
                        wrap_filter(others, input)
                    }
                }
                _ => child.clone(),
            };
            children.push(Arc::new(child));
        }
        Ok(wrap_filter(remaining, operator.replace_children(children)))
    }
}

/// Join types whose probe side may drop rows that have no match on the build side.
fn supported_join_type(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::LeftSemi
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}

fn split_runtime_filters(predicates: &[ScalarExpr]) -> (Vec<ScalarExpr>, Vec<ScalarExpr>) {
    predicates
        .iter()
        .cloned()
        .partition(|predicate| as_runtime_filter_predicate(predicate).is_some())
}

fn wrap_filter(predicates: Vec<ScalarExpr>, input: SExpr) -> SExpr {
    if predicates.is_empty() {
        return input;
    }
    SExpr::create_unary(
        Arc::new(RelOperator::Filter(Filter { predicates })),
        Arc::new(input),
    )
}

#[async_trait::async_trait]
impl Optimizer for RuntimeFilterOptimizer {
    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
