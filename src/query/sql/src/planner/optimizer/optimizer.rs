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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;
use log::info;

use super::cost::CostContext;
use super::distributed::MergeSourceOptimizer;
use super::format::display_memo;
use super::Memo;
use crate::binder::MergeIntoType;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::decorrelate::decorrelate_subquery;
use crate::optimizer::distributed::optimize_distributed_query;
use crate::optimizer::hyper_dp::DPhpy;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::util::contains_local_table_scan;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::optimizer::DEFAULT_REWRITE_RULES;
use crate::optimizer::RESIDUAL_RULES;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Exchange;
use crate::plans::Join;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OptimizerContext {
    #[educe(Debug(ignore))]
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,

    // Optimizer configurations
    enable_distributed_optimization: bool,
    enable_join_reorder: bool,
    enable_dphyp: bool,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: false,
            enable_join_reorder: true,
            enable_dphyp: true,
        }
    }

    pub fn with_enable_distributed_optimization(mut self, enable: bool) -> Self {
        self.enable_distributed_optimization = enable;
        self
    }

    pub fn with_enable_join_reorder(mut self, enable: bool) -> Self {
        self.enable_join_reorder = enable;
        self
    }

    pub fn with_enable_dphyp(mut self, enable: bool) -> Self {
        self.enable_dphyp = enable;
        self
    }
}

/// A recursive optimizer that will apply the given rules recursively.
/// It will keep applying the rules on the substituted expression
/// until no more rules can be applied.
pub struct RecursiveOptimizer<'a> {
    ctx: &'a OptimizerContext,
    rules: &'static [RuleID],
}

impl<'a> RecursiveOptimizer<'a> {
    pub fn new(rules: &'static [RuleID], ctx: &'a OptimizerContext) -> Self {
        Self { ctx, rules }
    }

    /// Run the optimizer on the given expression.
    pub fn run(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(Arc::new(self.run(expr)?));
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_transform_rules(&optimized_expr, self.rules)?;

        Ok(result)
    }

    fn apply_transform_rules(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();

        for rule_id in rules {
            let rule = RuleFactory::create_rule(*rule_id, self.ctx.metadata.clone())?;
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
                    let optimized_result = self.optimize_expression(result)?;
                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}

#[minitrace::trace]
pub fn optimize(opt_ctx: OptimizerContext, plan: Plan) -> Result<Plan> {
    match plan {
        Plan::Query {
            s_expr,
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } => Ok(Plan::Query {
            s_expr: Box::new(optimize_query(opt_ctx, *s_expr)?),
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        }),
        Plan::Explain { kind, plan } => match kind {
            ExplainKind::Raw | ExplainKind::Ast(_) | ExplainKind::Syntax(_) => {
                Ok(Plan::Explain { kind, plan })
            }
            ExplainKind::Memo(_) => {
                if let box Plan::Query { ref s_expr, .. } = plan {
                    let (memo, cost_map) = get_optimized_memo(opt_ctx, *s_expr.clone())?;
                    Ok(Plan::Explain {
                        kind: ExplainKind::Memo(display_memo(&memo, &cost_map)?),
                        plan,
                    })
                } else {
                    Err(ErrorCode::BadArguments(
                        "Cannot use EXPLAIN MEMO with a non-query statement",
                    ))
                }
            }
            _ => Ok(Plan::Explain {
                kind,
                plan: Box::new(optimize(opt_ctx, *plan)?),
            }),
        },
        Plan::ExplainAnalyze { plan } => Ok(Plan::ExplainAnalyze {
            plan: Box::new(optimize(opt_ctx, *plan)?),
        }),
        Plan::CopyIntoLocation(CopyIntoLocationPlan { stage, path, from }) => {
            Ok(Plan::CopyIntoLocation(CopyIntoLocationPlan {
                stage,
                path,
                from: Box::new(optimize(opt_ctx, *from)?),
            }))
        }
        Plan::CopyIntoTable(mut plan) if !plan.no_file_to_copy => {
            plan.enable_distributed = opt_ctx.enable_distributed_optimization
                && opt_ctx
                    .table_ctx
                    .get_settings()
                    .get_enable_distributed_copy()?;
            info!(
                "after optimization enable_distributed_copy? : {}",
                plan.enable_distributed
            );
            Ok(Plan::CopyIntoTable(plan))
        }
        Plan::MergeInto(plan) => optimize_merge_into(opt_ctx.clone(), plan),

        // Passthrough statements.
        _ => Ok(plan),
    }
}

pub fn optimize_query(opt_ctx: OptimizerContext, mut s_expr: SExpr) -> Result<SExpr> {
    let contains_local_table_scan = contains_local_table_scan(&s_expr, &opt_ctx.metadata);

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = decorrelate_subquery(opt_ctx.metadata.clone(), s_expr.clone())?;
    }

    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, &opt_ctx).run(&s_expr)?;

    {
        // Cost based optimization
        let mut dphyp_optimized = false;
        if opt_ctx.enable_dphyp && opt_ctx.enable_join_reorder {
            let (dp_res, optimized) =
                DPhpy::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
                    .optimize(Arc::new(s_expr.clone()))?;
            if optimized {
                s_expr = (*dp_res).clone();
                dphyp_optimized = true;
            }
        }
        let mut cascades = CascadesOptimizer::create(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            dphyp_optimized,
        )?;
        s_expr = cascades.optimize(s_expr)?;
    }

    s_expr = if !opt_ctx.enable_join_reorder {
        RecursiveOptimizer::new(&[RuleID::EliminateEvalScalar], &opt_ctx).run(&s_expr)?
    } else {
        RecursiveOptimizer::new(&RESIDUAL_RULES, &opt_ctx).run(&s_expr)?
    };

    // Run distributed query optimization.
    //
    // So far, we don't have ability to execute distributed query
    // with reading data from local tales(e.g. system tables).
    let enable_distributed_query =
        opt_ctx.enable_distributed_optimization && !contains_local_table_scan;
    if enable_distributed_query {
        s_expr = optimize_distributed_query(opt_ctx.table_ctx.clone(), &s_expr)?;
    }

    Ok(s_expr)
}

// TODO(leiysky): reuse the optimization logic with `optimize_query`
fn get_optimized_memo(
    opt_ctx: OptimizerContext,
    s_expr: SExpr,
) -> Result<(Memo, HashMap<IndexType, CostContext>)> {
    let result = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, &opt_ctx).run(&s_expr)?;

    let mut cascades =
        CascadesOptimizer::create(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone(), false)?;
    cascades.optimize(result)?;
    Ok((cascades.memo, cascades.best_cost_map))
}

fn optimize_merge_into(opt_ctx: OptimizerContext, plan: Box<MergeInto>) -> Result<Plan> {
    // optimize source :fix issue #13733
    // reason: if there is subquery,windowfunc exprs etc. see
    // src/planner/semantic/lowering.rs `as_raw_expr()`, we will
    // get dummy index. So we need to use optimizer to solve this.
    let mut right_source = optimize_query(opt_ctx.clone(), plan.input.child(1)?.clone())?;

    // if it's not distributed execution, we should reserve
    // exchange to merge source data.
    if opt_ctx.enable_distributed_optimization
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?
    {
        // we need to remove exchange of right_source, because it's
        // not an end query.
        if let RelOperator::Exchange(_) = right_source.plan.as_ref() {
            right_source = right_source.child(0)?.clone();
        }
    }
    // replace right source
    let mut join_sexpr = plan.input.clone();
    join_sexpr = Box::new(join_sexpr.replace_children(vec![
        Arc::new(join_sexpr.child(0)?.clone()),
        Arc::new(right_source),
    ]));

    // before, we think source table is always the small table.
    // 1. for matched only, we use inner join
    // 2. for insert only, we use right anti join
    // 3. for full merge into, we use right outer join
    // for now, let's import the statistic info to determine left join or right join
    // we just do optimization for the top join (target and source),won't do recursive optimization.
    let rule = RuleFactory::create_rule(RuleID::CommuteJoin, plan.meta_data.clone())?;
    let mut state = TransformResult::new();
    // we will reorder the join order according to the cardinality of target and source.
    rule.apply(&join_sexpr, &mut state)?;
    assert!(state.results().len() <= 1);
    // we need to check whether we do swap left and right.
    let change_join_order = if state.results().len() == 1 {
        join_sexpr = Box::new(state.results()[0].clone());
        true
    } else {
        false
    };

    // we just support left join to use MergeIntoBlockInfoHashTable
    if change_join_order && matches!(plan.merge_type, MergeIntoType::FullOperation) {
        opt_ctx.table_ctx.set_merge_into_join_type(MergeIntoJoin {
            merge_into_join_type: MergeIntoJoinType::Left,
            is_distributed: false,
            target_tbl_idx: plan.target_table_idx,
        })
    }
    // try to optimize distributed join, only if
    // - distributed optimization is enabled
    // - no local table scan
    // - distributed merge-into is enabled
    // - join spilling is disabled
    if opt_ctx.enable_distributed_optimization
        && !contains_local_table_scan(&join_sexpr, &opt_ctx.metadata)
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_join_spilling_threshold()?
            == 0
    {
        // input is a Join_SExpr
        let mut merge_into_join_sexpr =
            optimize_distributed_query(opt_ctx.table_ctx.clone(), &join_sexpr)?;
        // after optimize source, we need to add
        let merge_source_optimizer = MergeSourceOptimizer::create();
        let (optimized_distributed_merge_into_join_sexpr, distributed) = if !merge_into_join_sexpr
            .match_pattern(&merge_source_optimizer.merge_source_pattern)
            || change_join_order
        {
            // we need to judge whether it'a broadcast join to support runtime filter.
            merge_into_join_sexpr = try_to_change_as_broadcast_join(
                merge_into_join_sexpr,
                change_join_order,
                opt_ctx.table_ctx.clone(),
                plan.merge_type.clone(),
                plan.target_table_idx,
            )?;
            (merge_into_join_sexpr.clone(), false)
        } else {
            (
                merge_source_optimizer.optimize(&merge_into_join_sexpr)?,
                true,
            )
        };

        Ok(Plan::MergeInto(Box::new(MergeInto {
            input: Box::new(optimized_distributed_merge_into_join_sexpr),
            distributed,
            change_join_order,
            ..*plan
        })))
    } else {
        Ok(Plan::MergeInto(Box::new(MergeInto {
            input: join_sexpr,
            change_join_order,
            ..*plan
        })))
    }
}

fn try_to_change_as_broadcast_join(
    merge_into_join_sexpr: SExpr,
    change_join_order: bool,
    table_ctx: Arc<dyn TableContext>,
    merge_into_type: MergeIntoType,
    target_tbl_idx: usize,
) -> Result<SExpr> {
    if let RelOperator::Exchange(Exchange::Merge) = merge_into_join_sexpr.plan.as_ref() {
        let right_exchange = merge_into_join_sexpr.child(0)?.child(1)?;
        if let RelOperator::Exchange(Exchange::Broadcast) = right_exchange.plan.as_ref() {
            let mut join: Join = merge_into_join_sexpr.child(0)?.plan().clone().try_into()?;
            join.broadcast = true;
            let join_s_expr = merge_into_join_sexpr
                .child(0)?
                .replace_plan(Arc::new(RelOperator::Join(join)));
            // for now, when we use target table as build side and it's a broadcast join,
            // we will use merge_into_block_info_hashtable to reduce i/o operations.
            if change_join_order && matches!(merge_into_type, MergeIntoType::FullOperation) {
                table_ctx.set_merge_into_join_type(MergeIntoJoin {
                    merge_into_join_type: MergeIntoJoinType::Left,
                    is_distributed: true,
                    target_tbl_idx,
                })
            }
            return Ok(merge_into_join_sexpr.replace_children(vec![Arc::new(join_s_expr)]));
        }
    }
    Ok(merge_into_join_sexpr)
}
