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

use async_recursion::async_recursion;
use databend_common_ast::ast::ExplainKind;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;

use crate::InsertInputSource;
use crate::binder::MutationStrategy;
use crate::binder::MutationType;
use crate::binder::target_probe;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::CTEFilterPushdownOptimizer;
use crate::optimizer::optimizers::CascadesOptimizer;
use crate::optimizer::optimizers::CommonSubexpressionOptimizer;
use crate::optimizer::optimizers::DPhpyOptimizer;
use crate::optimizer::optimizers::EliminateSelfJoinOptimizer;
use crate::optimizer::optimizers::distributed::BroadcastToShuffleOptimizer;
use crate::optimizer::optimizers::operator::CleanupUnusedCTEOptimizer;
use crate::optimizer::optimizers::operator::DeduplicateJoinConditionOptimizer;
use crate::optimizer::optimizers::operator::PullUpFilterOptimizer;
use crate::optimizer::optimizers::operator::RuleNormalizeAggregateOptimizer;
use crate::optimizer::optimizers::operator::RuleStatsAggregateOptimizer;
use crate::optimizer::optimizers::operator::SingleToInnerOptimizer;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use crate::optimizer::optimizers::rule::RuleEagerAggregation;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::pipeline::OptimizerPipeline;
use crate::optimizer::statistics::CollectStatisticsOptimizer;
use crate::plans::ConstantTableScan;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::MatchedEvaluator;
use crate::plans::Mutation;
use crate::plans::MutationSource;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::SetScalarsOrQuery;

#[fastrace::trace]
#[async_recursion(# [recursive::recursive])]
pub async fn optimize(opt_ctx: Arc<OptimizerContext>, plan: Plan) -> Result<Plan> {
    match plan {
        Plan::Query {
            s_expr,
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } => Ok(Plan::Query {
            s_expr: Box::new(optimize_query(opt_ctx, *s_expr).await?),
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        }),
        Plan::Explain { kind, config, plan } => match kind {
            ExplainKind::Ast(_) | ExplainKind::Syntax(_) => {
                Ok(Plan::Explain { config, kind, plan })
            }
            ExplainKind::Plan if config.decorrelated => {
                let Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    rewrite_kind,
                    formatted_ast,
                    ignore_result,
                } = *plan
                else {
                    return Err(ErrorCode::BadArguments(
                        "Cannot use EXPLAIN DECORRELATED with a non-query statement",
                    ));
                };

                let s_expr = Box::new(
                    SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None)
                        .optimize_sync(&s_expr)?,
                );
                Ok(Plan::Explain {
                    kind,
                    config,
                    plan: Box::new(Plan::Query {
                        s_expr,
                        bind_context,
                        metadata,
                        rewrite_kind,
                        formatted_ast,
                        ignore_result,
                    }),
                })
            }
            ExplainKind::Memo(_) => {
                if let box Plan::Query { ref s_expr, .. } = plan {
                    let memo = get_optimized_memo(opt_ctx.clone(), *s_expr.clone()).await?;
                    Ok(Plan::Explain {
                        config,
                        kind: ExplainKind::Memo(memo.display()?),
                        plan,
                    })
                } else {
                    Err(ErrorCode::BadArguments(
                        "Cannot use EXPLAIN MEMO with a non-query statement",
                    ))
                }
            }
            _ => {
                if config.optimized || !config.logical {
                    let optimized_plan = Box::pin(optimize(opt_ctx.clone(), *plan)).await?;
                    Ok(Plan::Explain {
                        kind,
                        config,
                        plan: Box::new(optimized_plan),
                    })
                } else {
                    Ok(Plan::Explain { kind, config, plan })
                }
            }
        },
        Plan::ExplainAnalyze {
            plan,
            partial,
            graphical,
        } => Ok(Plan::ExplainAnalyze {
            partial,
            graphical,
            plan: Box::new(Box::pin(optimize(opt_ctx, *plan)).await?),
        }),
        Plan::CopyIntoLocation(mut plan) => {
            plan.from = Box::new(Box::pin(optimize(opt_ctx, *plan.from)).await?);
            Ok(Plan::CopyIntoLocation(plan))
        }
        Plan::CopyIntoTable(mut plan) if !plan.no_file_to_copy => {
            plan.enable_distributed = opt_ctx.get_enable_distributed_optimization()
                && opt_ctx
                    .get_table_ctx()
                    .get_settings()
                    .get_enable_distributed_copy()?;
            info!(
                "after optimization enable_distributed_copy? : {}",
                plan.enable_distributed
            );

            if let Some(p) = &plan.query {
                let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                plan.query = Some(Box::new(optimized_plan));
            }
            Ok(Plan::CopyIntoTable(plan))
        }
        Plan::DataMutation { s_expr, .. } => optimize_mutation(opt_ctx, *s_expr).await,

        // distributed insert will be optimized in `physical_plan_builder`
        Plan::Insert(mut plan) => {
            match plan.source {
                InsertInputSource::SelectPlan(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::SelectPlan(Box::new(optimized_plan));
                }
                InsertInputSource::Stage(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::Stage(Box::new(optimized_plan));
                }
                _ => {}
            }
            Ok(Plan::Insert(plan))
        }
        Plan::InsertMultiTable(mut plan) => {
            plan.input_source = optimize(opt_ctx.clone(), plan.input_source.clone()).await?;
            Ok(Plan::InsertMultiTable(plan))
        }
        Plan::Replace(mut plan) => {
            match plan.source {
                InsertInputSource::SelectPlan(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::SelectPlan(Box::new(optimized_plan));
                }
                InsertInputSource::Stage(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::Stage(Box::new(optimized_plan));
                }
                _ => {}
            }
            Ok(Plan::Replace(plan))
        }

        Plan::CreateTable(mut plan) => {
            if let Some(p) = &plan.as_select {
                let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                plan.as_select = Some(Box::new(optimized_plan));
            }

            Ok(Plan::CreateTable(plan))
        }

        Plan::Set(mut plan) => {
            if let SetScalarsOrQuery::Query(q) = plan.values {
                let optimized_plan = optimize(opt_ctx.clone(), *q.clone()).await?;
                plan.values = SetScalarsOrQuery::Query(Box::new(optimized_plan))
            }

            Ok(Plan::Set(plan))
        }

        // Already done in binder
        // Plan::RefreshIndex(mut plan) => {
        //     // use fresh index
        //     let opt_ctx =
        //         OptimizerContext::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone());
        //     plan.query_plan = Box::new(optimize(opt_ctx.clone(), *plan.query_plan.clone()).await?);
        //     Ok(Plan::RefreshIndex(plan))
        // }
        // Pass through statements.
        _ => Ok(plan),
    }
}

pub async fn optimize_query(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<SExpr> {
    let settings = opt_ctx.get_table_ctx().get_settings();
    let mut pipeline = OptimizerPipeline::new(opt_ctx.clone(), s_expr.clone())
        .await?
        // Eliminate subqueries by rewriting them into more efficient form
        .add(SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None))
        // Apply statistics aggregation to gather and propagate statistics
        .add(RuleStatsAggregateOptimizer::new(opt_ctx.clone()))
        // Collect statistics for SExpr nodes to support cost estimation
        .add(CollectStatisticsOptimizer::new(opt_ctx.clone()))
        // Normalize aggregate, it should be executed before RuleSplitAggregate.
        .add(RuleNormalizeAggregateOptimizer::new())
        // Pull up and infer filter.
        .add(PullUpFilterOptimizer::new(opt_ctx.clone()))
        // Run default rewrite rules
        .add(RecursiveRuleOptimizer::new(
            opt_ctx.clone(),
            &DEFAULT_REWRITE_RULES,
        ))
        // CTE filter pushdown optimization
        .add(CTEFilterPushdownOptimizer::new(opt_ctx.clone()))
        // Run post rewrite rules
        .add(RecursiveRuleOptimizer::new(opt_ctx.clone(), &[
            RuleID::SplitAggregate,
        ]))
        // Apply DPhyp algorithm for cost-based join reordering
        .add(DPhpyOptimizer::new(opt_ctx.clone()))
        // Eliminate self joins when possible
        .add(EliminateSelfJoinOptimizer::new(opt_ctx.clone()))
        // After join reorder, Convert some single join to inner join.
        .add(SingleToInnerOptimizer::new())
        // Deduplicate join conditions.
        .add(DeduplicateJoinConditionOptimizer::new())
        // Apply join commutativity to further optimize join ordering
        .add_if(
            opt_ctx.get_enable_join_reorder(),
            RecursiveRuleOptimizer::new(opt_ctx.clone(), [RuleID::CommuteJoin].as_slice()),
        )
        .add_if(
            settings.get_force_eager_aggregate()?,
            RuleEagerAggregation::new(opt_ctx.get_metadata()),
        )
        // Common subexpression elimination optimization
        // TODO(Sky): Currently uses heuristic approach, will be integrated into Cascades optimizer in the future.
        .add_if(
            settings.get_enable_cse_optimizer()?,
            CommonSubexpressionOptimizer::new(opt_ctx.clone()),
        )
        // Cascades optimizer may fail due to timeout, fallback to heuristic optimizer in this case.
        .add(CascadesOptimizer::new(opt_ctx.clone())?)
        // Eliminate unnecessary scalar calculations to clean up the final plan
        .add_if(
            !opt_ctx.get_planning_agg_index(),
            RecursiveRuleOptimizer::new(opt_ctx.clone(), [RuleID::EliminateEvalScalar].as_slice()),
        )
        // Clean up unused CTEs
        .add(CleanupUnusedCTEOptimizer);

    // 17. Execute the pipeline
    let s_expr = pipeline.execute().await?;

    Ok(s_expr)
}

async fn get_optimized_memo(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<Memo> {
    let mut pipeline = OptimizerPipeline::new(opt_ctx.clone(), s_expr.clone())
        .await?
        // Decorrelate subqueries, after this step, there should be no subquery in the expression.
        .add(SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None))
        .add(RuleStatsAggregateOptimizer::new(opt_ctx.clone()))
        // Collect statistics for each leaf node in SExpr.
        .add(CollectStatisticsOptimizer::new(opt_ctx.clone()))
        // Pull up and infer filter.
        .add(PullUpFilterOptimizer::new(opt_ctx.clone()))
        // Run default rewrite rules
        .add(RecursiveRuleOptimizer::new(
            opt_ctx.clone(),
            &DEFAULT_REWRITE_RULES,
        ))
        // Run post rewrite rules
        .add(RecursiveRuleOptimizer::new(opt_ctx.clone(), &[
            RuleID::SplitAggregate,
        ]))
        // Cost based optimization
        .add(DPhpyOptimizer::new(opt_ctx.clone()))
        .add(CascadesOptimizer::new(opt_ctx.clone())?);

    let _s_expr = pipeline.execute().await?;

    Ok(pipeline.memo())
}

async fn optimize_mutation(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<Plan> {
    // Optimize the input plan.
    let mut input_s_expr = optimize_query(opt_ctx.clone(), s_expr.child(0)?.clone()).await?;
    input_s_expr = RecursiveRuleOptimizer::new(opt_ctx.clone(), &[RuleID::MergeFilterIntoMutation])
        .optimize_sync(&input_s_expr)?;

    // For distributed query optimization, we need to remove the Exchange operator at the top of the plan.
    if let &RelOperator::Exchange(_) = input_s_expr.plan() {
        input_s_expr = input_s_expr.child(0)?.clone();
    }
    // If there still exists an Exchange::Merge operator, we should disable distributed optimization and
    // optimize the input plan again.
    if input_s_expr.has_merge_exchange() {
        opt_ctx.set_enable_distributed_optimization(false);
        input_s_expr = optimize_query(opt_ctx.clone(), s_expr.child(0)?.clone()).await?;
    }

    let mut mutation: Mutation = s_expr.plan().clone().try_into()?;
    mutation.distributed = opt_ctx.get_enable_distributed_optimization();

    let schema = mutation.schema();
    // To fix issue #16588, if target table is rewritten as an empty scan, that means
    // the condition is false and the match branch can never be executed.
    // Therefore, the match evaluators can be reset.
    let inner_rel_op = input_s_expr.plan.rel_op();
    if !mutation.matched_evaluators.is_empty() {
        match inner_rel_op {
            RelOp::ConstantTableScan => {
                let constant_table_scan = ConstantTableScan::try_from(input_s_expr.plan().clone())?;
                if constant_table_scan.num_rows == 0 {
                    mutation.no_effect = true;
                }
            }
            RelOp::Join => {
                let mut right_child = input_s_expr.child(1)?;
                let mut right_child_rel = right_child.plan.rel_op();
                if right_child_rel == RelOp::Exchange {
                    right_child_rel = right_child.child(0)?.plan.rel_op();
                    right_child = right_child.child(0)?;
                }
                if right_child_rel == RelOp::ConstantTableScan {
                    let constant_table_scan =
                        ConstantTableScan::try_from(right_child.plan().clone())?;
                    if constant_table_scan.num_rows == 0 {
                        mutation.matched_evaluators = vec![MatchedEvaluator {
                            condition: None,
                            update: None,
                        }];
                        mutation.can_try_update_column_only = false;
                    }
                }
            }
            _ => (),
        }
    }

    input_s_expr = match mutation.mutation_type {
        MutationType::Merge => {
            if mutation.distributed && inner_rel_op == RelOp::Join {
                let join = Join::try_from(input_s_expr.plan().clone())?;
                let broadcast_to_shuffle = BroadcastToShuffleOptimizer::create();
                let is_broadcast = broadcast_to_shuffle.matcher.matches(&input_s_expr)
                    && broadcast_to_shuffle.is_broadcast(&input_s_expr)?;

                // If the mutation strategy is matched only, the join type is inner join, if it is a broadcast
                // join and the target table on the probe side, we can avoid row id shuffle after the join.
                let target_probe = target_probe(&input_s_expr, mutation.target_table_index)?;
                if is_broadcast
                    && target_probe
                    && mutation.strategy == MutationStrategy::MatchedOnly
                {
                    mutation.row_id_shuffle = false;
                }

                // Change broadcast join to shuffle join if the join type is left or left-anti join, because
                // broadcast join can not deduplicate row ids.
                if is_broadcast && matches!(join.join_type, JoinType::Left | JoinType::LeftAnti) {
                    broadcast_to_shuffle.optimize(&input_s_expr)?
                } else {
                    input_s_expr
                }
            } else {
                input_s_expr
            }
        }
        MutationType::Update | MutationType::Delete => {
            // Helper function to find MutationSource in a chain of operators
            fn find_mutation_source(s_expr: &SExpr) -> Option<&MutationSource> {
                match s_expr.plan() {
                    RelOperator::MutationSource(rel) => Some(rel),
                    RelOperator::Udf(_) | RelOperator::EvalScalar(_) => {
                        if s_expr.arity() == 1 {
                            find_mutation_source(s_expr.unary_child())
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }

            if let Some(rel) = find_mutation_source(&input_s_expr) {
                if rel.mutation_type == MutationType::Delete && rel.predicates.is_empty() {
                    mutation.truncate_table = true;
                }
                mutation.direct_filter = rel.predicates.clone();
                if let Some(index) = rel.predicate_column_index {
                    mutation.required_columns.insert(index);
                    mutation.predicate_column_index = Some(index);
                }
            }
            input_s_expr
        }
    };

    Ok(Plan::DataMutation {
        schema,
        s_expr: Box::new(SExpr::create_unary(
            Arc::new(RelOperator::Mutation(mutation)),
            Arc::new(input_s_expr),
        )),
        metadata: opt_ctx.get_metadata(),
    })
}
