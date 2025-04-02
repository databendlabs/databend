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

use crate::binder::target_probe;
use crate::binder::MutationStrategy;
use crate::binder::MutationType;
use crate::optimizer::distributed::optimize_distributed_query;
use crate::optimizer::distributed::BroadcastToShuffleOptimizer;
use crate::optimizer::distributed::SortAndLimitPushDownOptimizer;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::operator::DeduplicateJoinConditionOptimizer;
use crate::optimizer::operator::PullUpFilterOptimizer;
use crate::optimizer::operator::RuleNormalizeAggregateOptimizer;
use crate::optimizer::operator::RuleStatsAggregateOptimizer;
use crate::optimizer::operator::SingleToInnerOptimizer;
use crate::optimizer::operator::SubqueryRewriter;
use crate::optimizer::optimizers::CascadesOptimizer;
use crate::optimizer::optimizers::DPhpy;
use crate::optimizer::optimizers::RecursiveOptimizer;
use crate::optimizer::statistics::CollectStatisticsOptimizer;
use crate::optimizer::util::contains_local_table_scan;
use crate::optimizer::util::contains_warehouse_table_scan;
use crate::optimizer::OptimizerContext;
use crate::optimizer::RuleID;
use crate::optimizer::DEFAULT_REWRITE_RULES;
use crate::plans::ConstantTableScan;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::MatchedEvaluator;
use crate::plans::Mutation;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::SetScalarsOrQuery;
use crate::InsertInputSource;

#[fastrace::trace]
#[async_recursion(#[recursive::recursive])]
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

                let mut s_expr = s_expr;
                if s_expr.contain_subquery() {
                    s_expr =
                        Box::new(SubqueryRewriter::new(opt_ctx.clone(), None).rewrite(&s_expr)?);
                }
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
        Plan::CopyIntoLocation(CopyIntoLocationPlan {
            stage,
            path,
            from,
            options,
        }) => Ok(Plan::CopyIntoLocation(CopyIntoLocationPlan {
            stage,
            path,
            from: Box::new(Box::pin(optimize(opt_ctx, *from)).await?),
            options,
        })),
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

pub async fn optimize_query(opt_ctx: Arc<OptimizerContext>, mut s_expr: SExpr) -> Result<SExpr> {
    let metadata = opt_ctx.get_metadata();

    if contains_local_table_scan(&s_expr, &metadata) {
        opt_ctx.set_enable_distributed_optimization(false);
        info!("Disable distributed optimization due to local table scan.");
    } else if contains_warehouse_table_scan(&s_expr, &metadata) {
        let warehouse = opt_ctx.get_table_ctx().get_warehouse_cluster().await?;

        if !warehouse.is_empty() {
            opt_ctx.set_enable_distributed_optimization(true);
            info!("Enable distributed optimization due to warehouse table scan.");
        }
    }

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = SubqueryRewriter::new(opt_ctx.clone(), None).rewrite(&s_expr)?;
    }

    s_expr = RuleStatsAggregateOptimizer::new(opt_ctx.clone())
        .run(&s_expr)
        .await?;

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.clone())
        .run(&s_expr)
        .await?;

    // Normalize aggregate, it should be executed before RuleSplitAggregate.
    s_expr = RuleNormalizeAggregateOptimizer::new().run(&s_expr)?;

    // Pull up and infer filter.
    s_expr = PullUpFilterOptimizer::new(opt_ctx.clone()).run(&s_expr)?;

    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES).run(&s_expr)?;

    // Run post rewrite rules
    s_expr = RecursiveOptimizer::new(opt_ctx.clone(), &[RuleID::SplitAggregate]).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.get_enable_dphyp() && opt_ctx.get_enable_join_reorder() {
        let (dp_res, optimized) = DPhpy::new(opt_ctx.clone()).optimize(&s_expr).await?;
        if optimized {
            s_expr = (*dp_res).clone();
            dphyp_optimized = true;
        }
    }

    // After join reorder, Convert some single join to inner join.
    s_expr = SingleToInnerOptimizer::new().run(&s_expr)?;
    // Deduplicate join conditions.
    s_expr = DeduplicateJoinConditionOptimizer::new().run(&s_expr)?;

    let mut cascades = CascadesOptimizer::new(opt_ctx.clone(), dphyp_optimized)?;

    if opt_ctx.get_enable_join_reorder() {
        s_expr = RecursiveOptimizer::new(opt_ctx.clone(), [RuleID::CommuteJoin].as_slice())
            .run(&s_expr)?;
    }

    // Cascades optimizer may fail due to timeout, fallback to heuristic optimizer in this case.
    s_expr = match cascades.optimize(s_expr.clone()) {
        Ok(mut s_expr) => {
            // Push down sort and limit
            // TODO(leiysky): do this optimization in cascades optimizer
            if opt_ctx.get_enable_distributed_optimization() {
                let sort_and_limit_optimizer = SortAndLimitPushDownOptimizer::create();
                s_expr = sort_and_limit_optimizer.optimize(&s_expr)?;
            }
            s_expr
        }

        Err(e) => {
            info!(
                "CascadesOptimizer failed, fallback to heuristic optimizer: {}",
                e
            );
            if opt_ctx.get_enable_distributed_optimization() {
                s_expr = optimize_distributed_query(opt_ctx.get_table_ctx().clone(), &s_expr)?;
            }

            s_expr
        }
    };

    if !opt_ctx.get_planning_agg_index() {
        s_expr = RecursiveOptimizer::new(opt_ctx.clone(), [RuleID::EliminateEvalScalar].as_slice())
            .run(&s_expr)?;
    }

    Ok(s_expr)
}

// TODO(leiysky): reuse the optimization logic with `optimize_query`
async fn get_optimized_memo(opt_ctx: Arc<OptimizerContext>, mut s_expr: SExpr) -> Result<Memo> {
    let metadata = opt_ctx.get_metadata();
    let _table_ctx = opt_ctx.get_table_ctx();

    if contains_local_table_scan(&s_expr, &metadata) {
        opt_ctx.set_enable_distributed_optimization(false);
        info!("Disable distributed optimization due to local table scan.");
    } else if contains_warehouse_table_scan(&s_expr, &metadata) {
        let warehouse = opt_ctx.get_table_ctx().get_warehouse_cluster().await?;

        if !warehouse.is_empty() {
            opt_ctx.set_enable_distributed_optimization(true);
            info!("Enable distributed optimization due to warehouse table scan.");
        }
    }

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = SubqueryRewriter::new(opt_ctx.clone(), None).rewrite(&s_expr)?;
    }

    s_expr = RuleStatsAggregateOptimizer::new(opt_ctx.clone())
        .run(&s_expr)
        .await?;

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.clone())
        .run(&s_expr)
        .await?;

    // Pull up and infer filter.
    s_expr = PullUpFilterOptimizer::new(opt_ctx.clone()).run(&s_expr)?;
    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(opt_ctx.clone(), &DEFAULT_REWRITE_RULES).run(&s_expr)?;
    // Run post rewrite rules
    s_expr = RecursiveOptimizer::new(opt_ctx.clone(), &[RuleID::SplitAggregate]).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.get_enable_dphyp() && opt_ctx.get_enable_join_reorder() {
        let (dp_res, optimized) = DPhpy::new(opt_ctx.clone()).optimize(&s_expr).await?;
        if optimized {
            s_expr = (*dp_res).clone();
            dphyp_optimized = true;
        }
    }
    let mut cascades = CascadesOptimizer::new(opt_ctx.clone(), dphyp_optimized)?;
    cascades.optimize(s_expr)?;

    Ok(cascades.memo)
}

async fn optimize_mutation(opt_ctx: Arc<OptimizerContext>, s_expr: SExpr) -> Result<Plan> {
    // Optimize the input plan.
    let mut input_s_expr = optimize_query(opt_ctx.clone(), s_expr.child(0)?.clone()).await?;
    input_s_expr = RecursiveOptimizer::new(opt_ctx.clone(), &[RuleID::MergeFilterIntoMutation])
        .run(&input_s_expr)?;

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
            if let RelOperator::MutationSource(rel) = input_s_expr.plan() {
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
