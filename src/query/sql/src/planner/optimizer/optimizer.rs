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
use databend_common_expression::Symbol;
use log::info;

use crate::InsertInputSource;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Memo;
use crate::optimizer::ir::SExpr;
use crate::optimizer::mutation::optimize_mutation;
use crate::optimizer::optimizers::CTEFilterPushdownOptimizer;
use crate::optimizer::optimizers::CascadesOptimizer;
use crate::optimizer::optimizers::CommonSubexpressionOptimizer;
use crate::optimizer::optimizers::DPhpyOptimizer;
use crate::optimizer::optimizers::EliminateSelfJoinOptimizer;
use crate::optimizer::optimizers::operator::CleanupUnusedCTEOptimizer;
use crate::optimizer::optimizers::operator::DeduplicateJoinConditionOptimizer;
use crate::optimizer::optimizers::operator::FinalizeSpatialJoinOptimizer;
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
use crate::plans::EvalScalar;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
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
        } => {
            let query_output_columns = bind_context
                .columns
                .iter()
                .map(|column| column.index)
                .collect();
            Ok(Plan::Query {
                s_expr: Box::new(
                    optimize_query_with_output_columns(opt_ctx, *s_expr, query_output_columns)
                        .await?,
                ),
                bind_context,
                metadata,
                rewrite_kind,
                formatted_ast,
                ignore_result,
            })
        }
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
                        .optimize_sync(*s_expr)?,
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
                if let deref!( Plan::Query { ref s_expr, .. }) = plan {
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
            // WHEN subqueries introduce logical joins/aggregates. Rewrite them before
            // selecting the source implementation so those nodes participate in CBO.
            rewrite_insert_multi_table_whens(opt_ctx.clone(), plan.as_mut())?;
            if let Plan::Query {
                s_expr,
                bind_context,
                ..
            } = &mut plan.input_source
            {
                let mut output_columns = bind_context
                    .column_set()
                    .into_iter()
                    .collect::<std::collections::HashSet<_>>();
                for when in &plan.whens {
                    output_columns.extend(when.condition.used_columns());
                }
                let input = s_expr.as_ref().clone();
                let planned =
                    optimize_query_with_output_columns(opt_ctx, input, output_columns).await?;
                *s_expr = Box::new(planned);
            } else {
                plan.input_source = optimize(opt_ctx, plan.input_source.clone()).await?;
            }
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
        Plan::CreateDynamicTable(mut plan) => {
            if let Some(p) = plan.table_plan.as_select.take() {
                plan.table_plan.as_select = Some(Box::new(optimize(opt_ctx.clone(), *p).await?));
            }
            Ok(Plan::CreateDynamicTable(plan))
        }
        // `CreateView.query_plan` only feeds lineage and access checks, both of which work
        // on the bound plan; it is never executed, so there is nothing to optimize.
        Plan::CreateView(plan) => Ok(Plan::CreateView(plan)),
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
    optimize_query_inner(opt_ctx, s_expr, None).await
}

async fn optimize_query_with_output_columns(
    opt_ctx: Arc<OptimizerContext>,
    s_expr: SExpr,
    output_columns: std::collections::HashSet<Symbol>,
) -> Result<SExpr> {
    optimize_query_inner(opt_ctx, s_expr, Some(output_columns)).await
}

async fn optimize_query_inner(
    opt_ctx: Arc<OptimizerContext>,
    s_expr: SExpr,
    output_columns: Option<std::collections::HashSet<Symbol>>,
) -> Result<SExpr> {
    let pipeline = query_logical_pipeline(opt_ctx.clone(), s_expr, output_columns).await?;
    let mut pipeline = query_planning_pipeline(opt_ctx, pipeline)?;
    pipeline.execute().await
}

/// Build the common logical passes without selecting distributions or execution plans.
/// Mutation preparation runs after these passes and before query planning.
pub(super) async fn query_logical_pipeline(
    opt_ctx: Arc<OptimizerContext>,
    s_expr: SExpr,
    output_columns: Option<std::collections::HashSet<Symbol>>,
) -> Result<OptimizerPipeline> {
    let settings = opt_ctx.get_table_ctx().get_settings();
    let pipeline = OptimizerPipeline::new(opt_ctx.clone(), s_expr)
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
        // Common subexpression elimination optimization
        // TODO(Sky): Currently uses heuristic approach, will be integrated into Cascades optimizer in the future.
        .add_if(
            settings.get_enable_cse_optimizer()?,
            CommonSubexpressionOptimizer::new(opt_ctx.clone()),
        )
        // Run default rewrite rules. Only an outer Plan::Query supplies authoritative result
        // columns; internal mutation/query fragments leave them unset.
        .add(
            RecursiveRuleOptimizer::new_with_materialized_view_output_columns(
                opt_ctx.clone(),
                &DEFAULT_REWRITE_RULES,
                output_columns,
            ),
        )
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
        );
    Ok(pipeline)
}

/// Append the existing planning and cleanup passes. Ordinary queries keep a single
/// pipeline; mutation inputs enter here only after their logical preparation.
pub(super) fn query_planning_pipeline(
    opt_ctx: Arc<OptimizerContext>,
    pipeline: OptimizerPipeline,
) -> Result<OptimizerPipeline> {
    Ok(pipeline
        // Cascades optimizer may fail due to timeout, fallback to heuristic optimizer in this case.
        .add(CascadesOptimizer::new(opt_ctx.clone())?)
        // Eliminate unnecessary scalar calculations to clean up the final plan
        .add(RecursiveRuleOptimizer::new(
            opt_ctx.clone(),
            [RuleID::EliminateEvalScalar].as_slice(),
        ))
        // Clean up unused CTEs
        .add(CleanupUnusedCTEOptimizer)
        // Finalize derived join annotations after all logical rewrites.
        .add(FinalizeSpatialJoinOptimizer::new(opt_ctx.clone())))
}

fn rewrite_insert_multi_table_whens(
    opt_ctx: Arc<OptimizerContext>,
    plan: &mut crate::plans::InsertMultiTable,
) -> Result<()> {
    let Plan::Query { s_expr, .. } = &mut plan.input_source else {
        return Ok(());
    };

    let mut source_expr = s_expr.as_ref().clone();
    let mut rewritten_any = false;

    for (idx, when) in plan.whens.iter_mut().enumerate() {
        if !when.condition.has_subquery() {
            continue;
        }

        let condition_index = opt_ctx.get_metadata().write().add_derived_column(
            format!("_insert_multi_when_{}", idx),
            when.condition.data_type().into_owned(),
        );
        let eval_expr = source_expr.clone().build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: when.condition.clone(),
                index: condition_index,
            }],
        });

        let mut rewriter = SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None);
        let rewritten = rewriter.optimize_sync(eval_expr)?;
        let RelOperator::EvalScalar(eval) = rewritten.plan() else {
            return Err(ErrorCode::Internal(
                "Subquery rewrite for multi-table insert must keep the top eval scalar".to_string(),
            ));
        };
        let scalar_item = eval.items.first().ok_or_else(|| {
            ErrorCode::Internal(
                "Subquery rewrite for multi-table insert must keep one eval scalar item"
                    .to_string(),
            )
        })?;

        when.condition = scalar_item.scalar.clone();
        source_expr = rewritten.child(0)?.clone();
        rewritten_any = true;
    }

    if rewritten_any {
        *s_expr = Box::new(source_expr);
    }

    Ok(())
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
