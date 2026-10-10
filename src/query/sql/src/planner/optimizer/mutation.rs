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
use databend_common_expression::DataSchemaRef;

use super::optimizer::query_logical_pipeline;
use crate::binder::MutationStrategy;
use crate::binder::MutationType;
use crate::optimizer::OptimizerContext;
use crate::optimizer::PhysicalPlanner;
use crate::optimizer::ir::MutationPlan;
use crate::optimizer::ir::PExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::distributed::BroadcastToShuffleOptimizer;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::pipeline::configure_distributed_optimization;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::MatchedEvaluator;
use crate::plans::Mutation;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator;

/// Mutation-specific logical state, ready for input-plan selection. Keep preparation
/// separate from decisions that depend on the selected join/distribution implementation.
struct PreparedMutation {
    mutation: Mutation,
    input: SExpr,
    schema: DataSchemaRef,
}

impl PreparedMutation {
    async fn prepare(opt_ctx: Arc<OptimizerContext>, s_expr: &SExpr) -> Result<Self> {
        let mut mutation: Mutation = s_expr.plan().clone().try_into()?;
        // Logical simplification must not change the statement's result columns.
        let schema = mutation.schema();
        let mut pipeline =
            query_logical_pipeline(opt_ctx.clone(), s_expr.child(0)?.clone(), None).await?;
        let input = pipeline.execute().await?;
        // Mutation preparation is required even when optional recursive rewrites are
        // disabled. Keep this outside the pipeline's optimizer skip-list mechanism.
        let mut input = RecursiveRuleOptimizer::new(opt_ctx, &[RuleID::MergeFilterIntoMutation])
            .optimize_sync(input)?;
        prepare_empty_input(&input, &mut mutation);
        if mutation.strategy == MutationStrategy::Direct
            && let Some(prepared) = prepare_direct_source(&input, &mut mutation)?
        {
            input = prepared;
        }
        #[cfg(debug_assertions)]
        {
            input.validate_types(&mutation.metadata)?;
            input.validate_column_scope(&mutation.metadata)?;
            if let Some(index) = mutation.predicate_column_index {
                debug_assert!(mutation.required_columns.contains(&index));
                // MutationSource materializes this execution-only predicate column;
                // it is not part of the source's logical output-column set.
            }
        }
        Ok(Self {
            mutation,
            input,
            schema,
        })
    }
}

/// Select and finalize the input using the mutation distribution policy. A local
/// retry reuses the prepared logical input, rather than rerunning preparation on raw SQL.
async fn plan_input(opt_ctx: Arc<OptimizerContext>, input: SExpr, local: bool) -> Result<PExpr> {
    configure_distributed_optimization(&opt_ctx, &input).await?;
    let mut planner = PhysicalPlanner::new(opt_ctx);
    let planned = if local {
        planner.plan_local(input).await?
    } else {
        planner.plan(input).await?
    };
    Ok(planned.into_expr())
}

pub(super) async fn optimize_mutation(
    opt_ctx: Arc<OptimizerContext>,
    s_expr: SExpr,
) -> Result<Plan> {
    let PreparedMutation {
        mut mutation,
        input,
        schema,
    } = PreparedMutation::prepare(opt_ctx.clone(), &s_expr).await?;
    let mut planned_input = plan_input(opt_ctx.clone(), input.clone(), false).await?;

    // Apply the mutation consumer policy until requirements are part of
    // physical search: discard the query-root Exchange and retry locally if necessary.
    if matches!(planned_input.plan(), RelOperator::Exchange(_)) {
        planned_input = planned_input.child(0)?.clone();
    }
    if planned_input.has_merge_exchange() {
        planned_input = plan_input(opt_ctx.clone(), input, true).await?;
    }
    mutation.distributed = opt_ctx.get_enable_distributed_optimization();
    let inner_rel_op = planned_input.plan.rel_op();
    planned_input = match mutation.mutation_type {
        MutationType::Merge => {
            if mutation.distributed && inner_rel_op == RelOp::Join {
                let join = Join::try_from(planned_input.plan().clone())?;
                let broadcast_to_shuffle = BroadcastToShuffleOptimizer::create();
                let is_broadcast = broadcast_to_shuffle.matcher.matches(&planned_input)
                    && broadcast_to_shuffle.is_broadcast(&planned_input)?;

                // If the mutation strategy is matched only, the join type is inner join, if it is a broadcast
                // join and the target table on the probe side, we can avoid row id shuffle after the join.
                let target_probe = target_probe(&planned_input, mutation.target_table_index)?;
                if is_broadcast
                    && target_probe
                    && mutation.strategy == MutationStrategy::MatchedOnly
                {
                    mutation.row_id_shuffle = false;
                }

                // Change broadcast join to shuffle join if the join type is left or left-anti join, because
                // broadcast join can not deduplicate row ids.
                if is_broadcast && matches!(join.join_type, JoinType::Left | JoinType::LeftAnti) {
                    broadcast_to_shuffle.optimize(&planned_input)?
                } else {
                    planned_input
                }
            } else {
                planned_input
            }
        }
        MutationType::Update | MutationType::Delete => planned_input,
    };

    Ok(Plan::DataMutation {
        schema,
        s_expr: Box::new(MutationPlan::Planned(PExpr::create_unary(
            Arc::new(RelOperator::Mutation(mutation)),
            Arc::new(planned_input),
        ))),
        metadata: opt_ctx.get_metadata(),
    })
}

fn prepare_empty_input(input: &SExpr, mutation: &mut Mutation) {
    if mutation.matched_evaluators.is_empty() {
        return;
    }
    match input.plan() {
        RelOperator::ConstantTableScan(scan) if scan.num_rows == 0 => mutation.no_effect = true,
        RelOperator::Join(_) => {
            // Logical join rewrites may commute the target. Its row-id symbol survives
            // an empty-scan rewrite and identifies it without relying on child position.
            for child in input.children() {
                if let RelOperator::ConstantTableScan(scan) = child.plan()
                    && scan.num_rows == 0
                    && scan.columns.contains(&mutation.row_id_index)
                {
                    mutation.matched_evaluators = vec![MatchedEvaluator {
                        condition: None,
                        update: None,
                    }];
                    mutation.can_try_update_column_only = false;
                    break;
                }
            }
        }
        _ => {}
    }
}

fn prepare_direct_source(s_expr: &SExpr, mutation: &mut Mutation) -> Result<Option<SExpr>> {
    match s_expr.plan() {
        RelOperator::MutationSource(rel) => {
            let mut rel = rel.clone();
            rel.refresh_read_partition_columns();
            let is_truncate = rel.mutation_type == MutationType::Delete && !rel.has_predicates();
            let direct_filter = rel.all_predicates_cloned();
            let predicate_column_index =
                rel.ensure_mutation_predicate_column_if_needed(&mutation.metadata);
            let new_s_expr = SExpr::create_leaf(Arc::new(RelOperator::MutationSource(rel)));
            mutation.truncate_table = is_truncate;
            mutation.direct_filter = direct_filter;
            if let Some(index) = predicate_column_index {
                mutation.required_columns.insert(index);
                mutation.predicate_column_index = Some(index);
            }
            Ok(Some(new_s_expr))
        }
        RelOperator::Udf(_) | RelOperator::EvalScalar(_) if s_expr.arity() == 1 => {
            if let Some(child) = prepare_direct_source(s_expr.unary_child(), mutation)? {
                Ok(Some(s_expr.replace_children([Arc::new(child)])))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn target_probe(s_expr: &PExpr, target_table_index: usize) -> Result<bool> {
    if !matches!(s_expr.plan(), RelOperator::Join(_)) {
        return Ok(false);
    }

    fn contains_target_table(s_expr: &PExpr, target_table_index: usize) -> bool {
        if let RelOperator::Scan(scan) = s_expr.plan() {
            scan.table_index == target_table_index
        } else {
            s_expr
                .children()
                .any(|child| contains_target_table(child, target_table_index))
        }
    }

    Ok(contains_target_table(s_expr.child(0)?, target_table_index))
}
