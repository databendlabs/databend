// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::FilterPlan;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct AliasPushDownOptimizer;

impl AliasPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        AliasPushDownOptimizer {}
    }
}

impl IOptimizer for AliasPushDownOptimizer {
    fn name(&self) -> &str {
        "FilterPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_plan = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        let projection_map = PlanRewriter::projection_to_map(plan)?;
        plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::Filter(plan) => {
                    let rewritten_expr =
                        PlanRewriter::rewrite_alias_expr(&projection_map, &plan.predicate)?;
                    let mut new_plan = PlanNode::Filter(FilterPlan {
                        predicate: rewritten_expr,
                        input: Arc::from(PlanNode::Empty(EmptyPlan::create()))
                    });
                    new_plan.set_child(0, &rewritten_plan);
                    rewritten_plan = new_plan;
                }
                PlanNode::AggregatorPartial(plan) => {
                    let aggr_expr =
                        PlanRewriter::rewrite_alias_exprs(&projection_map, &plan.aggr_expr)?;
                    let group_expr =
                        PlanRewriter::rewrite_alias_exprs(&projection_map, &plan.group_expr)?;
                    let mut new_plan = PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr,
                        aggr_expr,
                        input: Arc::from(PlanNode::Empty(EmptyPlan::create()))
                    });
                    new_plan.set_child(0, &rewritten_plan);
                    rewritten_plan = new_plan;
                }
                PlanNode::AggregatorFinal(plan) => {
                    let aggr_expr =
                        PlanRewriter::rewrite_alias_exprs(&projection_map, &plan.aggr_expr)?;
                    let group_expr =
                        PlanRewriter::rewrite_alias_exprs(&projection_map, &plan.group_expr)?;
                    let mut new_plan = PlanNode::AggregatorFinal(AggregatorFinalPlan {
                        group_expr,
                        aggr_expr,
                        input: Arc::from(PlanNode::Empty(EmptyPlan::create())),
                        schema: plan.schema()
                    });
                    new_plan.set_child(0, &rewritten_plan);
                    rewritten_plan = new_plan;
                }
                _ => {
                    let mut new_plan = node.clone();
                    new_plan.set_child(0, &rewritten_plan);
                    rewritten_plan = new_plan;
                }
            }
            Ok(true)
        })?;

        Ok(rewritten_plan)
    }
}
