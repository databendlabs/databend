// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataSchema;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::EmptyPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::optimizers::OptimizerCommon;
use crate::sessions::FuseQueryContextRef;

pub struct GroupByPushDownOptimizer {}

impl GroupByPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        GroupByPushDownOptimizer {}
    }
}

impl IOptimizer for GroupByPushDownOptimizer {
    fn name(&self) -> &str {
        "GroupByPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        let projection_map = OptimizerCommon::projection_to_map(plan)?;
        plan.walk_postorder(|node| {
            match node {
                PlanNode::AggregatorPartial(plan) => {
                    let aggr_expr =
                        OptimizerCommon::rewrite_alias_exprs(&projection_map, &plan.aggr_expr)?;
                    let group_expr =
                        OptimizerCommon::rewrite_alias_exprs(&projection_map, &plan.group_expr)?;
                    let mut new_node = PlanNode::AggregatorPartial(AggregatorPartialPlan {
                        group_expr,
                        aggr_expr,
                        input: rewritten_node.input()
                    });
                    new_node.set_input(&rewritten_node)?;
                    rewritten_node = new_node;
                }
                PlanNode::AggregatorFinal(plan) => {
                    let aggr_expr =
                        OptimizerCommon::rewrite_alias_exprs(&projection_map, &plan.aggr_expr)?;
                    let group_expr =
                        OptimizerCommon::rewrite_alias_exprs(&projection_map, &plan.group_expr)?;
                    let mut new_node = PlanNode::AggregatorFinal(AggregatorFinalPlan {
                        group_expr,
                        aggr_expr,
                        input: rewritten_node.input(),
                        schema: plan.schema()
                    });
                    new_node.set_input(&rewritten_node)?;
                    rewritten_node = new_node;
                }
                _ => {
                    let mut clone_node = node.clone();
                    clone_node.set_input(&rewritten_node)?;
                    rewritten_node = clone_node;
                }
            }
            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}
