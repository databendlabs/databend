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

use crate::optimizers::IOptimizer;
use crate::optimizers::OptimizerCommon;
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
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        let projection_map = OptimizerCommon::projection_to_map(plan)?;
        plan.walk_postorder(|node| -> Result<bool> {
            match node {
                PlanNode::Filter(plan) => {
                    let rewritten_expr =
                        OptimizerCommon::rewrite_alias_expr(&projection_map, &plan.predicate)?;
                    let mut new_node = PlanNode::Filter(FilterPlan {
                        predicate: rewritten_expr,
                        input: rewritten_node.input()
                    });
                    new_node.set_input(&rewritten_node)?;
                    rewritten_node = new_node;
                }
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
