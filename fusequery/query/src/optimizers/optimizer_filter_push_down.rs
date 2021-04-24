// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataSchema;
use common_planners::EmptyPlan;
use common_planners::FilterPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::optimizers::OptimizerCommon;
use crate::sessions::FuseQueryContextRef;

pub struct FilterPushDownOptimizer {}

impl FilterPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        FilterPushDownOptimizer {}
    }
}

impl IOptimizer for FilterPushDownOptimizer {
    fn name(&self) -> &str {
        "FilterPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        let projection_map = OptimizerCommon::projection_to_map(plan)?;
        plan.walk_postorder(|node| {
            if let PlanNode::Filter(filter) = node {
                let rewritten_expr =
                    OptimizerCommon::rewrite_alias_expr(&filter.predicate, &projection_map)?;
                let mut new_filter_node = PlanNode::Filter(FilterPlan {
                    predicate: rewritten_expr,
                    input: rewritten_node.input()
                });
                new_filter_node.set_input(&rewritten_node)?;
                rewritten_node = new_filter_node;
            } else {
                let mut clone_node = node.clone();
                clone_node.set_input(&rewritten_node)?;
                rewritten_node = clone_node;
            }
            Ok(true)
        })?;

        Ok(rewritten_node)
    }
}
