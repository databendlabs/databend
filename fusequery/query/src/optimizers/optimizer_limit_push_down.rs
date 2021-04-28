// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::{Result, ErrorCodes};
use common_datavalues::DataSchema;
use common_planners::EmptyPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct LimitPushDownOptimizer {}

impl LimitPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        LimitPushDownOptimizer {}
    }
}

fn limit_push_down(upper_limit: Option<usize>, plan: &PlanNode) -> Result<PlanNode> {
    match plan {
        PlanNode::Limit(LimitPlan { n, input }) => {
            let smallest = upper_limit.map(|x| std::cmp::min(x, *n)).unwrap_or(*n);
            Ok(PlanNode::Limit(LimitPlan {
                n: smallest,
                input: Arc::new(limit_push_down(Some(smallest), input.as_ref())?)
            }))
        }
        _ => Ok(plan.clone())
    }
}

impl IOptimizer for LimitPushDownOptimizer {
    fn name(&self) -> &str {
        "LimitPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });

        plan.walk_postorder(|node| {
            if let PlanNode::Limit(LimitPlan { n, input: _ }) = node {
                let mut new_filter_node = limit_push_down(Some(*n), node)?;
                new_filter_node.set_input(&rewritten_node)?;
                rewritten_node = new_filter_node;
            } else {
                let mut clone_node = node.clone();
                clone_node.set_input(&rewritten_node)?;
                rewritten_node = clone_node;
            }
            Ok(true)
        }).map_err(ErrorCodes::from_anyhow)?;

        Ok(rewritten_node)
    }
}
