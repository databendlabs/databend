// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use common_datavalues::DataSchema;
use common_planners::{EmptyPlan, ProjectionPlan, PlanNode};

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ProjectionPushDownOptimizer {}

impl ProjectionPushDownOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ProjectionPushDownOptimizer {}
    }
}

impl IOptimizer for ProjectionPushDownOptimizer {
    fn name(&self) -> &str {
        "ProjectionPushDown"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty()),
        });

        plan.walk_postorder(|node| {
            if let PlanNode::Projection(ProjectionPlan {expr, schema, input}) = node {
                let mut new_expr = Vec::new();                  
                let mut new_fields = Vec::new();                  
                    

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
