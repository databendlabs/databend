// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::EmptyPlan;
use common_planners::LimitPlan;
use common_planners::PlanNode;

use crate::optimizers::IOptimizer;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}

impl IOptimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut rewritten_node = PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty())
        });
        Ok(rewritten_node)
    }
}
