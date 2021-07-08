// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;

use crate::shuffle::ExecutorPlan;

pub struct DefaultExecutorPlan(pub PlanNode, pub Arc<Box<dyn ExecutorPlan>>);

impl ExecutorPlan for DefaultExecutorPlan {
    fn get_plan(&self, node_name: &str, executors: &[Arc<ClusterExecutor>]) -> Result<PlanNode> {
        let mut clone_node = self.0.clone();
        clone_node.set_inputs(vec![&self.1.get_plan(node_name, executors)?])?;
        Ok(clone_node)
    }
}
