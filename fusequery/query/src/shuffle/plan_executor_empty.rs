// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::EmptyPlan;
use common_planners::PlanNode;

use crate::shuffle::ExecutorPlan;

pub struct EmptyExecutorPlan;

impl ExecutorPlan for EmptyExecutorPlan {
    fn get_plan(&self, _node_name: &str, _executors: &[Arc<ClusterExecutor>]) -> Result<PlanNode> {
        Ok(PlanNode::Empty(EmptyPlan {
            schema: Arc::new(DataSchema::empty()),
        }))
    }
}
