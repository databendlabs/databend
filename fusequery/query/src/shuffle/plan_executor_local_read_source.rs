// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;

use crate::shuffle::ExecutorPlan;

pub struct LocalReadSourceExecutorPlan(pub ReadDataSourcePlan, pub Arc<Box<dyn ExecutorPlan>>);

impl ExecutorPlan for LocalReadSourceExecutorPlan {
    fn get_plan(
        &self,
        executor_name: &str,
        executors: &[Arc<ClusterExecutor>],
    ) -> Result<PlanNode> {
        match executors
            .iter()
            .filter(|executor| executor.name == executor_name && executor.local)
            .count()
        {
            0 => Result::Err(ErrorCode::NotFoundLocalNode(
                "The PlanScheduler must be in the query cluster",
            )),
            _ => Ok(PlanNode::ReadSource(self.0.clone())),
        }
    }
}
