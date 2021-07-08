// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;
use common_planners::RemotePlan;
use common_planners::StageKind;
use common_planners::StagePlan;

use crate::shuffle::ExecutorPlan;

pub struct RemoteExecutorPlan(pub String, pub String, pub StagePlan);

impl RemoteExecutorPlan {
    pub fn create(
        query_id: String,
        stage_id: String,
        plan: &StagePlan,
    ) -> Arc<Box<dyn ExecutorPlan>> {
        Arc::new(Box::new(RemoteExecutorPlan(
            query_id,
            stage_id,
            plan.clone(),
        )))
    }
}

impl ExecutorPlan for RemoteExecutorPlan {
    fn get_plan(
        &self,
        executor_name: &str,
        executors: &[Arc<ClusterExecutor>],
    ) -> Result<PlanNode> {
        match self.2.kind {
            StageKind::Expansive => {
                for executor in executors {
                    if executor.local {
                        return Ok(PlanNode::Remote(RemotePlan {
                            schema: self.2.schema(),
                            fetch_name: format!("{}/{}/{}", self.0, self.1, executor_name),
                            fetch_nodes: vec![executor.name.clone()],
                        }));
                    }
                }

                Err(ErrorCode::NotFoundLocalNode(
                    "The PlanScheduler must be in the query cluster",
                ))
            }
            _ => {
                let executors_name = executors
                    .iter()
                    .map(|node| node.name.clone())
                    .collect::<Vec<_>>();
                Ok(PlanNode::Remote(RemotePlan {
                    schema: self.2.schema(),
                    fetch_name: format!("{}/{}/{}", self.0, self.1, executor_name),
                    fetch_nodes: executors_name,
                }))
            }
        }
    }
}
