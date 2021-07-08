// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;

use crate::shuffle::ExecutorPlan;

pub struct RemoteReadSourceExecutorPlan(
    pub ReadDataSourcePlan,
    pub Arc<HashMap<String, Partitions>>,
    pub Arc<Box<dyn ExecutorPlan>>,
);

impl ExecutorPlan for RemoteReadSourceExecutorPlan {
    fn get_plan(&self, executor_name: &str, _: &[Arc<ClusterExecutor>]) -> Result<PlanNode> {
        let partitions = self
            .1
            .get(executor_name)
            .map(Clone::clone)
            .unwrap_or_default();
        Ok(PlanNode::ReadSource(ReadDataSourcePlan {
            db: self.0.db.clone(),
            table: self.0.table.clone(),
            schema: self.0.schema.clone(),
            parts: partitions,
            statistics: self.0.statistics.clone(),
            description: self.0.description.clone(),
            scan_plan: self.0.scan_plan.clone(),
            remote: self.0.remote,
        }))
    }
}
