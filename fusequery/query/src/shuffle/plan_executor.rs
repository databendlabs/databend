// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;

pub trait ExecutorPlan {
    fn get_plan(&self, node_name: &str, executors: &[Arc<ClusterExecutor>]) -> Result<PlanNode>;
}
