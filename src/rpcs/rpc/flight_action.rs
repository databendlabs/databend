// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::planners::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanAction {
    pub(crate) job_id: String,
    pub(crate) plan: PlanNode,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct FetchPartitionAction {
    pub(crate) job_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum ExecuteAction {
    ExecutePlan(ExecutePlanAction),
    FetchPartition(FetchPartitionAction),
}

impl ExecutePlanAction {
    pub fn create(job_id: String, plan: PlanNode) -> Self {
        Self { job_id, plan }
    }
}
