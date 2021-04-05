// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_planners::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanAction {
    pub job_id: String,
    pub plan: PlanNode,
}

// Action wrapper for do_get.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum ExecuteGetAction {
    ExecutePlan(ExecutePlanAction),
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct FetchPartitionAction {
    pub uuid: String,
    pub nums: u32,
}
