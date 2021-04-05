// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;

use common_planners::PlanNode;

use crate::protobuf::FlightRequest;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanAction {
    pub job_id: String,
    pub plan: PlanNode,
}

// Action wrapper for do_get.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum DoGetAction {
    ExecutePlan(ExecutePlanAction),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct FetchPartitionAction {
    pub uuid: String,
    pub nums: u32,
}

// Action wrapper for do_action.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum DoActionAction {
    FetchPartition(FetchPartitionAction),
}

impl TryInto<DoGetAction> for FlightRequest {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<DoGetAction, Self::Error> {
        todo!()
    }
}

impl TryInto<DoActionAction> for FlightRequest {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<DoActionAction, Self::Error> {
        todo!()
    }
}
