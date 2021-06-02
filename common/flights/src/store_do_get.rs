// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Actions for store do_get.
use std::convert::TryInto;

use common_arrow::arrow_flight::Ticket;
use common_planners::Partition;
use common_planners::PlanNode;
use common_planners::ScanPlan;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadAction {
    pub partition: Partition,
    pub push_down: PlanNode,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ScanPartitionsAction {
    pub can_plan: ScanPlan,
}

/// Pull a file. This is used to replicate data between store servers, which is only used internally.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PullAction {
    pub key: String,
}

// Action wrapper for do_get.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum StoreDoGet {
    Read(ReadAction),
    Pull(PullAction),
}

/// Try convert tonic::Request<Ticket> to StoreDoGet.
impl TryInto<StoreDoGet> for tonic::Request<Ticket> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<StoreDoGet, Self::Error> {
        let ticket = self.into_inner();
        let buf = ticket.ticket;

        let action = serde_json::from_slice::<StoreDoGet>(&buf)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        Ok(action)
    }
}

impl From<&StoreDoGet> for tonic::Request<Ticket> {
    fn from(v: &StoreDoGet) -> Self {
        let ticket = serde_json::to_vec(v).unwrap();
        tonic::Request::new(Ticket { ticket })
    }
}
