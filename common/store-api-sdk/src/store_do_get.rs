// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// Actions for store do_get.
use std::convert::TryInto;

use common_arrow::arrow_flight::Ticket;
use common_dfs_api_vo::ReadAction;
use common_planners::ScanPlan;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ScanPartitionsAction {
    pub scan_plan: ScanPlan,
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
