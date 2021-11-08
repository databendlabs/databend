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

use common_meta_types::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::proto::RaftMes;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::TryInto)]
pub enum AdminRequestInner {
    Join(JoinRequest),
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AdminRequest {
    /// Forward the request to leader if the node received this request is not leader.
    pub forward_to_leader: bool,

    pub req: AdminRequestInner,
}

impl AdminRequest {
    pub fn set_forward(&mut self, allow: bool) {
        self.forward_to_leader = allow;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::TryInto)]
pub enum AdminResponse {
    Join(()),
}

impl tonic::IntoRequest<RaftMes> for AdminRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}
