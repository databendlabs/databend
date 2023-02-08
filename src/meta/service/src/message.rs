// Copyright 2023 Datafuse Labs.
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

use common_meta_raft_store::applied_state::AppliedState;
use common_meta_sled_store::openraft::NodeId;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::Endpoint;
use common_meta_types::GetKVReply;
use common_meta_types::GetKVReq;
use common_meta_types::ListKVReply;
use common_meta_types::ListKVReq;
use common_meta_types::LogEntry;
use common_meta_types::MGetKVReply;
use common_meta_types::MGetKVReq;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub endpoint: Endpoint,

    #[serde(skip)]
    #[deprecated(note = "it is listening addr, not advertise addr")]
    pub grpc_api_addr: String,

    pub grpc_api_advertise_address: Option<String>,
}

impl JoinRequest {
    pub fn new(
        node_id: NodeId,
        endpoint: Endpoint,
        grpc_api_advertise_address: Option<impl ToString>,
    ) -> Self {
        Self {
            node_id,
            endpoint,
            grpc_api_advertise_address: grpc_api_advertise_address.map(|x| x.to_string()),
            ..Default::default()
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LeaveRequest {
    pub node_id: NodeId,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    derive_more::From,
    derive_more::TryInto,
)]
pub enum ForwardRequestBody {
    Ping,

    Join(JoinRequest),
    Leave(LeaveRequest),

    Write(LogEntry),

    GetKV(GetKVReq),
    MGetKV(MGetKVReq),
    ListKV(ListKVReq),
}

/// A request that is forwarded from one raft node to another
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ForwardRequest {
    /// Forward the request to leader if the node received this request is not leader.
    pub forward_to_leader: u64,

    pub body: ForwardRequestBody,
}

impl ForwardRequest {
    pub fn decr_forward(&mut self) {
        self.forward_to_leader -= 1;
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, derive_more::TryInto,
)]
#[allow(clippy::large_enum_variant)]
pub enum ForwardResponse {
    #[try_into(ignore)]
    Pong,

    Join(()),
    Leave(()),
    AppliedState(AppliedState),

    GetKV(GetKVReply),
    MGetKV(MGetKVReply),
    ListKV(ListKVReply),
}

impl tonic::IntoRequest<RaftRequest> for ForwardRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for ForwardRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}
