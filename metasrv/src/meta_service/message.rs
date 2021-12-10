// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use async_raft::raft::AppendEntriesRequest;
use async_raft::raft::InstallSnapshotRequest;
use async_raft::raft::VoteRequest;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_types::DatabaseInfo;
use common_meta_types::GetDatabaseReq;
use common_meta_types::LogEntry;
use common_meta_types::NodeId;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::errors::RetryableError;
use crate::proto::RaftReply;
use crate::proto::RaftRequest;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::TryInto)]
pub enum AdminRequestInner {
    Join(JoinRequest),
    Write(LogEntry),

    GetDatabase(GetDatabaseReq),
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
#[allow(clippy::large_enum_variant)]
pub enum AdminResponse {
    Join(()),
    AppliedState(AppliedState),

    DatabaseInfo(Arc<DatabaseInfo>),
}

impl tonic::IntoRequest<RaftRequest> for AdminRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for AdminRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}

impl tonic::IntoRequest<RaftRequest> for LogEntry {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftRequest> for LogEntry {
    type Error = tonic::Status;

    fn try_from(mes: RaftRequest) -> Result<Self, Self::Error> {
        let req: LogEntry = serde_json::from_str(&mes.data)
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        Ok(req)
    }
}

impl tonic::IntoRequest<RaftRequest> for AppendEntriesRequest<LogEntry> {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl tonic::IntoRequest<RaftRequest> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftRequest> {
        let mes = RaftRequest {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl From<RetryableError> for RaftReply {
    fn from(err: RetryableError) -> Self {
        let error = serde_json::to_string(&err).expect("fail to serialize");
        RaftReply {
            data: "".to_string(),
            error,
        }
    }
}

impl From<AppliedState> for RaftReply {
    fn from(msg: AppliedState) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftReply {
            data,
            error: "".to_string(),
        }
    }
}

impl<T, E> From<RaftReply> for Result<T, E>
where
    T: DeserializeOwned,
    E: DeserializeOwned,
{
    fn from(msg: RaftReply) -> Self {
        if !msg.data.is_empty() {
            let resp: T = serde_json::from_str(&msg.data).expect("fail to deserialize");
            Ok(resp)
        } else {
            let err: E = serde_json::from_str(&msg.error).expect("fail to deserialize");
            Err(err)
        }
    }
}

impl<T, E> From<Result<T, E>> for RaftReply
where
    T: Serialize,
    E: Serialize,
{
    fn from(r: Result<T, E>) -> Self {
        match r {
            Ok(x) => {
                let data = serde_json::to_string(&x).expect("fail to serialize");
                RaftReply {
                    data,
                    error: Default::default(),
                }
            }
            Err(e) => {
                let error = serde_json::to_string(&e).expect("fail to serialize");
                RaftReply {
                    data: Default::default(),
                    error,
                }
            }
        }
    }
}
