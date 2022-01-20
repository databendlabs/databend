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

use openraft::raft::AppendEntriesRequest;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::VoteRequest;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::protobuf::RaftReply;
use crate::protobuf::RaftRequest;
use crate::AppliedState;
use crate::DatabaseInfo;
use crate::GetDatabaseReq;
use crate::GetKVActionReply;
use crate::GetKVReq;
use crate::GetTableReq;
use crate::ListDatabaseReq;
use crate::ListKVReq;
use crate::ListTableReq;
use crate::LogEntry;
use crate::MGetKVActionReply;
use crate::MGetKVReq;
use crate::NodeId;
use crate::PrefixListReply;
use crate::TableInfo;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(
    Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::From, derive_more::TryInto,
)]
pub enum ForwardRequestBody {
    Join(JoinRequest),
    Write(LogEntry),

    ListDatabase(ListDatabaseReq),
    GetDatabase(GetDatabaseReq),
    ListTable(ListTableReq),
    GetTable(GetTableReq),

    GetKV(GetKVReq),
    MGetKV(MGetKVReq),
    ListKV(ListKVReq),
}

/// A request that is forwarded from one raft node to another
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, derive_more::TryInto)]
#[allow(clippy::large_enum_variant)]
pub enum ForwardResponse {
    Join(()),
    AppliedState(AppliedState),
    ListDatabase(Vec<Arc<DatabaseInfo>>),
    DatabaseInfo(Arc<DatabaseInfo>),
    ListTable(Vec<Arc<TableInfo>>),
    TableInfo(Arc<TableInfo>),

    GetKV(GetKVActionReply),
    MGetKV(MGetKVActionReply),
    ListKV(PrefixListReply),
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
