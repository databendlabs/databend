// Copyright 2021 Datafuse Labs
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

use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;

use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::GetKVReq;
use databend_common_meta_kvapi::kvapi::ListKVReply;
use databend_common_meta_kvapi::kvapi::ListKVReq;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::MGetKVReq;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use log::debug;
use tonic::codegen::BoxStream;
use tonic::Request;

use crate::established_client::EstablishedClient;
use crate::message::ExportReq;
use crate::message::GetClientInfo;
use crate::message::GetClusterStatus;
use crate::message::GetEndpoints;
use crate::message::MakeEstablishedClient;
use crate::message::Streamed;

/// Bind a request type to its corresponding response type.
pub trait RequestFor: Clone + fmt::Debug {
    type Reply;
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, derive_more::From)]
pub enum MetaGrpcReq {
    UpsertKV(UpsertKVReq),

    GetKV(GetKVReq),
    MGetKV(MGetKVReq),
    ListKV(ListKVReq),
}

impl TryInto<MetaGrpcReq> for Request<RaftRequest> {
    type Error = tonic::Status;

    fn try_into(self) -> Result<MetaGrpcReq, Self::Error> {
        let raft_request = self.into_inner();

        let json_str = raft_request.data.as_str();
        let req = serde_json::from_str::<MetaGrpcReq>(json_str)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(req)
    }
}

impl From<MetaGrpcReq> for RaftRequest {
    fn from(v: MetaGrpcReq) -> Self {
        let raft_request = RaftRequest {
            // Safe unwrap(): serialize to string must be ok.
            data: serde_json::to_string(&v).unwrap(),
        };

        debug!(
            req :? =(&raft_request);
            "build raft_request"
        );

        raft_request
    }
}

impl MetaGrpcReq {
    pub fn to_raft_request(&self) -> Result<RaftRequest, InvalidArgument> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(self)
                .map_err(|e| InvalidArgument::new(e, "fail to encode request"))?,
        };

        debug!(
            req :? =(&raft_request);
            "build raft_request"
        );

        Ok(raft_request)
    }
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
pub enum MetaGrpcReadReq {
    GetKV(GetKVReq),
    MGetKV(MGetKVReq),
    ListKV(ListKVReq),
}

// All Read requests returns a stream of KV pairs.
impl RequestFor for MetaGrpcReadReq {
    type Reply = BoxStream<StreamItem>;
}

impl From<MetaGrpcReadReq> for MetaGrpcReq {
    fn from(v: MetaGrpcReadReq) -> Self {
        match v {
            MetaGrpcReadReq::GetKV(v) => MetaGrpcReq::GetKV(v),
            MetaGrpcReadReq::MGetKV(v) => MetaGrpcReq::MGetKV(v),
            MetaGrpcReadReq::ListKV(v) => MetaGrpcReq::ListKV(v),
        }
    }
}

impl From<MetaGrpcReadReq> for RaftRequest {
    fn from(v: MetaGrpcReadReq) -> Self {
        let raft_request = RaftRequest {
            // Safe unwrap(): serialize to string must be ok.
            data: serde_json::to_string(&v).unwrap(),
        };

        debug!(
            req :? =(&raft_request);
            "build raft_request"
        );

        raft_request
    }
}

impl MetaGrpcReadReq {
    pub fn to_raft_request(&self) -> Result<RaftRequest, InvalidArgument> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(self)
                .map_err(|e| InvalidArgument::new(e, "fail to encode request"))?,
        };

        debug!(
            req :? =(&raft_request);
            "build raft_request"
        );

        Ok(raft_request)
    }
}

impl RequestFor for GetKVReq {
    type Reply = GetKVReply;
}

impl RequestFor for MGetKVReq {
    type Reply = MGetKVReply;
}

impl RequestFor for ListKVReq {
    type Reply = ListKVReply;
}

impl RequestFor for Streamed<GetKVReq> {
    type Reply = BoxStream<StreamItem>;
}

impl RequestFor for Streamed<MGetKVReq> {
    type Reply = BoxStream<StreamItem>;
}

impl RequestFor for Streamed<ListKVReq> {
    type Reply = BoxStream<StreamItem>;
}

impl RequestFor for UpsertKVReq {
    type Reply = UpsertKVReply;
}

impl RequestFor for WatchRequest {
    type Reply = tonic::codec::Streaming<WatchResponse>;
}

impl RequestFor for ExportReq {
    type Reply = tonic::codec::Streaming<WatchResponse>;
}

impl RequestFor for MakeEstablishedClient {
    type Reply = EstablishedClient;
}

impl RequestFor for GetEndpoints {
    type Reply = Vec<String>;
}

impl RequestFor for TxnRequest {
    type Reply = TxnReply;
}

impl RequestFor for GetClusterStatus {
    type Reply = ClusterStatus;
}

impl RequestFor for GetClientInfo {
    type Reply = ClientInfo;
}
