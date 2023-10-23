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
use std::fmt::Debug;

use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::GetKVReq;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_kvapi::kvapi::ListKVReq;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::MGetKVReq;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ClusterStatus;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::StreamItem;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::InvalidArgument;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use log::as_debug;
use log::debug;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Streaming;

use crate::grpc_client::AuthInterceptor;
use crate::message::ExportReq;
use crate::message::GetClientInfo;
use crate::message::GetClusterStatus;
use crate::message::GetEndpoints;
use crate::message::MakeClient;
use crate::message::Streamed;

/// Bind a request type to its corresponding response type.
pub trait RequestFor {
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

impl MetaGrpcReq {
    pub fn to_raft_request(&self) -> Result<RaftRequest, InvalidArgument> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(self)
                .map_err(|e| InvalidArgument::new(e, "fail to encode request"))?,
        };

        debug!(
            req = as_debug!(&raft_request);
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

impl MetaGrpcReadReq {
    pub fn to_raft_request(&self) -> Result<RaftRequest, InvalidArgument> {
        let raft_request = RaftRequest {
            data: serde_json::to_string(self)
                .map_err(|e| InvalidArgument::new(e, "fail to encode request"))?,
        };

        debug!(
            req = as_debug!(&raft_request);
            "build raft_request"
        );

        Ok(raft_request)
    }
}

impl RequestFor for GetKVReq {
    type Reply = GetKVReply;
}

impl RequestFor for Streamed<GetKVReq> {
    type Reply = Streaming<StreamItem>;
}

impl RequestFor for MGetKVReq {
    type Reply = MGetKVReply;
}

impl RequestFor for Streamed<MGetKVReq> {
    type Reply = Streaming<StreamItem>;
}

impl RequestFor for ListKVReq {
    type Reply = ListKVReply;
}

impl RequestFor for Streamed<ListKVReq> {
    type Reply = Streaming<StreamItem>;
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

impl RequestFor for MakeClient {
    type Reply = MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>;
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
