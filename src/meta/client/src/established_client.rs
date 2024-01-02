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

use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::Empty;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use tonic::codec::Streaming;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Response;
use tonic::Status;

use crate::grpc_client::AuthInterceptor;

/// A gRPC client that has established a connection to a server and passed handshake.
#[derive(Debug, Clone)]
pub struct EstablishedClient {
    client: MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,

    server_protocol_version: u64,

    /// The error that occurred when sending an RPC.
    ///
    /// The client with error will be dropped by the client pool.
    error: Arc<Mutex<Option<Status>>>,
}

impl EstablishedClient {
    pub(crate) fn new(
        client: MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        server_protocol_version: u64,
    ) -> Self {
        Self {
            client,
            server_protocol_version,
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn server_protocol_version(&self) -> u64 {
        self.server_protocol_version
    }

    pub(crate) fn set_error(&self, error: Status) {
        *self.error.lock().unwrap() = Some(error);
    }

    pub(crate) fn take_error(&self) -> Option<Status> {
        self.error.lock().unwrap().take()
    }

    pub async fn kv_api(
        &mut self,
        request: impl tonic::IntoRequest<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        self.client.kv_api(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn kv_read_v1(
        &mut self,
        request: impl tonic::IntoRequest<RaftRequest>,
    ) -> Result<Response<Streaming<StreamItem>>, Status> {
        self.client.kv_read_v1(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn export(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<Streaming<ExportedChunk>>, Status> {
        self.client.export(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn watch(
        &mut self,
        request: impl tonic::IntoRequest<WatchRequest>,
    ) -> Result<Response<Streaming<WatchResponse>>, Status> {
        self.client.watch(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn transaction(
        &mut self,
        request: impl tonic::IntoRequest<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        self.client.transaction(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn member_list(
        &mut self,
        request: impl tonic::IntoRequest<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.client.member_list(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn get_cluster_status(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        self.client.get_cluster_status(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }

    pub async fn get_client_info(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        self.client.get_client_info(request).await.map_err(|e| {
            self.set_error(e.clone());
            e
        })
    }
}
