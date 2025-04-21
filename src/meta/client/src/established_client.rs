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

use databend_common_meta_types::protobuf as pb;
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
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use log::error;
use log::info;
use log::warn;
use parking_lot::Mutex;
use tonic::codec::Streaming;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Response;
use tonic::Status;

use crate::endpoints::Endpoints;
use crate::grpc_client::AuthInterceptor;
use crate::grpc_client::RealClient;
use crate::required::Features;

/// Update the client state according to the result of an RPC.
trait HandleRPCResult<T> {
    fn update_client(self, client: &mut EstablishedClient) -> Self;
}

impl<T> HandleRPCResult<T> for Result<Response<T>, Status> {
    fn update_client(self, client: &mut EstablishedClient) -> Result<Response<T>, Status> {
        // - Set the `current` node in endpoints to the leader if the request is forwarded by a follower to a leader.
        // - Store the error if received an error.

        self.inspect(|response| {
            let forwarded_leader = GrpcHelper::get_response_meta_leader(response);

            // `leader` is set iff the request is forwarded by a follower to a leader
            if let Some(leader) = forwarded_leader {
                info!(
                    "EstablishedClient update_client: received forward_to_leader({}) for further RPC, endpoints: {}",
                    leader,
                    &*client.endpoints.lock(),
                );

                let update_leader_res = {
                    let mut endpoints = client.endpoints.lock();
                    let set_res = endpoints.set_current(Some(leader.to_string()));

                    if let Err(ref e) = set_res {
                        error!("fail to update leader: {:?}; endpoints: {}", e, endpoints);
                    }

                    set_res
                };

                info!(
                    "EstablishedClient update_client: switch to use leader({}) for further RPC, result: {:?}",
                    leader, update_leader_res,
                );
            }
        })
        .inspect_err(|status| {
            warn!("EstablishedClient update_client: set received error: {:?}", status);
            client.set_error(status.clone());
        })
    }
}

/// A gRPC client that has established a connection to a server and passed handshake.
#[derive(Debug, Clone)]
pub struct EstablishedClient {
    client: RealClient,

    server_protocol_version: u64,

    features: Features,

    /// The target endpoint this client connected to.
    ///
    /// Note that `target_endpoint` may be different from the `self.endpoints.current()`,
    /// which is used for in future connections and may have been updated by other thread.
    target_endpoint: String,

    /// The endpoints shared in a client pool.
    endpoints: Arc<Mutex<Endpoints>>,

    /// The error that occurred when sending an RPC.
    ///
    /// The client with error will be dropped by the client pool.
    error: Arc<Mutex<Option<Status>>>,
}

impl EstablishedClient {
    pub(crate) fn new(
        client: MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        server_protocol_version: u64,
        features: Features,
        target_endpoint: impl ToString,
        endpoints: Arc<Mutex<Endpoints>>,
    ) -> Self {
        Self {
            client,
            server_protocol_version,
            features,
            target_endpoint: target_endpoint.to_string(),
            endpoints,
            error: Arc::new(Mutex::new(None)),
        }
    }

    pub fn target_endpoint(&self) -> &str {
        &self.target_endpoint
    }

    pub fn endpoints(&self) -> &Arc<Mutex<Endpoints>> {
        &self.endpoints
    }

    pub fn server_protocol_version(&self) -> u64 {
        self.server_protocol_version
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.contains_key(feature)
    }

    pub fn ensure_feature(&self, feature: &str) -> Result<(), Status> {
        if self.has_feature(feature) {
            Ok(())
        } else {
            Err(Status::failed_precondition(format!(
                "Feature {} is not supported by the server; server:{{version: {}, features: {:?}}}",
                feature, self.server_protocol_version, self.features
            )))
        }
    }

    pub(crate) fn set_error(&self, error: Status) {
        *self.error.lock() = Some(error);
    }

    pub(crate) fn take_error(&self) -> Option<Status> {
        self.error.lock().take()
    }

    #[async_backtrace::framed]
    pub async fn kv_api(
        &mut self,
        request: impl tonic::IntoRequest<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        self.client.kv_api(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn kv_read_v1(
        &mut self,
        request: impl tonic::IntoRequest<RaftRequest>,
    ) -> Result<Response<Streaming<StreamItem>>, Status> {
        let resp = self.client.kv_read_v1(request).await;
        resp.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn export(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<Streaming<ExportedChunk>>, Status> {
        self.client.export(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn export_v1(
        &mut self,
        request: impl tonic::IntoRequest<pb::ExportRequest>,
    ) -> Result<Response<Streaming<ExportedChunk>>, Status> {
        self.client.export_v1(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn watch(
        &mut self,
        request: impl tonic::IntoRequest<WatchRequest>,
    ) -> Result<Response<Streaming<WatchResponse>>, Status> {
        self.client.watch(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn transaction(
        &mut self,
        request: impl tonic::IntoRequest<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        self.client.transaction(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn member_list(
        &mut self,
        request: impl tonic::IntoRequest<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.client.member_list(request).await.update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn get_cluster_status(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        self.client
            .get_cluster_status(request)
            .await
            .update_client(self)
    }

    #[async_backtrace::framed]
    pub async fn get_client_info(
        &mut self,
        request: impl tonic::IntoRequest<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        self.client
            .get_client_info(request)
            .await
            .update_client(self)
    }
}
