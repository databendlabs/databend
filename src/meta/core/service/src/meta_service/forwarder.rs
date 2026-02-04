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

//! Forward request to another node

use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_types::ConnectionError;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::ForwardRPCError;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use databend_common_meta_types::raft_types::NodeId;
use log::debug;
use tonic::codegen::BoxStream;
use tonic::transport::Channel;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::meta_node::meta_node::MetaRaft;
use crate::meta_service::MetaNode;
use crate::request_handling::Forwarder;
use crate::store::RaftStore;
use crate::util::reply_to_api_result;

/// Handle a request locally if it is leader. Otherwise, forward it to the leader.
pub struct MetaForwarder<'a, SP> {
    sto: &'a RaftStore<SP>,
    #[allow(dead_code)]
    raft: &'a MetaRaft,
}

impl<'a, SP: SpawnApi> MetaForwarder<'a, SP> {
    pub fn new(meta_node: &'a MetaNode<SP>) -> Self {
        Self {
            sto: &meta_node.raft_store,
            raft: &meta_node.raft,
        }
    }

    async fn new_raft_client(
        &self,
        target: &NodeId,
    ) -> Result<(Endpoint, RaftServiceClient<Channel>), MetaNetworkError> {
        debug!("new RaftServiceClient to: {}", target);

        let endpoint = self
            .sto
            .get_node_raft_endpoint(target)
            .await
            .ok_or_else(|| {
                MetaNetworkError::GetNodeAddrError(format!(
                    "Node {} not found in state machine",
                    target
                ))
            })?;

        let client = RaftServiceClient::connect(format!("http://{}", endpoint))
            .await
            .map_err(|e| {
                let conn_err = ConnectionError::new(e, format!("address: {}", endpoint));
                MetaNetworkError::ConnectionError(conn_err)
            })?;

        let max_msg_size = self.sto.config.raft_grpc_max_message_size();
        let client = client
            .max_decoding_message_size(max_msg_size)
            .max_encoding_message_size(max_msg_size);

        Ok((endpoint, client))
    }
}

#[async_trait::async_trait]
impl<SP: SpawnApi> Forwarder<ForwardRequestBody> for MetaForwarder<'_, SP> {
    #[fastrace::trace]
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest<ForwardRequestBody>,
    ) -> Result<(Endpoint, ForwardResponse), ForwardRPCError> {
        debug!("forward ForwardRequest to: {} {:?}", target, req);

        let (endpoint, mut client) = self.new_raft_client(&target).await?;

        let resp = client.forward(req).await.map_err(|e| {
            MetaNetworkError::from(e)
                .add_context(format!("target: {}, endpoint: {}", target, endpoint))
        })?;
        let raft_mes = resp.into_inner();

        let res: Result<ForwardResponse, MetaAPIError> = reply_to_api_result(raft_mes);
        let resp = res?;

        Ok((endpoint, resp))
    }
}

#[async_trait::async_trait]
impl<SP: SpawnApi> Forwarder<MetaGrpcReadReq> for MetaForwarder<'_, SP> {
    #[fastrace::trace]
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest<MetaGrpcReadReq>,
    ) -> Result<(Endpoint, BoxStream<StreamItem>), ForwardRPCError> {
        debug!("forward ReadRequest to: {} {:?}", target, req);

        let (endpoint, mut client) = self.new_raft_client(&target).await?;

        let strm = client.kv_read_v1(req).await.map_err(|e| {
            MetaNetworkError::from(e)
                .add_context(format!("target: {}, endpoint: {}", target, endpoint))
        })?;

        let strm = strm.into_inner();
        Ok((endpoint, Box::pin(strm)))
    }
}
