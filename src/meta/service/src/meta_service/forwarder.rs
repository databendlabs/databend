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

use common_meta_api::reply::reply_to_api_result;
use common_meta_client::MetaGrpcReadReq;
use common_meta_types::protobuf::raft_service_client::RaftServiceClient;
use common_meta_types::protobuf::StreamItem;
use common_meta_types::ConnectionError;
use common_meta_types::Endpoint;
use common_meta_types::ForwardRPCError;
use common_meta_types::GrpcConfig;
use common_meta_types::MetaAPIError;
use common_meta_types::MetaNetworkError;
use common_meta_types::NodeId;
use log::debug;
use tonic::codegen::BoxStream;
use tonic::transport::Channel;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::message::ForwardResponse;
use crate::meta_service::raftmeta::MetaRaft;
use crate::meta_service::MetaNode;
use crate::request_handling::Forwarder;
use crate::store::RaftStore;

/// Handle a request locally if it is leader. Otherwise, forward it to the leader.
pub struct MetaForwarder<'a> {
    sto: &'a RaftStore,
    #[allow(dead_code)]
    raft: &'a MetaRaft,
}

impl<'a> MetaForwarder<'a> {
    pub fn new(meta_node: &'a MetaNode) -> Self {
        Self {
            sto: &meta_node.sto,
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
            .get_node_endpoint(target)
            .await
            .map_err(|e| MetaNetworkError::GetNodeAddrError(e.to_string()))?;

        let client = RaftServiceClient::connect(format!("http://{}", endpoint))
            .await
            .map_err(|e| {
                let conn_err = ConnectionError::new(e, format!("address: {}", endpoint));
                MetaNetworkError::ConnectionError(conn_err)
            })?;

        let client = client
            .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
            .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        Ok((endpoint, client))
    }
}

#[async_trait::async_trait]
impl<'a> Forwarder<ForwardRequestBody> for MetaForwarder<'a> {
    #[minitrace::trace]
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest<ForwardRequestBody>,
    ) -> Result<ForwardResponse, ForwardRPCError> {
        debug!("forward ForwardRequest to: {} {:?}", target, req);

        let (endpoint, mut client) = self.new_raft_client(&target).await?;

        let resp = client.forward(req).await.map_err(|e| {
            MetaNetworkError::from(e)
                .add_context(format!("target: {}, endpoint: {}", target, endpoint))
        })?;
        let raft_mes = resp.into_inner();

        let res: Result<ForwardResponse, MetaAPIError> = reply_to_api_result(raft_mes);
        let resp = res?;
        Ok(resp)
    }
}

#[async_trait::async_trait]
impl<'a> Forwarder<MetaGrpcReadReq> for MetaForwarder<'a> {
    #[minitrace::trace]
    async fn forward(
        &self,
        target: NodeId,
        req: ForwardRequest<MetaGrpcReadReq>,
    ) -> Result<BoxStream<StreamItem>, ForwardRPCError> {
        debug!("forward ReadRequest to: {} {:?}", target, req);

        let (endpoint, mut client) = self.new_raft_client(&target).await?;

        let strm = client.kv_read_v1(req).await.map_err(|e| {
            MetaNetworkError::from(e)
                .add_context(format!("target: {}, endpoint: {}", target, endpoint))
        })?;

        let strm = strm.into_inner();
        Ok(Box::pin(strm))
    }
}
