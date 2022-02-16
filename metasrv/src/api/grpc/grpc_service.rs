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

use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::task::Context;
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::task::Poll;
use common_grpc::GrpcClaim;
use common_grpc::GrpcToken;
use common_meta_grpc::MetaGrpcReadReq;
use common_meta_grpc::MetaGrpcWriteReq;
use common_meta_types::protobuf::meta_service_server::MetaService;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::HandshakeResponse;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_tracing::tracing;
use futures::StreamExt;
use prost::Message;
use tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::executor::ActionHandler;
use crate::meta_service::meta_service_impl::GrpcStream;
use crate::meta_service::MetaNode;

pub struct MetaServiceImpl {
    token: GrpcToken,
    action_handler: ActionHandler,
}

impl MetaServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            token: GrpcToken::create(),
            action_handler: ActionHandler::create(meta_node),
        }
    }

    fn check_token(&self, metadata: &MetadataMap) -> Result<GrpcClaim, Status> {
        let token = metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .ok_or_else(|| Status::unauthenticated("Error auth-token-bin is empty"))?;

        let claim = self
            .token
            .try_verify_token(token)
            .map_err(|e| Status::unauthenticated(e.to_string()))?;
        Ok(claim)
    }
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    // rpc handshake related type
    type HandshakeStream = GrpcStream<HandshakeResponse>;

    // rpc handshake first
    #[tracing::instrument(level = "info", skip(self))]
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let req = request
            .into_inner()
            .next()
            .await
            .ok_or_else(|| Status::internal("Error request next is None"))??;

        let HandshakeRequest { payload, .. } = req;
        let auth = BasicAuth::decode(&*payload).map_err(|e| Status::internal(e.to_string()))?;

        let user = "root";
        if auth.username == user {
            let claim = GrpcClaim {
                username: user.to_string(),
            };
            let token = self
                .token
                .try_create_token(claim)
                .map_err(|e| Status::internal(e.to_string()))?;

            let resp = HandshakeResponse {
                payload: token.into_bytes(),
                ..HandshakeResponse::default()
            };
            let output = futures::stream::once(async { Ok(resp) });
            Ok(Response::new(Box::pin(output)))
        } else {
            Err(Status::unauthenticated(format!(
                "Unknown user: {}",
                auth.username
            )))
        }
    }

    async fn write_msg(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        self.check_token(request.metadata())?;
        common_tracing::extract_remote_span_as_parent(&request);

        let action: MetaGrpcWriteReq = request.try_into()?;
        tracing::info!("Receive write_action: {:?}", action);

        let body = self.action_handler.execute_write(action).await;
        Ok(Response::new(body))
    }

    async fn read_msg(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        self.check_token(request.metadata())?;
        common_tracing::extract_remote_span_as_parent(&request);

        let action: MetaGrpcReadReq = request.try_into()?;
        tracing::info!("Receive read_action: {:?}", action);

        let res = self.action_handler.execute_read(action).await;

        Ok(Response::new(res))
    }

    type ExportStream =
        Pin<Box<dyn Stream<Item = Result<ExportedChunk, tonic::Status>> + Send + Sync + 'static>>;

    // Export all meta data.
    //
    // Including raft hard state, logs and state machine.
    // The exported data is a list of json strings in form of `(tree_name, sub_tree_prefix, key, value)`.
    async fn export(
        &self,
        _request: Request<common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let meta_node = &self.action_handler.meta_node;

        let res = meta_node.sto.export().await?;

        let stream = ExportStream { data: res };

        let s = stream.map(|strings| Ok(ExportedChunk { data: strings }));

        Ok(Response::new(Box::pin(s)))
    }
}

pub struct ExportStream {
    pub data: Vec<String>,
}

impl Stream for ExportStream {
    type Item = Vec<String>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let l = self.data.len();

        if l == 0 {
            return Poll::Ready(None);
        }

        let chunk_size = 16;

        Poll::Ready(Some(self.data.drain(0..chunk_size).collect()))
    }
}
