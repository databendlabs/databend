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
use std::task::Context;
use std::task::Poll;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio::sync::mpsc;
use common_grpc::GrpcClaim;
use common_grpc::GrpcToken;
use common_meta_api::KVApi;
use common_meta_client::MetaGrpcReq;
use common_meta_types::protobuf::meta_service_server::MetaService;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::HandshakeResponse;
use common_meta_types::protobuf::MemberListReply;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_metrics::counter::WithCount;
use futures::StreamExt;
use prost::Message;
use tokio_stream;
use tokio_stream::Stream;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::info;

use crate::meta_service::meta_service_impl::GrpcStream;
use crate::meta_service::MetaNode;
use crate::metrics::incr_meta_metrics_meta_recv_bytes;
use crate::metrics::incr_meta_metrics_meta_request_result;
use crate::metrics::incr_meta_metrics_meta_sent_bytes;
use crate::metrics::RequestInFlight;
use crate::version::from_digit_ver;
use crate::version::to_digit_ver;
use crate::version::METASRV_SEMVER;
use crate::version::MIN_METACLI_SEMVER;

pub struct MetaServiceImpl {
    token: GrpcToken,
    pub(crate) meta_node: Arc<MetaNode>,
}

impl MetaServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            token: GrpcToken::create(),
            meta_node,
        }
    }

    fn check_token(&self, metadata: &MetadataMap) -> Result<GrpcClaim, Status> {
        let token = metadata
            .get_bin("auth-token-bin")
            .and_then(|v| v.to_bytes().ok())
            .and_then(|b| String::from_utf8(b.to_vec()).ok())
            .ok_or_else(|| Status::unauthenticated("Error auth-token-bin is empty"))?;

        let claim = self.token.try_verify_token(token.clone()).map_err(|e| {
            Status::unauthenticated(format!("token verify failed: {}, {}", token, e))
        })?;
        Ok(claim)
    }

    async fn execute_txn(&self, req: TxnRequest) -> TxnReply {
        let ret = self.meta_node.transaction(req).await;
        incr_meta_metrics_meta_request_result(ret.is_ok());

        match ret {
            Ok(resp) => resp,
            Err(err) => TxnReply {
                success: false,
                error: serde_json::to_string(&err).expect("fail to serialize"),
                responses: vec![],
            },
        }
    }
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    // rpc handshake related type
    type HandshakeStream = GrpcStream<HandshakeResponse>;

    // rpc handshake first
    #[tracing::instrument(level = "debug", skip(self))]
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let req = request
            .into_inner()
            .next()
            .await
            .ok_or_else(|| Status::internal("Error request next is None"))??;

        let HandshakeRequest {
            protocol_version,
            payload,
        } = req;

        let min_compatible = to_digit_ver(&MIN_METACLI_SEMVER);

        // backward compatibility: no version in handshake.
        if protocol_version > 0 && protocol_version < min_compatible {
            return Err(Status::invalid_argument(format!(
                "meta-client protocol_version({}) < metasrv min-compatible({})",
                from_digit_ver(protocol_version),
                MIN_METACLI_SEMVER,
            )));
        }

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
                protocol_version: to_digit_ver(&METASRV_SEMVER),
                payload: token.into_bytes(),
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

    async fn write_msg(&self, r: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        self.kv_api(r).await
    }

    async fn read_msg(&self, r: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        self.kv_api(r).await
    }

    async fn kv_api(&self, r: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let _guard = WithCount::new((), RequestInFlight);

        self.check_token(r.metadata())?;
        common_tracing::extract_remote_span_as_parent(&r);
        incr_meta_metrics_meta_recv_bytes(r.get_ref().encoded_len() as u64);

        let req: MetaGrpcReq = r.try_into()?;
        info!("Received MetaGrpcReq: {:?}", req);

        let m = &self.meta_node;
        let reply = match req {
            MetaGrpcReq::UpsertKV(a) => {
                let res = m.upsert_kv(a).await;
                RaftReply::from(res)
            }
            MetaGrpcReq::GetKV(a) => {
                let res = m.get_kv(&a.key).await;
                RaftReply::from(res)
            }
            MetaGrpcReq::MGetKV(a) => {
                let res = m.mget_kv(&a.keys).await;
                RaftReply::from(res)
            }
            MetaGrpcReq::ListKV(a) => {
                let res = m.prefix_list_kv(&a.prefix).await;
                RaftReply::from(res)
            }
        };

        incr_meta_metrics_meta_request_result(reply.error.is_empty());
        incr_meta_metrics_meta_sent_bytes(reply.encoded_len() as u64);

        Ok(Response::new(reply))
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
        let _guard = WithCount::new((), RequestInFlight);

        let meta_node = &self.meta_node;
        let res = meta_node.sto.export().await?;

        let stream = ExportStream { data: res };
        let s = stream.map(|strings| Ok(ExportedChunk { data: strings }));

        Ok(Response::new(Box::pin(s)))
    }

    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<WatchResponse, tonic::Status>> + Send + Sync + 'static>>;

    #[tracing::instrument(level = "debug", skip(self))]
    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let meta_node = &self.meta_node;
        meta_node.create_watcher_stream(request.into_inner(), tx);

        let output_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::WatchStream))
    }

    async fn transaction(
        &self,
        request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        self.check_token(request.metadata())?;
        incr_meta_metrics_meta_recv_bytes(request.get_ref().encoded_len() as u64);
        let _guard = WithCount::new((), RequestInFlight);

        common_tracing::extract_remote_span_as_parent(&request);

        let request = request.into_inner();

        info!("Receive txn_request: {}", request);

        let body = self.execute_txn(request).await;
        incr_meta_metrics_meta_sent_bytes(body.encoded_len() as u64);

        Ok(Response::new(body))
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.check_token(request.metadata())?;
        let _guard = WithCount::new((), RequestInFlight);

        let meta_node = &self.meta_node;
        let members = meta_node.get_meta_addrs().await.map_err(|e| {
            Status::internal(format!("Cannot get metasrv member list, error: {:?}", e))
        })?;

        let resp = MemberListReply { data: members };
        incr_meta_metrics_meta_sent_bytes(resp.encoded_len() as u64);

        Ok(Response::new(resp))
    }

    async fn get_client_info(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        let _guard = WithCount::new((), RequestInFlight);

        let r = request.remote_addr();
        if let Some(addr) = r {
            let resp = ClientInfo {
                client_addr: addr.to_string(),
            };
            return Ok(Response::new(resp));
        }
        Err(Status::unavailable("can not get client ip address"))
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

        let chunk_size = std::cmp::min(16, l);

        Poll::Ready(Some(self.data.drain(0..chunk_size).collect()))
    }
}
