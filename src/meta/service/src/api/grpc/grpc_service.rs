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

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::time::Instant;
use common_grpc::GrpcClaim;
use common_grpc::GrpcToken;
use common_meta_client::MetaGrpcReadReq;
use common_meta_client::MetaGrpcReq;
use common_meta_kvapi::kvapi::KVApi;
use common_meta_types::protobuf::meta_service_server::MetaService;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::ClusterStatus;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::HandshakeResponse;
use common_meta_types::protobuf::MemberListReply;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::StreamItem;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_metrics::counter::Count;
use common_tracing::func_name;
use futures::stream::TryChunksError;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::info;
use minitrace::prelude::*;
use prost::Message;
use tokio_stream;
use tokio_stream::Stream;
use tonic::codegen::BoxStream;
use tonic::metadata::MetadataMap;
use tonic::transport::NamedService;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::grpc_helper::GrpcHelper;
use crate::message::ForwardRequest;
use crate::meta_service::MetaNode;
use crate::metrics::network_metrics;
use crate::metrics::RequestInFlight;
use crate::version::from_digit_ver;
use crate::version::to_digit_ver;
use crate::version::METASRV_SEMVER;
use crate::version::MIN_METACLI_SEMVER;
use crate::watcher::WatchStream;

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

    #[minitrace::trace]
    async fn handle_kv_api(&self, request: Request<RaftRequest>) -> Result<RaftReply, Status> {
        let req: MetaGrpcReq = request.try_into()?;
        info!("{}: Received MetaGrpcReq: {:?}", func_name!(), req);

        let t0 = Instant::now();

        let m = &self.meta_node;
        let reply = match &req {
            MetaGrpcReq::UpsertKV(a) => {
                let res = m.upsert_kv(a.clone()).await;
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
        let elapsed = t0.elapsed();
        info!("Handled(elapsed: {:?}) MetaGrpcReq: {:?}", elapsed, req);

        network_metrics::incr_request_result(reply.error.is_empty());

        Ok(reply)
    }

    #[minitrace::trace]
    async fn handle_kv_read_v1(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<BoxStream<StreamItem>, Status> {
        let req: MetaGrpcReadReq = GrpcHelper::parse_req(request)?;

        info!("{}: Received ReadRequest: {:?}", func_name!(), req);

        let req = ForwardRequest {
            forward_to_leader: 1,
            body: req,
        };

        let t0 = Instant::now();

        let res = self
            .meta_node
            .handle_forwardable_request::<MetaGrpcReadReq>(req.clone())
            .await
            .map_err(GrpcHelper::internal_err);

        let elapsed = t0.elapsed();
        info!("Handled(elapsed: {:?}) ReadRequest: {:?}", elapsed, req);

        network_metrics::incr_request_result(res.is_ok());
        res
    }

    #[minitrace::trace]
    async fn handle_txn(&self, request: Request<TxnRequest>) -> Result<TxnReply, Status> {
        let request = request.into_inner();

        info!("{}: Receive txn_request: {}", func_name!(), request);

        let ret = self.meta_node.transaction(request).await;

        let body = match ret {
            Ok(resp) => TxnReply {
                success: resp.success,
                error: "".to_string(),
                responses: resp.responses,
            },
            Err(err) => TxnReply {
                success: false,
                error: serde_json::to_string(&err).expect("fail to serialize"),
                responses: vec![],
            },
        };

        network_metrics::incr_request_result(body.error.is_empty());

        Ok(body)
    }
}

impl NamedService for MetaServiceImpl {
    const NAME: &'static str = "meta_service";
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    type HandshakeStream = BoxStream<HandshakeResponse>;

    // rpc handshake first
    #[minitrace::trace]
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

        debug!("handle handshake request, client ver: {}", protocol_version);

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

            debug!("handshake OK");
            Ok(Response::new(Box::pin(output)))
        } else {
            Err(Status::unauthenticated(format!(
                "Unknown user: {}",
                auth.username
            )))
        }
    }

    async fn kv_api(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        self.check_token(request.metadata())?;

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
        let _guard = RequestInFlight::guard();

        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);
        let reply = self.handle_kv_api(request).in_span(root).await?;

        network_metrics::incr_sent_bytes(reply.encoded_len() as u64);

        Ok(Response::new(reply))
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        self.check_token(request.metadata())?;

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);

        let strm = self.handle_kv_read_v1(request).in_span(root).await?;

        Ok(Response::new(strm))
    }

    async fn transaction(
        &self,
        request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        self.check_token(request.metadata())?;

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
        let _guard = RequestInFlight::guard();

        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);
        let reply = self.handle_txn(request).in_span(root).await?;

        network_metrics::incr_sent_bytes(reply.encoded_len() as u64);

        Ok(Response::new(reply))
    }

    type ExportStream = Pin<Box<dyn Stream<Item = Result<ExportedChunk, Status>> + Send + 'static>>;

    /// Export all meta data.
    ///
    /// Including header, raft state, logs and state machine.
    /// The exported data is a series of JSON encoded strings of `RaftStoreEntry`.
    async fn export(
        &self,
        _request: Request<common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let _guard = RequestInFlight::guard();

        let meta_node = &self.meta_node;
        let strm = meta_node.sto.inner().export();

        let chunk_size = 32;
        // - Chunk up upto 32 Ok items inside a Vec<String>;
        // - Convert Vec<String> to ExportedChunk;
        // - Convert TryChunkError<_, io::Error> to Status;
        let s = strm
            .try_chunks(chunk_size)
            .map_ok(|chunk: Vec<String>| ExportedChunk { data: chunk })
            .map_err(|e: TryChunksError<_, io::Error>| Status::internal(e.1.to_string()));

        Ok(Response::new(Box::pin(s)))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + 'static>>;

    #[minitrace::trace]
    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let (tx, rx) = mpsc::channel(4);

        let mn = &self.meta_node;

        let add_res = mn.add_watcher(request.into_inner(), tx).await;

        match add_res {
            Ok(watcher) => {
                let stream = WatchStream::new(rx, watcher, mn.dispatcher_handle.clone());
                Ok(Response::new(Box::pin(stream) as Self::WatchStream))
            }
            Err(e) => {
                // TODO: test error return.
                Err(Status::invalid_argument(e))
            }
        }
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.check_token(request.metadata())?;

        let _guard = RequestInFlight::guard();

        let meta_node = &self.meta_node;
        let members = meta_node.get_grpc_advertise_addrs().await;

        let resp = MemberListReply { data: members };
        network_metrics::incr_sent_bytes(resp.encoded_len() as u64);

        Ok(Response::new(resp))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        let _guard = RequestInFlight::guard();
        let status = self
            .meta_node
            .get_status()
            .await
            .map_err(|e| Status::internal(format!("get meta node status failed: {}", e)))?;

        let resp = ClusterStatus {
            id: status.id,
            binary_version: status.binary_version,
            data_version: status.data_version.to_string(),
            endpoint: status.endpoint,
            db_size: status.db_size,
            state: status.state,
            is_leader: status.is_leader,
            current_term: status.current_term,
            last_log_index: status.last_log_index,
            last_applied: status.last_applied.to_string(),
            snapshot_last_log_id: status.snapshot_last_log_id.map(|id| id.to_string()),
            purged: status.purged.map(|id| id.to_string()),
            leader: status.leader.map(|node| node.to_string()),
            replication: status
                .replication
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v.to_string())))
                .collect(),
            voters: status.voters.iter().map(|n| n.to_string()).collect(),
            non_voters: status.non_voters.iter().map(|n| n.to_string()).collect(),
            last_seq: status.last_seq,
        };
        Ok(Response::new(resp))
    }

    async fn get_client_info(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ClientInfo>, Status> {
        let _guard = RequestInFlight::guard();

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
