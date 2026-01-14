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
use std::sync::Weak;
use std::time::Instant;
use std::time::SystemTime;

use arrow_flight::BasicAuth;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::future::TimedFutureExt;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingGuard;
use databend_common_grpc::GrpcClaim;
use databend_common_grpc::GrpcToken;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_client::MetaGrpcReq;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::Empty;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::HandshakeRequest;
use databend_common_meta_types::protobuf::HandshakeResponse;
use databend_common_meta_types::protobuf::KeysCount;
use databend_common_meta_types::protobuf::KeysLayoutRequest;
use databend_common_meta_types::protobuf::KvGetManyRequest;
use databend_common_meta_types::protobuf::KvListRequest;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::protobuf::meta_service_server::MetaService;
use databend_common_metrics::count::Count;
use databend_common_metrics::count::WithCount;
use databend_common_tracing::start_trace_for_remote_request;
use display_more::DisplayOptionExt;
use fastrace::func_name;
use fastrace::func_path;
use fastrace::prelude::*;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::TryChunksError;
use log::debug;
use log::error;
use log::info;
use prost::Message;
use tokio_stream;
use tokio_stream::Stream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tonic::codegen::BoxStream;
use tonic::metadata::MetadataMap;
use tonic::server::NamedService;
use watcher::watch_stream::WatchStreamSender;

use crate::meta_node::meta_handle::MetaHandle;
use crate::meta_service::watcher::DispatcherHandle;
use crate::meta_service::watcher::WatchTypes;
use crate::metrics::InFlightRead;
use crate::metrics::InFlightWrite;
use crate::metrics::network_metrics;
use crate::version::MIN_METACLI_SEMVER;
use crate::version::from_digit_ver;
use crate::version::to_digit_ver;

/// Guard type for in-flight read requests.
type InFlightReadGuard = WithCount<InFlightRead, ()>;

/// A request that is currently being processed.
///
/// Created when request handling begins, dropped when stream completes.
/// Holds guards that should live for the entire streaming lifecycle.
struct InFlightRequest {
    /// Guard to track in-flight request count.
    _guard: InFlightReadGuard,
    /// Guard for thread tracking with query ID.
    _thread_guard: Option<TrackingGuard>,
    /// Label for `incr_stream_sent_item` metrics.
    metrics_label: &'static str,
    /// Logs stream throughput stats when dropped.
    throughput_logger: ThroughputLogger,
}

impl InFlightRequest {
    /// Create from request reference (extracts thread tracking automatically).
    fn new<T>(request: &Request<T>, metrics_label: &'static str, log_label: String) -> Self {
        Self {
            _guard: InFlightRead::guard(),
            _thread_guard: thread_tracking_guard(request),
            metrics_label,
            throughput_logger: ThroughputLogger::new(log_label),
        }
    }

    /// Create with pre-extracted thread guard (use when request is consumed before metrics_label/log_label are known).
    fn with_thread_guard(
        thread_guard: Option<TrackingGuard>,
        metrics_label: &'static str,
        log_label: String,
    ) -> Self {
        Self {
            _guard: InFlightRead::guard(),
            _thread_guard: thread_guard,
            metrics_label,
            throughput_logger: ThroughputLogger::new(log_label),
        }
    }

    /// Wrap a stream with in-flight tracking; logs throughput when stream completes.
    fn track<S>(mut self, strm: S) -> impl Stream<Item = Result<StreamItem, Status>>
    where S: Stream<Item = Result<StreamItem, Status>> {
        let metrics_label = self.metrics_label;
        strm.map(move |item| {
            // Reference guards to keep them alive for stream lifetime.
            // Rust 2021 closures only capture fields that are used;
            // without these references, guards would be dropped when track() returns.
            let _guard = &self._guard;
            let _thread_guard = &self._thread_guard;

            network_metrics::incr_stream_sent_item(metrics_label);
            self.throughput_logger.incr_count();
            item
        })
    }
}

/// Logs stream throughput stats (items/ms, latency) when dropped.
struct ThroughputLogger {
    count: u64,
    start: Instant,
    log_label: String,
}

impl ThroughputLogger {
    fn new(log_label: String) -> Self {
        Self {
            count: 0,
            start: Instant::now(),
            log_label,
        }
    }

    fn incr_count(&mut self) {
        self.count += 1;
    }
}

impl Drop for ThroughputLogger {
    fn drop(&mut self) {
        let total = self.start.elapsed();
        let total_items = self.count;
        let items_per_ms = total_items / (total.as_millis() as u64 + 1);
        let latency = total / (total_items.max(1) as u32);
        info!(
            "StreamElapsed: total: {:?}; items: {}, items/ms: {}, item_latency: {:?}; {}",
            total, total_items, items_per_ms, latency, self.log_label
        );
    }
}

pub struct MetaServiceImpl {
    token: GrpcToken,
    version: BuildInfoRef,
    /// MetaServiceImpl is not dropped if there is an alive connection.
    ///
    /// Thus make the reference to [`MetaNode`] a Weak reference so that it does not prevent [`MetaNode`] to be dropped
    pub(crate) meta_handle: Weak<MetaHandle>,
}

impl Drop for MetaServiceImpl {
    fn drop(&mut self) {
        if let Some(meta_node) = self.meta_handle.upgrade() {
            debug!("MetaServiceImpl::drop: id={}", meta_node.id);
        } else {
            debug!("MetaServiceImpl::drop: inner MetaNode already dropped");
        }
    }
}

impl MetaServiceImpl {
    pub fn create(meta_handle: Weak<MetaHandle>) -> Self {
        Self {
            version: meta_handle.upgrade().unwrap().version,
            token: GrpcToken::create(),
            meta_handle,
        }
    }

    pub fn try_get_meta_handle(&self) -> Result<Arc<MetaHandle>, Status> {
        self.meta_handle.upgrade().ok_or_else(|| {
            Status::internal("MetaNode is already dropped, can not serve new requests")
        })
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

    #[fastrace::trace]
    async fn handle_kv_api(&self, request: Request<RaftRequest>) -> Result<RaftReply, Status> {
        let req: MetaGrpcReq = request.try_into()?;

        let meta_handle = self.try_get_meta_handle()?;
        let id = meta_handle.id;

        debug!(
            "id={} {}: Received MetaGrpcReq: {:?}",
            id,
            func_name!(),
            req
        );

        let reply = match &req {
            MetaGrpcReq::UpsertKV(a) => {
                let res = meta_handle.handle_upsert_kv(a.clone()).await;
                debug!(
                    "id={} MetaGrpcReq UpsertKV: request: {:?} res: {:?}",
                    id, req, res
                );
                let res = res?;
                // TODO: the MetaApiError should be converted to Status
                RaftReply::from(res)
            }
        };

        network_metrics::incr_request_result(reply.error.is_empty());

        Ok(reply)
    }

    #[fastrace::trace]
    async fn handle_kv_read_v1(
        &self,
        req: MetaGrpcReadReq,
    ) -> Result<(Option<Endpoint>, BoxStream<StreamItem>), Status> {
        debug!("{}: Received ReadRequest: {:?}", func_name!(), req);

        let meta_handle = self.try_get_meta_handle()?;

        let res = meta_handle
            .handle_kv_read_v1(req.clone())
            .await?
            .map_err(GrpcHelper::internal_err);

        network_metrics::incr_request_result(res.is_ok());
        res
    }

    #[fastrace::trace]
    async fn handle_kv_list(
        &self,
        prefix: String,
        limit: Option<u64>,
    ) -> Result<BoxStream<StreamItem>, Status> {
        debug!(
            "{}: Received KvListRequest: prefix={}, limit={}",
            func_name!(),
            prefix,
            limit.display()
        );

        let meta_handle = self.try_get_meta_handle()?;

        let res = meta_handle.handle_kv_list(prefix, limit).await;
        network_metrics::incr_request_result(res.is_ok());

        res
    }

    #[fastrace::trace]
    async fn handle_kv_get_many(
        &self,
        input: impl Stream<Item = Result<KvGetManyRequest, Status>> + Send + 'static,
    ) -> Result<BoxStream<StreamItem>, Status> {
        debug!("{}: Received KvGetMany stream", func_name!());

        let meta_handle = self.try_get_meta_handle()?;

        let res = meta_handle.handle_kv_get_many(input).await;
        network_metrics::incr_request_result(res.is_ok());

        res
    }

    #[fastrace::trace]
    async fn handle_txn(
        &self,
        request: Request<TxnRequest>,
    ) -> Result<(Option<Endpoint>, TxnReply), Status> {
        let txn = request.into_inner();

        debug!("{}: Received TxnRequest: {}", func_name!(), txn);

        let meta_handle = self.try_get_meta_handle()?;

        let log_msg = format!("TxnRequest: {}", txn);

        let forward_res = meta_handle
            .handle_transaction(txn)
            .log_elapsed_info(log_msg)
            .await?;

        let (endpoint, txn_reply) = match forward_res {
            Ok((endpoint, txn_reply)) => (endpoint, txn_reply),
            Err(err) => {
                network_metrics::incr_request_result(false);
                error!("txn request failed: {:?}", err);
                return Err(Status::internal(err.to_string()));
            }
        };

        network_metrics::incr_request_result(true);

        Ok((endpoint, txn_reply))
    }
}

impl NamedService for MetaServiceImpl {
    const NAME: &'static str = "meta_service";
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    type HandshakeStream = BoxStream<HandshakeResponse>;

    // rpc handshake first
    #[fastrace::trace]
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
                protocol_version: to_digit_ver(&self.version.semantic),
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

        let _guard = thread_tracking_guard(&request);
        ThreadTracker::tracking_future(async move {
            network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
            let _guard = InFlightWrite::guard();

            let root = start_trace_for_remote_request(func_path!(), &request);
            let reply = self.handle_kv_api(request).in_span(root).await?;

            network_metrics::incr_sent_bytes(reply.encoded_len() as u64);

            Ok(Response::new(reply))
        })
        .await
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        self.check_token(request.metadata())?;

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
        let root = start_trace_for_remote_request(func_path!(), &request);
        let thread_guard = thread_tracking_guard(&request);
        let req: MetaGrpcReadReq = GrpcHelper::parse_req(request)?;
        let in_flight = InFlightRequest::with_thread_guard(
            thread_guard,
            req.type_name(),
            format!("ReadRequest: {:?}", req),
        );

        let fut = async {
            let (endpoint, strm) = self.handle_kv_read_v1(req).await?;

            let strm = in_flight.track(strm);

            let mut resp = Response::new(strm.boxed());
            GrpcHelper::add_response_meta_leader(&mut resp, endpoint.as_ref());
            Ok(resp)
        };

        ThreadTracker::tracking_future(fut.in_span(root)).await
    }

    type KvListStream = BoxStream<StreamItem>;

    /// List key-value pairs by prefix.
    ///
    /// This RPC requires leadership. If this node is not the leader,
    /// it returns a `Status::unavailable` error with the leader's endpoint in metadata.
    /// Clients should retry with the leader directly.
    async fn kv_list(
        &self,
        request: Request<KvListRequest>,
    ) -> Result<Response<Self::KvListStream>, Status> {
        self.check_token(request.metadata())?;

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
        let root = start_trace_for_remote_request(func_path!(), &request);
        let in_flight = InFlightRequest::new(
            &request,
            "kv_list",
            format!(
                "KvList: prefix={}, limit={}",
                request.get_ref().prefix,
                request.get_ref().limit.display()
            ),
        );
        let req = request.into_inner();

        let fut = async {
            let strm = self.handle_kv_list(req.prefix, req.limit).await?;

            let strm = in_flight.track(strm);
            Ok(Response::new(strm.boxed()))
        };

        ThreadTracker::tracking_future(fut.in_span(root)).await
    }

    type KvGetManyStream = BoxStream<StreamItem>;

    /// Get multiple key-value pairs by streaming keys.
    ///
    /// This RPC requires leadership. If this node is not the leader,
    /// it returns a `Status::unavailable` error with the leader's endpoint in metadata.
    /// Clients should retry with the leader directly.
    async fn kv_get_many(
        &self,
        request: Request<Streaming<KvGetManyRequest>>,
    ) -> Result<Response<Self::KvGetManyStream>, Status> {
        self.check_token(request.metadata())?;

        let root = start_trace_for_remote_request(func_path!(), &request);
        let in_flight = InFlightRequest::new(&request, "kv_get_many", "KvGetMany".to_string());
        let input = request
            .into_inner()
            .inspect_ok(|req| network_metrics::incr_recv_bytes(req.encoded_len() as u64));

        let fut = async {
            let strm = self.handle_kv_get_many(input).await?;

            let strm = in_flight.track(strm);
            Ok(Response::new(strm.boxed()))
        };

        ThreadTracker::tracking_future(fut.in_span(root)).await
    }

    async fn transaction(
        &self,
        request: Request<TxnRequest>,
    ) -> Result<Response<TxnReply>, Status> {
        self.check_token(request.metadata())?;

        let _guard = thread_tracking_guard(&request);

        ThreadTracker::tracking_future(async move {
            network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);
            let _guard = InFlightWrite::guard();

            let root = start_trace_for_remote_request(func_path!(), &request);
            let (endpoint, reply) = self.handle_txn(request).in_span(root).await?;

            network_metrics::incr_sent_bytes(reply.encoded_len() as u64);

            let mut resp = Response::new(reply);
            GrpcHelper::add_response_meta_leader(&mut resp, endpoint.as_ref());

            Ok(resp)
        })
        .await
    }

    type ExportStream = Pin<Box<dyn Stream<Item = Result<ExportedChunk, Status>> + Send + 'static>>;

    /// Export all meta service data.
    ///
    /// Including header, raft state, logs and state machine.
    /// The exported data is a series of JSON encoded strings of `RaftStoreEntry`.
    async fn export(
        &self,
        _request: Request<databend_common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let guard = InFlightRead::guard();

        let meta_handle = self.try_get_meta_handle()?;

        let strm = meta_handle.handle_export().await?;

        let chunk_size = 32;
        // - Chunk up upto 32 Ok items inside a Vec<String>;
        // - Convert Vec<String> to ExportedChunk;
        // - Convert TryChunkError<_, io::Error> to Status;
        let s = strm
            .map(move |x| {
                let _g = &guard; // hold the guard until the stream is done.
                x
            })
            .try_chunks(chunk_size)
            .map_ok(|chunk: Vec<String>| ExportedChunk { data: chunk })
            .map_err(|e: TryChunksError<_, io::Error>| Status::internal(e.1.to_string()));

        Ok(Response::new(Box::pin(s)))
    }

    type ExportV1Stream =
        Pin<Box<dyn Stream<Item = Result<ExportedChunk, Status>> + Send + 'static>>;

    /// Export all meta data.
    ///
    /// Including header, raft state, logs and state machine.
    /// The exported data is a series of JSON encoded strings of `RaftStoreEntry`.
    async fn export_v1(
        &self,
        request: Request<pb::ExportRequest>,
    ) -> Result<Response<Self::ExportV1Stream>, Status> {
        let guard = InFlightRead::guard();

        let meta_handle = self.try_get_meta_handle()?;

        let strm = meta_handle.handle_export().await?;

        let chunk_size = request.get_ref().chunk_size.unwrap_or(32) as usize;
        // - Chunk up upto `chunk_size` Ok items inside a Vec<String>;
        // - Convert Vec<String> to ExportedChunk;
        // - Convert TryChunkError<_, io::Error> to Status;
        let s = strm
            .map(move |x| {
                let _g = &guard; // hold the guard until the stream is done.
                x
            })
            .try_chunks(chunk_size)
            .map_ok(|chunk: Vec<String>| ExportedChunk { data: chunk })
            .map_err(|e: TryChunksError<_, io::Error>| Status::internal(e.1.to_string()));

        Ok(Response::new(Box::pin(s)))
    }

    type SnapshotKeysLayoutStream =
        Pin<Box<dyn Stream<Item = Result<KeysCount, Status>> + Send + 'static>>;

    async fn snapshot_keys_layout(
        &self,
        request: Request<KeysLayoutRequest>,
    ) -> Result<Response<Self::SnapshotKeysLayoutStream>, Status> {
        let guard = InFlightRead::guard();

        let layout_request = request.into_inner();

        let meta_handle = self.try_get_meta_handle()?;

        let strm = meta_handle
            .handle_snapshot_keys_layout(layout_request)
            .await??;

        let s = strm.map(move |x| {
            let _g = &guard; // hold the guard until the stream is done.
            x.map_err(|e| Status::internal(e.to_string()))
        });

        Ok(Response::new(Box::pin(s)))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + 'static>>;

    #[fastrace::trace]
    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let watch = request.into_inner();

        let meta_handle = self.try_get_meta_handle()?;
        let stream = meta_handle.handle_watch(watch).await??;

        Ok(Response::new(stream as Self::WatchStream))
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.check_token(request.metadata())?;

        let _guard = InFlightRead::guard();

        let meta_handle = self.try_get_meta_handle()?;

        let members = meta_handle.handle_member_list(request.into_inner()).await?;

        let resp = MemberListReply { data: members };
        network_metrics::incr_sent_bytes(resp.encoded_len() as u64);

        Ok(Response::new(resp))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        let _guard = InFlightRead::guard();

        let meta_handle = self.try_get_meta_handle()?;

        let status = meta_handle.handle_get_status().await?;

        let resp = ClusterStatus {
            id: status.id,
            binary_version: status.binary_version,
            data_version: status.data_version.to_string(),
            endpoint: status.endpoint.unwrap_or_default(),

            raft_log_size: status.raft_log.wal_total_size,

            raft_log_status: Some(pb::RaftLogStatus {
                cache_items: status.raft_log.cache_items,
                cache_used_size: status.raft_log.cache_used_size,
                wal_total_size: status.raft_log.wal_total_size,
                wal_open_chunk_size: status.raft_log.wal_open_chunk_size,
                wal_offset: status.raft_log.wal_offset,
                wal_closed_chunk_count: status.raft_log.wal_closed_chunk_count,
                wal_closed_chunk_total_size: status.raft_log.wal_closed_chunk_total_size,
                wal_closed_chunk_sizes: status.raft_log.wal_closed_chunk_sizes,
            }),

            snapshot_key_count: status.snapshot_key_count,

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
        let _guard = InFlightRead::guard();

        let r = request.remote_addr();
        if let Some(addr) = r {
            let resp = ClientInfo {
                client_addr: addr.to_string(),
                server_time: Some(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                ),
            };
            return Ok(Response::new(resp));
        }
        Err(Status::unavailable("can not get client ip address"))
    }
}

fn thread_tracking_guard<T>(req: &tonic::Request<T>) -> Option<TrackingGuard> {
    if let Some(value) = req.metadata().get("QueryID") {
        if let Ok(value) = value.to_str() {
            let mut tracking_payload = ThreadTracker::new_tracking_payload();
            tracking_payload.query_id = Some(value.to_string());
            return Some(ThreadTracker::tracking(tracking_payload));
        }
    }

    None
}

/// Try to remove a [`WatchStream`] from the subscriber.
///
/// This function receives two weak references: one to the sender and one to the dispatcher handle.
/// Using weak references prevents memory leaks by avoiding cyclic references when these are captured in closures.
/// If either reference can't be upgraded, the function logs the situation and returns early.
pub(crate) fn try_remove_sender(
    weak_sender: Weak<WatchStreamSender<WatchTypes>>,
    weak_handle: Weak<DispatcherHandle>,
    ctx: &str,
) {
    info!("{ctx}: try removing:  {:?}", weak_sender);

    let Some(d) = weak_handle.upgrade() else {
        debug!(
            "{ctx}: when try removing {:?}; dispatcher is already dropped",
            weak_sender
        );
        return;
    };

    let Some(sender) = weak_sender.upgrade() else {
        debug!(
            "on_drop is called for WatchStream {:?}; but sender is already dropped",
            weak_sender
        );
        return;
    };

    debug!("{ctx}: request is sent to remove watcher: {}", sender);

    d.request(move |dispatcher| {
        dispatcher.remove_watcher(sender);
    })
}
