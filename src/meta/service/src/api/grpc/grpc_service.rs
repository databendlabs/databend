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

use std::future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Weak;
use std::time::Instant;
use std::time::SystemTime;

use arrow_flight::BasicAuth;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::BuildInfoRef;
use databend_common_base::future::TimedFutureExt;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingGuard;
use databend_common_grpc::GrpcClaim;
use databend_common_grpc::GrpcToken;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_client::MetaGrpcReq;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_raft_store::utils::seq_marked_to_seqv;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::meta_service_server::MetaService;
use databend_common_meta_types::protobuf::ClientInfo;
use databend_common_meta_types::protobuf::ClusterStatus;
use databend_common_meta_types::protobuf::Empty;
use databend_common_meta_types::protobuf::ExportedChunk;
use databend_common_meta_types::protobuf::HandshakeRequest;
use databend_common_meta_types::protobuf::HandshakeResponse;
use databend_common_meta_types::protobuf::MemberListReply;
use databend_common_meta_types::protobuf::MemberListRequest;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_meta_types::protobuf::WatchResponse;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use databend_common_metrics::count::Count;
use databend_common_tracing::start_trace_for_remote_request;
use fastrace::func_name;
use fastrace::func_path;
use fastrace::prelude::*;
use futures::stream::TryChunksError;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use map_api::mvcc::ScopedRange;
use prost::Message;
use state_machine_api::UserKey;
use tokio_stream;
use tokio_stream::Stream;
use tonic::codegen::BoxStream;
use tonic::metadata::MetadataMap;
use tonic::server::NamedService;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use watcher::dispatch::Command;
use watcher::key_range::build_key_range;
use watcher::util::new_initialization_sink;
use watcher::util::try_forward;
use watcher::watch_stream::WatchStream;
use watcher::watch_stream::WatchStreamSender;

use crate::api::grpc::OnCompleteStream;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::meta_service::watcher::DispatcherHandle;
use crate::meta_service::watcher::WatchTypes;
use crate::meta_service::MetaNode;
use crate::metrics::network_metrics;
use crate::metrics::InFlightRead;
use crate::metrics::InFlightWrite;
use crate::version::from_digit_ver;
use crate::version::to_digit_ver;
use crate::version::MIN_METACLI_SEMVER;

pub struct MetaServiceImpl {
    token: GrpcToken,
    version: BuildInfoRef,
    /// MetaServiceImpl is not dropped if there is an alive connection.
    ///
    /// Thus make the reference to [`MetaNode`] a Weak reference so that it does not prevent [`MetaNode`] to be dropped
    pub(crate) meta_node: Weak<MetaNode>,
}

impl Drop for MetaServiceImpl {
    fn drop(&mut self) {
        if let Some(meta_node) = self.meta_node.upgrade() {
            debug!("MetaServiceImpl::drop: id={}", meta_node.raft_store.id);
        } else {
            debug!("MetaServiceImpl::drop: inner MetaNode already dropped");
        }
    }
}

impl MetaServiceImpl {
    pub fn create(meta_node: Weak<MetaNode>) -> Self {
        Self {
            version: meta_node.upgrade().unwrap().version,
            token: GrpcToken::create(),
            meta_node,
        }
    }

    pub fn try_get_meta_node(&self) -> Result<Arc<MetaNode>, Status> {
        self.meta_node.upgrade().ok_or_else(|| {
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
        debug!("{}: Received MetaGrpcReq: {:?}", func_name!(), req);

        let m = self.try_get_meta_node()?;
        let reply = match &req {
            MetaGrpcReq::UpsertKV(a) => {
                let res = m
                    .kv_api()
                    .upsert_kv(a.clone())
                    .log_elapsed_info(format!("UpsertKV: {:?}", a))
                    .await;
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

        let req = ForwardRequest::new(1, req);

        let meta_node = self.try_get_meta_node()?;

        let res = meta_node
            .handle_forwardable_request::<MetaGrpcReadReq>(req.clone())
            .log_elapsed_info(format!("ReadRequest: {:?}", req))
            .await
            .map_err(GrpcHelper::internal_err);

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

        let ent = LogEntry::new(Cmd::Transaction(txn.clone()));

        let forward_req = ForwardRequest::new(1, ForwardRequestBody::Write(ent));

        let meta_node = self.try_get_meta_node()?;

        let forward_res = meta_node
            .handle_forwardable_request(forward_req)
            .log_elapsed_info(format!("TxnRequest: {}", txn))
            .await;

        let (endpoint, txn_reply) = match forward_res {
            Ok((endpoint, forward_resp)) => {
                let applied_state: AppliedState =
                    forward_resp.try_into().expect("expect AppliedState");

                let txn_reply: TxnReply = applied_state.try_into().expect("expect TxnReply");

                (endpoint, txn_reply)
            }
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

        let _guard = thread_tracking_guard(&request);

        network_metrics::incr_recv_bytes(request.get_ref().encoded_len() as u64);

        let root = start_trace_for_remote_request(func_path!(), &request);

        let req: MetaGrpcReadReq = GrpcHelper::parse_req(request)?;
        let req_typ = req.type_name();
        let req_str = format!("ReadRequest: {:?}", req);

        ThreadTracker::tracking_future(async move {
            let guard = InFlightRead::guard();
            let start = Instant::now();
            let (endpoint, strm) = self.handle_kv_read_v1(req).in_span(root).await?;

            // Counter to track total items sent
            let count = Arc::new(AtomicU64::new(0));
            let count2 = count.clone();

            let strm = strm
                .map(move |x| {
                    let _g = &guard; // hold the guard until the stream is done.
                    x
                })
                .map(move |item| {
                    network_metrics::incr_stream_sent_item(req_typ);
                    count2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    item
                });

            // Log the total time and item count when the stream is finished.
            let strm = OnCompleteStream::new(strm, move || {
                let total = start.elapsed();
                let total_items = count.load(std::sync::atomic::Ordering::Relaxed);
                let items_per_ms = total_items / (total.as_millis() as u64 + 1);
                let latency = total / (total_items.max(1) as u32);
                info!(
                    "StreamElapsed: total: {:?}; items: {}, items/ms: {}, item_latency: {:?}; {}",
                    total, total_items, items_per_ms, latency, req_str
                );
            });

            let strm = strm.boxed();

            let mut resp = Response::new(strm);
            GrpcHelper::add_response_meta_leader(&mut resp, endpoint.as_ref());

            Ok(resp)
        })
        .await
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

    /// Export all meta data.
    ///
    /// Including header, raft state, logs and state machine.
    /// The exported data is a series of JSON encoded strings of `RaftStoreEntry`.
    async fn export(
        &self,
        _request: Request<databend_common_meta_types::protobuf::Empty>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let guard = InFlightRead::guard();

        let meta_node = self.try_get_meta_node()?;

        let strm = meta_node.raft_store.clone().export();

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

        let meta_node = self.try_get_meta_node()?;

        let strm = meta_node.raft_store.clone().export();

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

    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send + 'static>>;

    #[fastrace::trace]
    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let watch = request.into_inner();

        info!("{}: Received WatchRequest: {}", func_name!(), watch);

        let key_range = build_key_range(
            &UserKey::new(&watch.key),
            &watch.key_end.as_ref().map(UserKey::new),
        )
        .map_err(Status::invalid_argument)?;
        let flush = watch.initial_flush;

        let (tx, rx) = mpsc::channel(4);

        let mn = self.try_get_meta_node()?;

        // Atomically:
        // - add watcher tx to dispatcher;
        // - reads and forwards a range of key-value pairs to the provided `tx`.
        //
        // This ensures consistency by:
        // 1. Queuing all data publishing through the singleton sender to maintain event ordering
        // 2. Reading the key-value range atomically within the state machine
        // 3. Forwarding the data to the event sender in a single transaction
        //
        // This approach prevents race conditions and guarantees that no events will be
        // delivered out of order to the watcher.
        let stream = {
            let sm = mn.raft_store.state_machine().get_inner();

            let sender = mn.new_watch_sender(watch, tx.clone())?;
            let sender_str = sender.to_string();
            let weak_sender = mn.insert_watch_sender(sender);

            // Build a closure to remove the stream tx from Dispatcher when the stream is dropped.
            let on_drop = {
                let weak_handle = Arc::downgrade(&mn.dispatcher_handle);
                move || {
                    try_remove_sender(weak_sender, weak_handle, "on-drop-WatchStream");
                }
            };

            let stream = WatchStream::new(rx, Box::new(on_drop));

            let stream = stream.map(move |item| {
                if let Ok(ref resp) = item {
                    network_metrics::incr_watch_sent(resp);
                }
                item
            });

            if flush {
                let ctx = "watch-Dispatcher";
                let snk = new_initialization_sink::<WatchTypes>(tx.clone(), ctx);
                let strm = sm.to_state_machine_snapshot().range(key_range).await?;
                let strm = strm
                    .try_filter_map(|(k, marked)| future::ready(Ok(seq_marked_to_seqv(k, marked))));

                info!("created initialization stream for {}", sender_str);

                let sndr = sender_str.clone();

                let fu = async move {
                    try_forward(strm, snk, ctx).await;

                    info!("initialization flush complete for watcher {}", sndr);

                    // Send an empty message with `is_initialization=false` to indicate
                    // the end of the initialization flush.
                    tx.send(Ok(WatchResponse::new_initialization_complete()))
                        .await
                        .map_err(|e| {
                            error!("failed to send flush complete message: {}", e);
                        })
                        .ok();

                    info!(
                        "finished sending initialization complete flag for watcher {}",
                        sndr
                    );
                };
                let fu = Box::pin(fu);

                info!(
                    "sending initial flush Future to watcher {} via Dispatcher",
                    sender_str
                );

                mn.dispatcher_handle.send_command(Command::Future(fu));
            }

            stream
        };

        Ok(Response::new(Box::pin(stream) as Self::WatchStream))
    }

    async fn member_list(
        &self,
        request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListReply>, Status> {
        self.check_token(request.metadata())?;

        let _guard = InFlightRead::guard();

        let meta_node = self.try_get_meta_node()?;

        let members = meta_node.get_grpc_advertise_addrs().await;

        let resp = MemberListReply { data: members };
        network_metrics::incr_sent_bytes(resp.encoded_len() as u64);

        Ok(Response::new(resp))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatus>, Status> {
        let _guard = InFlightRead::guard();

        let meta_node = self.try_get_meta_node()?;

        let status = meta_node
            .get_status()
            .await
            .map_err(|e| Status::internal(format!("get meta node status failed: {}", e)))?;

        let resp = ClusterStatus {
            id: status.id,
            binary_version: status.binary_version,
            data_version: status.data_version.to_string(),
            endpoint: status.endpoint,

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
fn try_remove_sender(
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
