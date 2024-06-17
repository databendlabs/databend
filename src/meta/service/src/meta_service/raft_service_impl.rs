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

//! Meta service impl a grpc server that serves both raft protocol: append_entries, vote and install_snapshot.
//! It also serves RPC for user-data access.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::future::TimingFutureExt;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_raft_store::sm_v003::adapter::upgrade_snapshot_data_v002_to_v003;
use databend_common_meta_raft_store::sm_v003::open_snapshot::OpenSnapshot;
use databend_common_meta_raft_store::sm_v003::received::Received;
use databend_common_meta_sled_store::openraft::StorageError;
use databend_common_meta_types::protobuf::raft_service_server::RaftService;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::AppendEntriesRequest;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::InstallSnapshotError;
use databend_common_meta_types::InstallSnapshotRequest;
use databend_common_meta_types::InstallSnapshotResponse;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageIOError;
use databend_common_meta_types::Vote;
use databend_common_meta_types::VoteRequest;
use databend_common_metrics::count::Count;
use futures::TryStreamExt;
use log::error;
use log::info;
use log::warn;
use minitrace::full_name;
use minitrace::prelude::*;
use tonic::codegen::BoxStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::meta_service::snapshot_receiver_v1;
use crate::meta_service::snapshot_receiver_v1::ReceiverV1;
use crate::meta_service::MetaNode;
use crate::metrics::raft_metrics;

pub struct RaftServiceImpl {
    pub meta_node: Arc<MetaNode>,

    /// The receiver for install-snapshot v1 RPC.
    receiver_v1: Mutex<Option<ReceiverV1>>,
}

impl RaftServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            meta_node,
            receiver_v1: Mutex::new(None),
        }
    }

    fn incr_meta_metrics_recv_bytes_from_peer(&self, request: &Request<RaftRequest>) {
        if let Some(addr) = request.remote_addr() {
            let message: &RaftRequest = request.get_ref();
            let bytes = message.data.len() as u64;
            raft_metrics::network::incr_recvfrom_bytes(addr.to_string(), bytes);
        }
    }

    async fn do_install_snapshot_v1(
        &self,
        request: Request<SnapshotChunkRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let addr = remote_addr(&request);

        let snapshot_req = request.into_inner();
        raft_metrics::network::incr_recvfrom_bytes(addr.clone(), snapshot_req.data_len());

        let _g = snapshot_recv_inflight(&addr).counter_guard();

        let chunk = snapshot_req
            .chunk
            .ok_or_else(|| GrpcHelper::invalid_arg("SnapshotChunkRequest.chunk is None"))?;

        let (vote, snapshot_meta): (Vote, SnapshotMeta) =
            GrpcHelper::parse(&snapshot_req.rpc_meta)?;

        let install_snapshot_req = InstallSnapshotRequest {
            vote,
            meta: snapshot_meta,
            offset: chunk.offset,
            data: chunk.data,
            done: chunk.done,
        };

        let res = self
            .receive_chunked_snapshot_v1(install_snapshot_req)
            .timed(observe_snapshot_recv_spent(&addr))
            .await;

        raft_metrics::network::incr_snapshot_recvfrom_result(addr.clone(), res.is_ok());

        GrpcHelper::make_grpc_result(res)
    }

    /// Receive a chunk based snapshot from the leader.
    async fn receive_chunked_snapshot_v1(
        &self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError<InstallSnapshotError>> {
        let raft = &self.meta_node.raft;

        let req_vote = req.vote;
        let my_vote = raft.with_raft_state(|state| *state.vote_ref()).await?;
        let snapshot_meta = req.meta.clone();
        let snapshot_id = snapshot_meta.snapshot_id.clone();
        let sig = snapshot_meta.signature();

        let io_err_to_read_snap_err = |e: io::Error| {
            let io_err = StorageIOError::read_snapshot(Some(sig.clone()), &e);
            StorageError::from(io_err)
        };

        let resp = InstallSnapshotResponse { vote: my_vote };

        let ss_store = self.meta_node.sto.snapshot_store();

        let finished_snapshot = {
            let mut receiver_v1 = self.receiver_v1.lock().await;
            snapshot_receiver_v1::receive_snapshot_v1(&mut receiver_v1, &ss_store, req).await?
        };

        if let Some(temp_path) = finished_snapshot {
            let snapshot_data_v1 =
                SnapshotData::open_temp(temp_path).map_err(io_err_to_read_snap_err)?;

            let db = upgrade_snapshot_data_v002_to_v003(
                &ss_store,
                Box::new(snapshot_data_v1),
                snapshot_id,
            )
            .await
            .map_err(io_err_to_read_snap_err)?;

            let snapshot = Snapshot {
                meta: snapshot_meta.clone(),
                snapshot: Box::new(db),
            };

            let resp = raft.install_full_snapshot(req_vote, snapshot).await?;
            Ok(resp.into())
        } else {
            Ok(resp)
        }
    }

    async fn do_install_snapshot_v003(
        &self,
        request: Request<Streaming<SnapshotChunkRequestV003>>,
    ) -> Result<Response<RaftReply>, Status> {
        let addr = remote_addr(&request);

        let received = self.receive_binary_snapshot(request).await?;

        let Received {
            vote: req_vote,
            snapshot_meta,
            temp_path,
            ..
        } = received;

        let raft_config = &self.meta_node.sto.config;

        let db = DB::open_snapshot(&temp_path, snapshot_meta.snapshot_id.clone(), raft_config)
            .map_err(|e| {
                Status::internal(format!(
                    "Fail to open snapshot: {:?}, snapshot_meta:{:?} path: {}",
                    e, &snapshot_meta, temp_path
                ))
            })?;

        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(db),
        };

        let res = self
            .meta_node
            .raft
            .install_full_snapshot(req_vote, snapshot)
            .await
            .map_err(GrpcHelper::internal_err);

        raft_metrics::network::incr_snapshot_recvfrom_result(addr.clone(), res.is_ok());

        let resp = res?;

        GrpcHelper::ok_response(&resp)
    }

    /// Receive a single file snapshot in binary chunks.
    ///
    /// Returns the (snapshot-format, request-vote, snapshot-meta, file-temp-path).
    async fn receive_binary_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunkRequestV003>>,
    ) -> Result<Received, Status> {
        let addr = remote_addr(&request);

        let _guard = snapshot_recv_inflight(&addr).counter_guard();

        let ss_store = self.meta_node.sto.snapshot_store();

        let mut receiver_v003 = ss_store.new_receiver(&addr).map_err(io_err_to_status)?;
        receiver_v003.set_on_recv_callback(new_incr_recvfrom_bytes(addr.clone()));

        let (tx, join_handle) = receiver_v003.spawn_receiving_thread("receive_binary_snapshot");

        let mut strm = request.into_inner();

        databend_common_base::runtime::spawn(async move {
            while let Some(chunk) = strm.try_next().await.map_err(|e| {
                error!("fail to receive binary snapshot chunk: {:?}", &e);
                e
            })? {
                let res = tx.send(chunk).await;
                if res.is_err() {
                    info!("fail to send snapshot chunk to receiver_v003 thread; Stop receiving");
                    break;
                }
            }
            Ok::<(), Status>(())
        });

        let received = join_handle
            .await
            // join error
            .map_err(|_e| {
                warn!("receiver_v003 thread quit");
                Status::aborted("snapshot receiver thread is closed without finishing")
            })?
            // io error
            .map_err(io_err_to_status)?
            // unfinished error
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        Ok(received)
    }
}

#[async_trait::async_trait]
impl RaftService for RaftServiceImpl {
    async fn forward(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let forward_req: ForwardRequest<ForwardRequestBody> = GrpcHelper::parse_req(request)?;

            let res = self.meta_node.handle_forwardable_request(forward_req).await;

            let res = res.map(|(_endpoint, forward_resp)| forward_resp);

            let raft_reply: RaftReply = res.into();

            Ok(Response::new(raft_reply))
        }
        .in_span(root)
        .await
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let forward_req: ForwardRequest<MetaGrpcReadReq> = GrpcHelper::parse_req(request)?;

            let (endpoint, strm) = self
                .meta_node
                .handle_forwardable_request(forward_req)
                .await
                .map_err(GrpcHelper::internal_err)?;

            let mut resp = Response::new(strm);
            GrpcHelper::add_response_meta_leader(&mut resp, endpoint.as_ref());

            Ok(resp)
        }
        .in_span(root)
        .await
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let ae_req: AppendEntriesRequest = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let prev = ae_req.prev_log_id;
            let last = ae_req.entries.last().map(|x| x.log_id);
            info!(
                "RaftServiceImpl::append_entries: start: [{:?}, {:?}]",
                prev, last
            );

            let resp = raft
                .append_entries(ae_req)
                .await
                .map_err(GrpcHelper::internal_err)?;

            info!(
                "RaftServiceImpl::append_entries: done: [{:?}, {:?}]",
                prev, last
            );

            GrpcHelper::ok_response(&resp)
        }
        .in_span(root)
        .await
    }

    async fn install_snapshot_v1(
        &self,
        request: Request<SnapshotChunkRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        self.do_install_snapshot_v1(request).in_span(root).await
    }

    async fn install_snapshot_v003(
        &self,
        request: Request<Streaming<SnapshotChunkRequestV003>>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        self.do_install_snapshot_v003(request).in_span(root).await
    }

    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let v_req: VoteRequest = GrpcHelper::parse_req(request)?;

            let (vote, last_log_id) = (v_req.vote, v_req.last_log_id);

            info!("RaftServiceImpl::vote: start: {:?}", (vote, last_log_id));

            let raft = &self.meta_node.raft;

            let resp = raft.vote(v_req).await.map_err(GrpcHelper::internal_err)?;

            info!("RaftServiceImpl::vote: done: {:?}", (vote, last_log_id));

            GrpcHelper::ok_response(&resp)
        }
        .in_span(root)
        .await
    }
}

/// Get remote address from tonic request.
fn remote_addr<T>(request: &Request<T>) -> String {
    if let Some(addr) = request.remote_addr() {
        addr.to_string()
    } else {
        "unknown address".to_string()
    }
}

/// Create a function record the time cost of snapshot receiving.
fn observe_snapshot_recv_spent(addr: &str) -> impl Fn(Duration, Duration) + '_ {
    |t, _b| {
        raft_metrics::network::observe_snapshot_recvfrom_spent(
            addr.to_string(),
            t.as_secs() as f64,
        );
    }
}

/// Create a function that increases metric value of inflight snapshot receiving.
fn snapshot_recv_inflight(addr: impl ToString) -> impl FnMut(i64) {
    move |i: i64| raft_metrics::network::incr_snapshot_recvfrom_inflight(addr.to_string(), i)
}

/// Create a function that increases metric value of bytes received from a peer.
fn new_incr_recvfrom_bytes(addr: impl ToString) -> impl Fn(u64) + Send {
    let addr = addr.to_string();
    move |size: u64| raft_metrics::network::incr_recvfrom_bytes(addr.clone(), size)
}

fn io_err_to_status(e: io::Error) -> Status {
    match e.kind() {
        io::ErrorKind::InvalidInput => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}
