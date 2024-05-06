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

use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio::io::AsyncWriteExt;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::future::TimingFutureExt;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::network::snapshot_transport;
use databend_common_meta_types::protobuf::raft_service_server::RaftService;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV2;
use databend_common_meta_types::protobuf::StreamItem;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::InstallSnapshotError;
use databend_common_meta_types::InstallSnapshotRequest;
use databend_common_meta_types::InstallSnapshotResponse;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use databend_common_metrics::count::Count;
use futures::TryStreamExt;
use log::debug;
use log::info;
use minitrace::full_name;
use minitrace::prelude::*;
use tonic::codegen::BoxStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::meta_service::MetaNode;
use crate::metrics::raft_metrics;

pub struct RaftServiceImpl {
    pub meta_node: Arc<MetaNode>,

    snapshot: Mutex<Option<snapshot_transport::Streaming<TypeConfig>>>,
}

impl RaftServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            meta_node,
            snapshot: Mutex::new(None),
        }
    }

    fn incr_meta_metrics_recv_bytes_from_peer(&self, request: &Request<RaftRequest>) {
        if let Some(addr) = request.remote_addr() {
            let message: &RaftRequest = request.get_ref();
            let bytes = message.data.len() as u64;
            raft_metrics::network::incr_recvfrom_bytes(addr.to_string(), bytes);
        }
    }

    async fn do_install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let addr = remote_addr(&request);

        self.incr_meta_metrics_recv_bytes_from_peer(&request);
        let _g = snapshot_recv_inflight(&addr).counter_guard();

        let is_req = GrpcHelper::parse_req(request)?;

        let res = self
            .receive_chunked_snapshot(is_req)
            .timed(observe_snapshot_recv_spent(&addr))
            .await;

        raft_metrics::network::incr_snapshot_recvfrom_result(addr.clone(), res.is_ok());

        GrpcHelper::make_grpc_result(res)
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
            .receive_chunked_snapshot(install_snapshot_req)
            .timed(observe_snapshot_recv_spent(&addr))
            .await;

        raft_metrics::network::incr_snapshot_recvfrom_result(addr.clone(), res.is_ok());

        GrpcHelper::make_grpc_result(res)
    }

    /// Receive a chunk based snapshot from the leader.
    async fn receive_chunked_snapshot(
        &self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError<InstallSnapshotError>> {
        let raft = &self.meta_node.raft;

        let req_vote = req.vote;
        let my_vote = raft.with_raft_state(|state| *state.vote_ref()).await?;
        let resp = InstallSnapshotResponse { vote: my_vote };

        let finished_snapshot = {
            use openraft::network::snapshot_transport::Chunked;
            use openraft::network::snapshot_transport::SnapshotTransport;

            let mut streaming = self.snapshot.lock().await;
            Chunked::receive_snapshot(&mut *streaming, raft, req).await?
        };

        if let Some(snapshot) = finished_snapshot {
            let resp = raft.install_full_snapshot(req_vote, snapshot).await?;
            Ok(resp.into())
        } else {
            Ok(resp)
        }
    }

    async fn do_install_snapshot_v2(
        &self,
        request: Request<Streaming<SnapshotChunkRequestV2>>,
    ) -> Result<Response<RaftReply>, Status> {
        let addr = remote_addr(&request);

        let _g = snapshot_recv_inflight(&addr).counter_guard();

        let mut strm = request.into_inner();

        // Extract the first chunk to get the rpc_meta.
        let (format, req_vote, snapshot_meta) = {
            let Some(chunk) = strm.try_next().await? else {
                return Err(GrpcHelper::invalid_arg("empty snapshot stream"));
            };

            let Some(meta) = &chunk.rpc_meta else {
                return Err(GrpcHelper::invalid_arg(
                    "first SnapshotChunkRequestV2.rpc_meta is None",
                ));
            };

            if !chunk.chunk.is_empty() {
                return Err(GrpcHelper::invalid_arg(
                    "first SnapshotChunkRequestV2.chunk should not contain any data",
                ));
            }

            let rpc_mta: (String, Vote, SnapshotMeta) = GrpcHelper::parse(meta)?;
            rpc_mta
        };

        // Snapshot format is not used for now.
        let _ = format;

        info!(
            format :% = &format,
            req_vote :% = &req_vote,
            snapshot_meta :% = &snapshot_meta;
            "Begin receiving snapshot v2 stream from: {}",
            addr
        );

        let mut snapshot_data = self
            .meta_node
            .raft
            .begin_receiving_snapshot()
            .await
            .map_err(GrpcHelper::internal_err)?;

        let mut ith = 0;
        let mut total_len = 0;
        while let Some(chunk) = strm.try_next().await? {
            let data_len = chunk.chunk.len() as u64;
            total_len += data_len;
            debug!(
                len = data_len,
                total_len = total_len;
                "received {ith}-th snapshot chunk from {addr}"
            );

            ith += 1;
            if ith % 100 == 0 {
                info!(
                    total_len = total_len;
                    "received {ith}-th snapshot chunk from {addr}"
                );
            }

            raft_metrics::network::incr_recvfrom_bytes(addr.clone(), data_len);

            snapshot_data.write_all(&chunk.chunk).await?;
        }

        snapshot_data.shutdown().await?;

        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: snapshot_data,
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

            let ae_req = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft
                .append_entries(ae_req)
                .await
                .map_err(GrpcHelper::internal_err)?;

            GrpcHelper::ok_response(&resp)
        }
        .in_span(root)
        .await
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        self.do_install_snapshot(request).in_span(root).await
    }

    async fn install_snapshot_v1(
        &self,
        request: Request<SnapshotChunkRequest>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        self.do_install_snapshot_v1(request).in_span(root).await
    }

    async fn install_snapshot_v2(
        &self,
        request: Request<Streaming<SnapshotChunkRequestV2>>,
    ) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);
        self.do_install_snapshot_v2(request).in_span(root).await
    }

    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftReply>, Status> {
        let root = databend_common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let v_req = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft.vote(v_req).await.map_err(GrpcHelper::internal_err)?;

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
