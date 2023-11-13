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

use common_base::future::TimingFutureExt;
use common_meta_client::MetaGrpcReadReq;
use common_meta_types::protobuf::raft_service_server::RaftService;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::StreamItem;
use common_metrics::count::Count;
use minitrace::full_name;
use minitrace::prelude::*;
use tonic::codegen::BoxStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::grpc_helper::GrpcHelper;
use crate::message::ForwardRequest;
use crate::message::ForwardRequestBody;
use crate::meta_service::MetaNode;
use crate::metrics::raft_metrics;

pub struct RaftServiceImpl {
    pub meta_node: Arc<MetaNode>,
}

impl RaftServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self { meta_node }
    }

    fn incr_meta_metrics_recv_bytes_from_peer(&self, request: &tonic::Request<RaftRequest>) {
        if let Some(addr) = request.remote_addr() {
            let message: &RaftRequest = request.get_ref();
            let bytes = message.data.len() as u64;
            raft_metrics::network::incr_recvfrom_bytes(addr.to_string(), bytes);
        }
    }
}

#[async_trait::async_trait]
impl RaftService for RaftServiceImpl {
    async fn forward(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let forward_req: ForwardRequest<ForwardRequestBody> = GrpcHelper::parse_req(request)?;

            let res = self.meta_node.handle_forwardable_request(forward_req).await;

            let raft_reply: RaftReply = res.into();

            Ok(tonic::Response::new(raft_reply))
        }
        .in_span(root)
        .await
    }

    type KvReadV1Stream = BoxStream<StreamItem>;

    async fn kv_read_v1(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<Self::KvReadV1Stream>, Status> {
        let root = common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let forward_req: ForwardRequest<MetaGrpcReadReq> = GrpcHelper::parse_req(request)?;

            let strm = self
                .meta_node
                .handle_forwardable_request(forward_req)
                .await
                .map_err(GrpcHelper::internal_err)?;

            Ok(tonic::Response::new(strm))
        }
        .in_span(root)
        .await
    }

    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let ae_req = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft
                .append_entries(ae_req)
                .await
                .map_err(GrpcHelper::internal_err)?;

            GrpcHelper::ok_response(resp)
        }
        .in_span(root)
        .await
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            let addr = remote_addr(&request);

            self.incr_meta_metrics_recv_bytes_from_peer(&request);
            let _g = snapshot_recv_inflight(&addr).counter_guard();

            let is_req = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft
                .install_snapshot(is_req)
                .timed(observe_snapshot_recv_spent(&addr))
                .await
                .map_err(GrpcHelper::internal_err);

            raft_metrics::network::incr_snapshot_recvfrom_result(addr.clone(), resp.is_ok());

            match resp {
                Ok(resp) => GrpcHelper::ok_response(resp),
                Err(e) => Err(e),
            }
        }
        .in_span(root)
        .await
    }

    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(full_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let v_req = GrpcHelper::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft.vote(v_req).await.map_err(GrpcHelper::internal_err)?;

            GrpcHelper::ok_response(resp)
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
