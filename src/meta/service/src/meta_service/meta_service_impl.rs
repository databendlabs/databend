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

use std::error::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use common_meta_types::protobuf::raft_service_server::RaftService;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_tracing::func_name;
use minitrace::prelude::*;
use tonic::codegen::futures_core::Stream;

use crate::message::ForwardRequest;
use crate::meta_service::MetaNode;
use crate::metrics::raft_metrics;

pub type GrpcStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

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
            raft_metrics::network::incr_recv_bytes_from_peer(addr.to_string(), bytes);
        }
    }

    /// Parse tonic::Request and decode it into required type.
    fn parse_req<T>(request: tonic::Request<RaftRequest>) -> Result<T, tonic::Status>
    where T: serde::de::DeserializeOwned {
        let raft_req = request.into_inner();
        let req: T = serde_json::from_str(&raft_req.data).map_err(Self::invalid_arg)?;
        Ok(req)
    }

    /// Create an Ok response for raft API.
    fn ok_response<D>(d: D) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where D: serde::Serialize {
        let data = serde_json::to_string(&d).expect("fail to serialize resp");
        let reply = RaftReply {
            data,
            error: "".to_string(),
        };
        Ok(tonic::Response::new(reply))
    }

    /// Create a tonic::Status with invalid argument error.
    fn invalid_arg(e: impl Error) -> tonic::Status {
        tonic::Status::invalid_argument(e.to_string())
    }

    /// Create a tonic::Status with internal error.
    fn internal_err(e: impl Error) -> tonic::Status {
        tonic::Status::internal(e.to_string())
    }
}

#[async_trait::async_trait]
impl RaftService for RaftServiceImpl {
    async fn forward(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);

        async {
            let forward_req: ForwardRequest = Self::parse_req(request)?;

            let res = self.meta_node.handle_forwardable_request(forward_req).await;

            let raft_reply: RaftReply = res.into();

            Ok(tonic::Response::new(raft_reply))
        }
        .in_span(root)
        .await
    }

    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let ae_req = Self::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft
                .append_entries(ae_req)
                .await
                .map_err(Self::internal_err)?;

            Self::ok_response(resp)
        }
        .in_span(root)
        .await
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);

        async {
            let start = Instant::now();
            let addr = if let Some(addr) = request.remote_addr() {
                addr.to_string()
            } else {
                "unknown address".to_string()
            };

            self.incr_meta_metrics_recv_bytes_from_peer(&request);
            raft_metrics::network::incr_snapshot_recv_inflights_from_peer(addr.clone(), 1);

            let is_req = Self::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft
                .install_snapshot(is_req)
                .await
                .map_err(Self::internal_err);

            raft_metrics::network::sample_snapshot_recv(
                addr.clone(),
                start.elapsed().as_secs() as f64,
            );
            raft_metrics::network::incr_snapshot_recv_inflights_from_peer(addr.clone(), -1);
            raft_metrics::network::incr_snapshot_recv_status_from_peer(addr.clone(), resp.is_ok());

            match resp {
                Ok(resp) => Self::ok_response(resp),
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
        let root = common_tracing::start_trace_for_remote_request(func_name!(), &request);

        async {
            self.incr_meta_metrics_recv_bytes_from_peer(&request);

            let v_req = Self::parse_req(request)?;
            let raft = &self.meta_node.raft;

            let resp = raft.vote(v_req).await.map_err(Self::internal_err)?;

            Self::ok_response(resp)
        }
        .in_span(root)
        .await
    }
}
