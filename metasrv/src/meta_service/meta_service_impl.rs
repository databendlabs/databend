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

//! Meta service impl a grpc server that serves both raft protocol: append_entries, vote and install_snapshot.
//! It also serves RPC for user-data access.

use std::convert::TryInto;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use common_meta_types::protobuf::raft_service_server::RaftService;
use common_meta_types::protobuf::GetReply;
use common_meta_types::protobuf::GetRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::AppliedState;
use common_meta_types::ForwardRequest;
use common_meta_types::LogEntry;
use common_meta_types::MetaError;
use common_meta_types::MetaRaftError;
use common_tracing::tracing;
use tonic::codegen::futures_core::Stream;

use crate::meta_service::ForwardRequestBody;
use crate::meta_service::MetaNode;
use crate::metrics::incr_meta_metrics_proposals_failed;
use crate::metrics::incr_meta_metrics_proposals_pending;
use crate::metrics::incr_meta_metrics_recv_bytes_from_peer;
use crate::metrics::incr_meta_metrics_snapshot_recv_failure_from_peer;
use crate::metrics::incr_meta_metrics_snapshot_recv_inflights_from_peer;
use crate::metrics::incr_meta_metrics_snapshot_recv_success_from_peer;
use crate::metrics::sample_meta_metrics_snapshot_recv;

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
            incr_meta_metrics_recv_bytes_from_peer(addr.to_string(), bytes);
        }
    }
}

#[async_trait::async_trait]
impl RaftService for RaftServiceImpl {
    /// Handles a write request.
    /// This node must be leader or an error returned.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn write(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let mes = request.into_inner();
        let ent: LogEntry = mes.try_into()?;

        incr_meta_metrics_proposals_pending(1);

        // TODO(xp): call meta_node.write()
        let res = self
            .meta_node
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Write(ent),
            })
            .await;

        incr_meta_metrics_proposals_pending(-1);

        let res = match res {
            Ok(r) => {
                let a: Result<AppliedState, MetaError> = r.try_into().map_err(|e: &str| {
                    incr_meta_metrics_proposals_failed();
                    MetaError::MetaRaftError(MetaRaftError::ForwardRequestError(e.to_string()))
                });
                a
            }
            Err(e) => {
                incr_meta_metrics_proposals_failed();
                Err(e)
            }
        };

        let raft_reply = RaftReply::from(res);
        return Ok(tonic::Response::new(raft_reply));
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetReply>, tonic::Status> {
        // TODO(xp): this method should be removed along with DFS
        common_tracing::extract_remote_span_as_parent(&request);

        // let req = request.into_inner();
        let rst = GetReply {
            ok: false,
            value: "".into(),
        };

        Ok(tonic::Response::new(rst))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn forward(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let admin_req: ForwardRequest = serde_json::from_str(&req.data)
            .map_err(|x| tonic::Status::invalid_argument(x.to_string()))?;

        let res = self.meta_node.handle_forwardable_request(admin_req).await;

        let raft_mes: RaftReply = res.into();

        Ok(tonic::Response::new(raft_mes))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        self.incr_meta_metrics_recv_bytes_from_peer(&request);
        let req = request.into_inner();

        let ae_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        let start = Instant::now();
        let addr = if let Some(addr) = request.remote_addr() {
            addr.to_string()
        } else {
            "unknown address".to_string()
        };
        common_tracing::extract_remote_span_as_parent(&request);

        self.incr_meta_metrics_recv_bytes_from_peer(&request);
        incr_meta_metrics_snapshot_recv_inflights_from_peer(addr.clone(), 1);
        let req = request.into_inner();

        let is_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()));

        sample_meta_metrics_snapshot_recv(addr.clone(), start.elapsed().as_secs() as f64);
        incr_meta_metrics_snapshot_recv_inflights_from_peer(addr.clone(), -1);

        match resp {
            Ok(resp) => {
                let data = serde_json::to_string(&resp).expect("fail to serialize resp");
                let mes = RaftReply {
                    data,
                    error: "".to_string(),
                };

                incr_meta_metrics_snapshot_recv_success_from_peer(addr.clone());
                return Ok(tonic::Response::new(mes));
            }
            Err(e) => {
                incr_meta_metrics_snapshot_recv_failure_from_peer(addr.clone());
                return Err(e);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, request))]
    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        self.incr_meta_metrics_recv_bytes_from_peer(&request);
        let req = request.into_inner();

        let v_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .vote(v_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
}
