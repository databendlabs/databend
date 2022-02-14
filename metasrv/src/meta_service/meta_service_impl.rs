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

pub type GrpcStream<T> =
    Pin<Box<dyn Stream<Item = Result<T, tonic::Status>> + Send + Sync + 'static>>;

pub struct RaftServiceImpl {
    pub meta_node: Arc<MetaNode>,
}

impl RaftServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self { meta_node }
    }
}

#[async_trait::async_trait]
impl RaftService for RaftServiceImpl {
    /// Handles a write request.
    /// This node must be leader or an error returned.
    #[tracing::instrument(level = "info", skip(self))]
    async fn write(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let mes = request.into_inner();
        let ent: LogEntry = mes.try_into()?;

        // TODO(xp): call meta_node.write()
        let res = self
            .meta_node
            .handle_forwardable_request(ForwardRequest {
                forward_to_leader: 1,
                body: ForwardRequestBody::Write(ent),
            })
            .await;

        let res = match res {
            Ok(r) => {
                let a: Result<AppliedState, MetaError> = r.try_into().map_err(|e: &str| {
                    MetaError::MetaRaftError(MetaRaftError::ForwardRequestError(e.to_string()))
                });
                match a {
                    Ok(applied_state) => Ok(applied_state),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        let raft_reply = RaftReply::from(res);
        return Ok(tonic::Response::new(raft_reply));
    }

    #[tracing::instrument(level = "info", skip(self))]
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

    #[tracing::instrument(level = "info", skip(self))]
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

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

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

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();

        let is_req =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = RaftReply {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn vote(
        &self,
        request: tonic::Request<RaftRequest>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

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
