// Copyright 2020 Datafuse Labs.
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
use std::sync::Arc;

use common_tracing::tracing;

use crate::meta_service::GetReply;
use crate::meta_service::GetReq;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaService;
use crate::meta_service::RaftMes;
use crate::raft::types::LogEntry;

pub struct MetaServiceImpl {
    pub meta_node: Arc<MetaNode>,
}

impl MetaServiceImpl {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        Self { meta_node }
    }
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    /// Handles a write request.
    /// This node must be leader or an error returned.
    #[tracing::instrument(level = "info", skip(self))]
    async fn write(
        &self,
        request: tonic::Request<RaftMes>,
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let mes = request.into_inner();
        let req: LogEntry = mes.try_into()?;

        let rst = self
            .meta_node
            .write_to_local_leader(req)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let raft_mes = rst.into();
        Ok(tonic::Response::new(raft_mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get(
        &self,
        request: tonic::Request<GetReq>,
    ) -> Result<tonic::Response<GetReply>, tonic::Status> {
        common_tracing::extract_remote_span_as_parent(&request);

        let req = request.into_inner();
        let resp = self.meta_node.get_file(&req.key).await?;
        let rst = match resp {
            Some(v) => GetReply {
                ok: true,
                key: req.key,
                value: v,
            },
            None => GetReply {
                ok: false,
                key: req.key,
                value: "".into(),
            },
        };

        Ok(tonic::Response::new(rst))
    }

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMes>,
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
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
        let mes = RaftMes {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftMes>,
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
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
        let mes = RaftMes {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self, request))]
    async fn vote(
        &self,
        request: tonic::Request<RaftMes>,
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
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
        let mes = RaftMes {
            data,
            error: "".to_string(),
        };

        Ok(tonic::Response::new(mes))
    }
}
