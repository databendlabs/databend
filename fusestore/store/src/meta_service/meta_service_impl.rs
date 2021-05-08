// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::meta_service::GetReply;
use crate::meta_service::GetReq;
use crate::meta_service::Meta;
use crate::meta_service::MetaNode;
use crate::meta_service::MetaService;
use crate::meta_service::RaftMes;
use crate::meta_service::SetReply;
use crate::meta_service::SetReq;

pub struct MetaServiceImpl {
    pub meta_node: Arc<MetaNode>,
    // TODO(xp): move metadata into meta_node.
    pub metadata: Arc<Mutex<Meta>>
}

impl MetaServiceImpl {
    pub async fn create(meta_node: Arc<MetaNode>) -> Self {
        Self {
            meta_node,
            metadata: Arc::new(Mutex::new(Meta::empty()))
        }
    }
}

#[async_trait::async_trait]
impl MetaService for MetaServiceImpl {
    #[tracing::instrument(level = "info", skip(self))]
    async fn set(
        &self,
        request: tonic::Request<SetReq>
    ) -> Result<tonic::Response<SetReply>, tonic::Status> {
        let req = request.into_inner();
        let mut m = self.metadata.lock().await;
        if req.if_absent && m.keys.contains_key(&req.key) {
            return Ok(tonic::Response::new(SetReply { ok: false }));
        }
        m.keys.insert(req.key, req.value);
        return Ok(tonic::Response::new(SetReply { ok: true }));
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get(
        &self,
        request: tonic::Request<GetReq>
    ) -> Result<tonic::Response<GetReply>, tonic::Status> {
        let req = request.into_inner();
        let m = self.metadata.lock().await;
        let v = m.keys.get(&req.key);
        let rst = match v {
            Some(v) => GetReply {
                ok: true,
                key: req.key,
                value: v.clone()
            },
            None => GetReply {
                ok: false,
                key: req.key,
                value: "".into()
            }
        };

        Ok(tonic::Response::new(rst))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn append_entries(
        &self,
        request: tonic::Request<RaftMes>
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
        let req = request.into_inner();

        let ae_req = serde_json::from_slice(&req.data)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .append_entries(ae_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_vec(&resp).expect("fail to serialize resp");
        let mes = RaftMes { data };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn install_snapshot(
        &self,
        request: tonic::Request<RaftMes>
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
        let req = request.into_inner();

        let is_req = serde_json::from_slice(&req.data)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .install_snapshot(is_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_vec(&resp).expect("fail to serialize resp");
        let mes = RaftMes { data };

        Ok(tonic::Response::new(mes))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn vote(
        &self,
        request: tonic::Request<RaftMes>
    ) -> Result<tonic::Response<RaftMes>, tonic::Status> {
        let req = request.into_inner();

        let v_req = serde_json::from_slice(&req.data)
            .map_err(|x| tonic::Status::internal(x.to_string()))?;

        let resp = self
            .meta_node
            .raft
            .vote(v_req)
            .await
            .map_err(|x| tonic::Status::internal(x.to_string()))?;
        let data = serde_json::to_vec(&resp).expect("fail to serialize resp");
        let mes = RaftMes { data };

        Ok(tonic::Response::new(mes))
    }
}
