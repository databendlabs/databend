// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_raft::async_trait::async_trait;
use async_raft::raft::AppendEntriesRequest;
use async_raft::raft::AppendEntriesResponse;
use async_raft::raft::InstallSnapshotRequest;
use async_raft::raft::InstallSnapshotResponse;
use async_raft::raft::VoteRequest;
use async_raft::raft::VoteResponse;
use async_raft::NodeId;
use async_raft::RaftNetwork;
use common_tracing::tracing;
use tonic::transport::channel::Channel;

use crate::meta_service::LogEntry;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaStore;
use crate::meta_service::RaftMes;
use crate::meta_service::RetryableError;

impl tonic::IntoRequest<RaftMes> for AppendEntriesRequest<LogEntry> {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}
impl tonic::IntoRequest<RaftMes> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}
impl tonic::IntoRequest<RaftMes> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
            error: "".to_string(),
        };
        tonic::Request::new(mes)
    }
}

impl From<RetryableError> for RaftMes {
    fn from(err: RetryableError) -> Self {
        let error = serde_json::to_string(&err).expect("fail to serialize");
        RaftMes {
            data: "".to_string(),
            error,
        }
    }
}

pub struct Network {
    sto: Arc<MetaStore>,
}

impl Network {
    pub fn new(sto: Arc<MetaStore>) -> Network {
        Network { sto }
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.sto.id))]
    pub async fn make_client(
        &self,
        node_id: &NodeId,
    ) -> anyhow::Result<MetaServiceClient<Channel>> {
        let addr = self.sto.get_node_addr(node_id).await?;
        tracing::info!("connect: id={}: {}", node_id, addr);
        let client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
        tracing::info!("connected: id={}: {}", node_id, addr);
        Ok(client)
    }
}

#[async_trait]
impl RaftNetwork<LogEntry> for Network {
    #[tracing::instrument(level = "info", skip(self), fields(myid=self.sto.id))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<LogEntry>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        tracing::debug!("append_entries req to: id={}: {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;
        let resp = client.append_entries(rpc).await;
        tracing::debug!("append_entries resp from: id={}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.sto.id))]
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        tracing::debug!("install_snapshot req to: id={}", target);

        let mut client = self.make_client(&target).await?;
        let resp = client.install_snapshot(rpc).await;
        tracing::debug!("install_snapshot resp from: id={}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.sto.id))]
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        tracing::debug!("vote req to: id={} {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;
        let resp = client.vote(rpc).await;
        tracing::info!("vote: resp from id={} {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }
}
