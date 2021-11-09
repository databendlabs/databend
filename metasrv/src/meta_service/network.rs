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

use std::sync::Arc;

use async_raft::async_trait::async_trait;
use async_raft::raft::AppendEntriesRequest;
use async_raft::raft::AppendEntriesResponse;
use async_raft::raft::InstallSnapshotRequest;
use async_raft::raft::InstallSnapshotResponse;
use async_raft::raft::VoteRequest;
use async_raft::raft::VoteResponse;
use async_raft::RaftNetwork;
use common_meta_types::LogEntry;
use common_meta_types::NodeId;
use common_tracing::tracing;
use tonic::transport::channel::Channel;

use crate::proto::meta_service_client::MetaServiceClient;
use crate::store::MetaRaftStore;

pub struct Network {
    sto: Arc<MetaRaftStore>,
}

impl Network {
    pub fn new(sto: Arc<MetaRaftStore>) -> Network {
        Network { sto }
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.sto.id))]
    pub async fn make_client(&self, target: &NodeId) -> anyhow::Result<MetaServiceClient<Channel>> {
        let addr = self.sto.get_node_addr(target).await?;

        tracing::info!("connect: target={}: {}", target, addr);

        let client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

        tracing::info!("connected: target={}: {}", target, addr);

        Ok(client)
    }
}

#[async_trait]
impl RaftNetwork<LogEntry> for Network {
    #[tracing::instrument(level = "debug", skip(self), fields(id=self.sto.id))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<LogEntry>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        tracing::debug!("append_entries req to: id={}: {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;

        let req = common_tracing::inject_span_to_tonic_request(rpc);

        let resp = client.append_entries(req).await;
        tracing::debug!("append_entries resp from: id={}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.sto.id))]
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        tracing::debug!("install_snapshot req to: id={}", target);

        let mut client = self.make_client(&target).await?;
        let req = common_tracing::inject_span_to_tonic_request(rpc);
        let resp = client.install_snapshot(req).await;
        tracing::debug!("install_snapshot resp from: id={}: {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.sto.id))]
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        tracing::debug!("vote req to: id={} {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;
        let req = common_tracing::inject_span_to_tonic_request(rpc);
        let resp = client.vote(req).await;
        tracing::info!("vote: resp from id={} {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }
}
