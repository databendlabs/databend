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

use std::sync::Arc;
use std::time::Duration;

use async_raft::async_trait::async_trait;
use async_raft::raft::AppendEntriesRequest;
use async_raft::raft::AppendEntriesResponse;
use async_raft::raft::InstallSnapshotRequest;
use async_raft::raft::InstallSnapshotResponse;
use async_raft::raft::VoteRequest;
use async_raft::raft::VoteResponse;
use async_raft::RaftNetwork;
use common_containers::ItemManager;
use common_containers::Pool;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::LogEntry;
use common_meta_types::NodeId;
use common_tracing::tracing;
use tonic::client::GrpcService;
use tonic::transport::channel::Channel;

use crate::store::MetaRaftStore;

struct ChannelManager {}

#[async_trait]
impl ItemManager for ChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = tonic::transport::Error;

    async fn build(&self, addr: &Self::Key) -> Result<Channel, tonic::transport::Error> {
        tonic::transport::Endpoint::new(addr.clone())?
            .connect()
            .await
    }

    async fn check(&self, mut ch: Channel) -> Result<Channel, tonic::transport::Error> {
        futures::future::poll_fn(|cx| (&mut ch).poll_ready(cx)).await?;
        Ok(ch)
    }
}

pub struct Network {
    sto: Arc<MetaRaftStore>,

    conn_pool: Pool<ChannelManager>,
}

impl Network {
    pub fn new(sto: Arc<MetaRaftStore>) -> Network {
        let mgr = ChannelManager {};
        Network {
            sto,
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
        }
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.sto.id))]
    pub async fn make_client(&self, target: &NodeId) -> anyhow::Result<MetaServiceClient<Channel>> {
        let addr = self.sto.get_node_addr(target).await?;
        let addr = format!("http://{}", addr);

        tracing::debug!("connect: target={}: {}", target, addr);

        let channel = self.conn_pool.get(&addr).await?;
        let client = MetaServiceClient::new(channel);

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
        tracing::debug!("vote: req to: target={} {:?}", target, rpc);

        let mut client = self.make_client(&target).await?;
        let req = common_tracing::inject_span_to_tonic_request(rpc);
        let resp = client.vote(req).await;
        tracing::info!("vote: resp from target={} {:?}", target, resp);

        let resp = resp?;
        let mes = resp.into_inner();
        let resp = serde_json::from_str(&mes.data)?;

        Ok(resp)
    }
}
