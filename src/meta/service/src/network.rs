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
use std::time::Instant;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use common_base::base::tokio::time::sleep;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::error::NetworkError;
use common_meta_sled_store::openraft::MessageSummary;
use common_meta_sled_store::openraft::RaftNetworkFactory;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::AppendEntriesRequest;
use common_meta_types::AppendEntriesResponse;
use common_meta_types::InstallSnapshotError;
use common_meta_types::InstallSnapshotRequest;
use common_meta_types::InstallSnapshotResponse;
use common_meta_types::MembershipNode;
use common_meta_types::MetaNetworkError;
use common_meta_types::NodeId;
use common_meta_types::RPCError;
use common_meta_types::RaftError;
use common_meta_types::TypeConfig;
use common_meta_types::VoteRequest;
use common_meta_types::VoteResponse;
use openraft::async_trait::async_trait;
use openraft::RaftNetwork;
use tonic::client::GrpcService;
use tonic::transport::channel::Channel;
use tracing::debug;
use tracing::info;

use crate::metrics::raft_metrics;
use crate::raft_client::RaftClient;
use crate::raft_client::RaftClientApi;
use crate::store::RaftStore;

#[derive(Debug)]
struct ChannelManager {}

#[async_trait]
impl ItemManager for ChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = tonic::transport::Error;

    #[tracing::instrument(level = "info", err(Debug))]
    async fn build(&self, addr: &Self::Key) -> Result<Channel, tonic::transport::Error> {
        tonic::transport::Endpoint::new(addr.clone())?
            .connect()
            .await
    }

    #[tracing::instrument(level = "debug", err(Debug))]
    async fn check(&self, mut ch: Channel) -> Result<Channel, tonic::transport::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx)).await?;
        Ok(ch)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    /// delay increase ratio of meta
    ///
    /// should be not little than 1.0
    back_off_ratio: f32,
    /// min delay duration of back off
    back_off_min_delay: Duration,
    /// max delay duration of back off
    back_off_max_delay: Duration,
    /// chances of back off
    back_off_chances: u64,
}

impl Backoff {
    /// Set exponential back off policy for meta service
    ///
    /// - `ratio`: delay increase ratio of meta
    ///
    ///   should be not smaller than 1.0
    /// - `min_delay`: minimum back off duration, where the backoff duration vary starts from
    /// - `max_delay`: maximum back off duration, if the backoff duration is larger than this, no backoff will be raised
    /// - `chances`: maximum back off times, chances off backoff
    #[allow(dead_code)]
    pub fn with_back_off_policy(
        mut self,
        ratio: f32,
        min_delay: Duration,
        max_delay: Duration,
        chances: u64,
    ) -> Self {
        self.back_off_ratio = ratio;
        self.back_off_min_delay = min_delay;
        self.back_off_max_delay = max_delay;
        self.back_off_chances = chances;
        self
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            back_off_ratio: 2.0,
            back_off_min_delay: Duration::from_secs(1),
            back_off_max_delay: Duration::from_secs(60),
            back_off_chances: 3,
        }
    }
}

#[derive(Clone)]
pub struct Network {
    sto: RaftStore,

    conn_pool: Arc<Pool<ChannelManager>>,

    backoff: Backoff,
}

impl Network {
    pub fn new(sto: RaftStore) -> Network {
        let mgr = ChannelManager {};
        Network {
            sto,
            conn_pool: Arc::new(Pool::new(mgr, Duration::from_millis(50))),
            backoff: Backoff::default(),
        }
    }

    fn incr_meta_metrics_sent_bytes_to_peer(target: &NodeId, message: &RaftRequest) {
        let bytes = message.data.len() as u64;
        raft_metrics::network::incr_sent_bytes_to_peer(target, bytes);
    }
}

pub struct NetworkConnection {
    id: NodeId,
    target: NodeId,

    // TODO: this will be used when learners is upgrade to be stored in Membership
    #[allow(dead_code)]
    target_node: MembershipNode,

    sto: RaftStore,
    conn_pool: Arc<Pool<ChannelManager>>,
    backoff: Backoff,
}

impl NetworkConnection {
    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id), err(Debug))]
    pub async fn make_client(&self) -> Result<RaftClient, MetaNetworkError> {
        let target = self.target;

        let endpoint = self
            .sto
            .get_node_endpoint(&target)
            .await
            .map_err(|e| MetaNetworkError::GetNodeAddrError(e.to_string()))?;

        let addr = format!("http://{}", endpoint);

        debug!("connect: target={}: {}", target, addr);

        match self.conn_pool.get(&addr).await {
            Ok(channel) => {
                let client = RaftClientApi::new(target, endpoint, channel);
                debug!("connected: target={}: {}", target, addr);

                Ok(client)
            }
            Err(err) => {
                raft_metrics::network::incr_fail_connections_to_peer(
                    &target,
                    &endpoint.to_string(),
                );
                Err(err.into())
            }
        }
    }

    pub(crate) fn back_off(&self) -> impl Iterator<Item = Duration> {
        let policy = ExponentialBuilder::default()
            .with_factor(self.backoff.back_off_ratio)
            .with_min_delay(self.backoff.back_off_min_delay)
            .with_max_delay(self.backoff.back_off_max_delay)
            .with_max_times(self.backoff.back_off_chances as usize)
            .build();
        // the last period of back off should be zero
        // so the longest back off will not be wasted
        let zero = vec![Duration::default()].into_iter();
        policy.chain(zero)
    }
}

#[async_trait]
impl RaftNetwork<TypeConfig> for NetworkConnection {
    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id, target=self.target), err(Debug))]
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RPCError<RaftError>> {
        debug!(
            "send_append_entries target: {}, rpc: {}",
            self.target,
            rpc.summary()
        );

        let mut client = self
            .make_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let mut last_err = None;

        for back_off in self.back_off() {
            let req = common_tracing::inject_span_to_tonic_request(&rpc);

            Network::incr_meta_metrics_sent_bytes_to_peer(&self.target, req.get_ref());

            let resp = client.append_entries(req).await;
            debug!(
                "append_entries resp from: target={}: {:?}",
                self.target, resp
            );

            match resp {
                Ok(resp) => {
                    let mes = resp.into_inner();
                    match serde_json::from_str(&mes.data) {
                        Ok(resp) => return Ok(resp),
                        Err(e) => {
                            // parsing error, won't increase send failures
                            last_err = Some(openraft::error::NetworkError::new(
                                &AnyError::new(&e).add_context(|| "send_append_entries"),
                            ));
                            // backoff and retry sending
                            sleep(back_off).await;
                        }
                    }
                }
                Err(e) => {
                    raft_metrics::network::incr_sent_failure_to_peer(&self.target);
                    last_err = Some(openraft::error::NetworkError::new(
                        &AnyError::new(&e).add_context(|| "send_append_entries"),
                    ));
                    // backoff and retry sending
                    sleep(back_off).await;
                }
            }
        }
        if let Some(net_err) = last_err {
            Err(RPCError::Network(net_err))
        } else {
            Err(RPCError::Network(NetworkError::new(&AnyError::error(
                "backoff does not send send_append_entries RPC",
            ))))
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id, target=self.target), err(Debug))]
    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RPCError<RaftError<InstallSnapshotError>>> {
        info!(
            "send_install_snapshot target: {}, rpc: {}",
            self.target,
            rpc.summary()
        );

        let start = Instant::now();
        let mut client = self
            .make_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let mut last_err = None;

        for back_off in self.back_off() {
            let req = common_tracing::inject_span_to_tonic_request(&rpc);

            Network::incr_meta_metrics_sent_bytes_to_peer(&self.target, req.get_ref());
            raft_metrics::network::incr_snapshot_send_inflights_to_peer(&self.target, 1);

            let resp = client.install_snapshot(req).await;
            info!(
                "install_snapshot resp from: target={}: {:?}",
                self.target, resp
            );

            match resp {
                Ok(resp) => {
                    raft_metrics::network::incr_snapshot_send_inflights_to_peer(&self.target, -1);
                    raft_metrics::network::incr_snapshot_send_success_to_peer(&self.target);
                    let mes = resp.into_inner();
                    match serde_json::from_str(&mes.data) {
                        Ok(resp) => {
                            raft_metrics::network::sample_snapshot_sent(
                                &self.target,
                                start.elapsed().as_secs() as f64,
                            );

                            return Ok(resp);
                        }
                        Err(e) => {
                            // parsing error, won't increase sending failures
                            last_err = Some(openraft::error::NetworkError::new(
                                &AnyError::new(&e).add_context(|| "send_install_snapshot"),
                            ));
                            // back off and retry sending
                            sleep(back_off).await;
                        }
                    }
                }
                Err(e) => {
                    raft_metrics::network::incr_sent_failure_to_peer(&self.target);
                    raft_metrics::network::incr_snapshot_send_failures_to_peer(&self.target);
                    last_err = Some(openraft::error::NetworkError::new(
                        &AnyError::new(&e).add_context(|| "send_install_snapshot"),
                    ));

                    // back off and retry sending
                    sleep(back_off).await;
                }
            }
        }

        if let Some(net_err) = last_err {
            Err(RPCError::Network(net_err))
        } else {
            Err(RPCError::Network(NetworkError::new(&AnyError::error(
                "backoff does not send send_install_snapshot RPC",
            ))))
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id, target=self.target), err(Debug))]
    async fn send_vote(&mut self, rpc: VoteRequest) -> Result<VoteResponse, RPCError<RaftError>> {
        info!("send_vote: target: {} rpc: {}", self.target, rpc.summary());

        let mut client = self
            .make_client()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let mut last_err = None;

        for back_off in self.back_off() {
            let req = common_tracing::inject_span_to_tonic_request(&rpc);

            Network::incr_meta_metrics_sent_bytes_to_peer(&self.target, req.get_ref());

            let resp = client.vote(req).await;
            info!("vote: resp from target={} {:?}", self.target, resp);

            match resp {
                Ok(resp) => {
                    let mes = resp.into_inner();
                    match serde_json::from_str(&mes.data) {
                        Ok(resp) => return Ok(resp),
                        Err(e) => {
                            // parsing error, won't increase sending errors
                            last_err = Some(openraft::error::NetworkError::new(
                                &AnyError::new(&e).add_context(|| "send_vote"),
                            ));
                            // back off and retry
                            sleep(back_off).await;
                        }
                    }
                }
                Err(e) => {
                    raft_metrics::network::incr_sent_failure_to_peer(&self.target);
                    last_err = Some(openraft::error::NetworkError::new(
                        &AnyError::new(&e).add_context(|| "send_vote"),
                    ));
                    // back off and retry
                    sleep(back_off).await;
                }
            }
        }

        if let Some(net_err) = last_err {
            Err(RPCError::Network(net_err))
        } else {
            Err(RPCError::Network(NetworkError::new(&AnyError::error(
                "backoff does not send send_vote RPC",
            ))))
        }
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(
        self: &mut Network,
        target: NodeId,
        node: &MembershipNode,
    ) -> Self::Network {
        tracing::info!(
            "new raft communication client: id:{}, target:{}, node:{}",
            self.sto.id,
            target,
            node
        );

        NetworkConnection {
            id: self.sto.id,
            target,
            target_node: node.clone(),
            sto: self.sto.clone(),
            conn_pool: self.conn_pool.clone(),
            backoff: self.backoff.clone(),
        }
    }
}
