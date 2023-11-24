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

use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use common_base::base::tokio::time::sleep;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_base::future::TimingFutureExt;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::MessageSummary;
use common_meta_sled_store::openraft::RaftNetworkFactory;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::SnapshotChunkRequest;
use common_meta_types::AppendEntriesRequest;
use common_meta_types::AppendEntriesResponse;
use common_meta_types::InstallSnapshotError;
use common_meta_types::InstallSnapshotRequest;
use common_meta_types::InstallSnapshotResponse;
use common_meta_types::MembershipNode;
use common_meta_types::MetaNetworkError;
use common_meta_types::NetworkError;
use common_meta_types::NodeId;
use common_meta_types::RPCError;
use common_meta_types::RaftError;
use common_meta_types::RemoteError;
use common_meta_types::TypeConfig;
use common_meta_types::VoteRequest;
use common_meta_types::VoteResponse;
use common_metrics::count::Count;
use log::debug;
use log::info;
use log::warn;
use openraft::async_trait::async_trait;
use openraft::RaftNetwork;
use tonic::client::GrpcService;
use tonic::transport::channel::Channel;
use tonic::Code;

use crate::grpc_helper::GrpcHelper;
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

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn build(&self, addr: &Self::Key) -> Result<Channel, tonic::transport::Error> {
        tonic::transport::Endpoint::new(addr.clone())?
            .connect()
            .await
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
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
        raft_metrics::network::incr_sendto_bytes(target, bytes);
    }
}

pub struct NetworkConnection {
    /// This node id
    id: NodeId,

    /// The node id to send message to.
    target: NodeId,

    /// The node info to send message to.
    target_node: MembershipNode,

    /// A counter to send snapshot via v0 API.
    ///
    /// v0 API should only be used during upgrading a meta cluster.
    /// During this period, i.e., this counter is `>0`,
    /// try to send via v0 if the remote is not upgraded.
    /// When this counter reaches 0, start sending via v1 API.
    install_snapshot_via_v0: u64,

    sto: RaftStore,

    conn_pool: Arc<Pool<ChannelManager>>,

    backoff: Backoff,
}

impl NetworkConnection {
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    pub async fn make_client(&self) -> Result<RaftClient, MetaNetworkError> {
        let target = self.target;

        let endpoint = self
            .sto
            .get_node_endpoint(&target)
            .await
            .map_err(|e| MetaNetworkError::GetNodeAddrError(e.to_string()))?;

        let addr = format!("http://{}", endpoint);

        debug!(id = self.id; "connect: target={}: {}", target, addr);

        match self.conn_pool.get(&addr).await {
            Ok(channel) => {
                let client = RaftClientApi::new(target, endpoint, channel);
                debug!("connected: target={}: {}", target, addr);

                Ok(client)
            }
            Err(err) => {
                raft_metrics::network::incr_connect_failure(&target, &endpoint.to_string());
                Err(err.into())
            }
        }
    }

    pub(crate) fn report_metrics_snapshot(&self, success: bool) {
        raft_metrics::network::incr_sendto_result(&self.target, success);
        raft_metrics::network::incr_snapshot_sendto_result(&self.target, success);
    }

    /// Wrap a RaftError with RPCError
    pub(crate) fn to_rpc_err<E: Error>(&self, e: RaftError<E>) -> RPCError<RaftError<E>> {
        let remote_err = RemoteError::new_with_node(self.target, self.target_node.clone(), e);
        RPCError::RemoteError(remote_err)
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
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn send_append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RPCError<RaftError>> {
        debug!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_append_entries",
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
                        Err(serde_err) => {
                            // parsing error, won't increase send failures
                            last_err = Some(NetworkError::new(
                                &AnyError::new(&serde_err).add_context(|| "send_append_entries"),
                            ));
                            // backoff and retry sending
                            sleep(back_off).await;
                        }
                    }
                }
                Err(status) => {
                    raft_metrics::network::incr_sendto_failure(&self.target);
                    last_err = Some(NetworkError::new(
                        &AnyError::new(&status).add_context(|| "send_append_entries"),
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

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn send_install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RPCError<RaftError<InstallSnapshotError>>> {
        info!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_install_snapshot"
        );

        let _g = snapshot_send_inflight(self.target).counter_guard();
        let bytes = rpc.data.len() as u64;

        let mut client = self
            .make_client()
            .await
            .map_err(|e| NetworkError::new(&e))?;

        let mut last_err = None;

        // It consumes a loop to retry sending install_snapshot via v0 API.
        // Thus add another backoff of 0.
        for back_off in [Duration::from_millis(0)]
            .into_iter()
            .chain(self.back_off())
        {
            raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

            // Try send via `v1` API, if the remote peer does not provide `v1` API,
            // revert to `v0` API.

            let res = if self.install_snapshot_via_v0 == 0 {
                // Send via v1 API

                let v1_req = SnapshotChunkRequest::new_v1(rpc.clone());
                let req = common_tracing::inject_span_to_tonic_request(v1_req);
                let res = client
                    .install_snapshot_v1(req)
                    .timed(observe_snapshot_send_spent(self.target))
                    .await;

                if let Err(ref status) = res {
                    if status.code() == Code::Unimplemented {
                        warn!(
                            "target={} does not support install_snapshot_v1 API, fallback to v0 API for next 10 times",
                            self.target
                        );
                        // The remote peer may not be upgraded yet, try to send via v0 API for the
                        // next 10 install_snapshot RPC and retry at once.
                        self.install_snapshot_via_v0 = 10;
                        continue;
                    }
                }
                res
            } else {
                // Send via v0 API

                self.install_snapshot_via_v0 -= 1;

                let req = common_tracing::inject_span_to_tonic_request(rpc.clone());
                client
                    .install_snapshot(req)
                    .timed(observe_snapshot_send_spent(self.target))
                    .await
            };

            info!("install_snapshot resp target={}: {:?}", self.target, res);

            let resp = match res {
                Ok(x) => x,
                Err(status) => {
                    self.report_metrics_snapshot(false);
                    last_err = Some(new_net_err(&status, || "send_install_snapshot"));
                    sleep(back_off).await;
                    continue;
                }
            };

            let rpc_res = GrpcHelper::parse_raft_reply(resp)
                .map_err(|serde_err| new_net_err(&serde_err, || "parse install_snapshot reply"))?;

            self.report_metrics_snapshot(rpc_res.is_ok());

            return rpc_res.map_err(|e| self.to_rpc_err(e));
        }

        if let Some(net_err) = last_err {
            Err(RPCError::Network(net_err))
        } else {
            Err(RPCError::Network(NetworkError::new(&AnyError::error(
                "backoff does not send send_install_snapshot RPC",
            ))))
        }
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn send_vote(&mut self, rpc: VoteRequest) -> Result<VoteResponse, RPCError<RaftError>> {
        info!(id = self.id, target = self.target, rpc = rpc.summary(); "send_vote");

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
                            last_err = Some(NetworkError::new(
                                &AnyError::new(&e).add_context(|| "send_vote"),
                            ));
                            // back off and retry
                            sleep(back_off).await;
                        }
                    }
                }
                Err(e) => {
                    raft_metrics::network::incr_sendto_failure(&self.target);
                    last_err = Some(NetworkError::new(
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
        info!(
            "new raft communication client: id:{}, target:{}, node:{}",
            self.sto.id, target, node
        );

        NetworkConnection {
            id: self.sto.id,
            target,
            target_node: node.clone(),
            install_snapshot_via_v0: 0,
            sto: self.sto.clone(),
            conn_pool: self.conn_pool.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

fn new_net_err<D: Display>(
    e: &(impl std::error::Error + 'static),
    msg: impl FnOnce() -> D,
) -> NetworkError {
    NetworkError::new(&AnyError::new(e).add_context(msg))
}

/// Create a function record the time cost of snapshot sending.
fn observe_snapshot_send_spent(target: NodeId) -> impl Fn(Duration, Duration) {
    move |t, _b| {
        raft_metrics::network::observe_snapshot_sendto_spent(&target, t.as_secs() as f64);
    }
}

/// Create a function that increases metric value of inflight snapshot sending.
fn snapshot_send_inflight(target: NodeId) -> impl FnMut(i64) {
    move |i: i64| raft_metrics::network::incr_snapshot_sendto_inflight(&target, i)
}
