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
use std::future::Future;
use std::time::Duration;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use databend_common_base::base::tokio;
use databend_common_base::future::TimingFutureExt;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::error::PayloadTooLarge;
use databend_common_meta_sled_store::openraft::error::ReplicationClosed;
use databend_common_meta_sled_store::openraft::error::Unreachable;
use databend_common_meta_sled_store::openraft::network::RPCOption;
use databend_common_meta_sled_store::openraft::MessageSummary;
use databend_common_meta_sled_store::openraft::RaftNetworkFactory;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequest;
use databend_common_meta_types::AppendEntriesRequest;
use databend_common_meta_types::AppendEntriesResponse;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::Fatal;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::InstallSnapshotError;
use databend_common_meta_types::InstallSnapshotRequest;
use databend_common_meta_types::InstallSnapshotResponse;
use databend_common_meta_types::MembershipNode;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::NetworkError;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::RPCError;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::RemoteError;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotResponse;
use databend_common_meta_types::StreamingError;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use databend_common_meta_types::VoteRequest;
use databend_common_meta_types::VoteResponse;
use databend_common_metrics::count::Count;
use log::debug;
use log::error;
use log::info;
use log::warn;
use openraft::RaftNetwork;
use tokio::sync::Mutex;

use crate::metrics::raft_metrics;
use crate::raft_client::RaftClient;
use crate::raft_client::RaftClientApi;
use crate::store::RaftStore;

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
            back_off_ratio: 1.5,
            back_off_min_delay: Duration::from_millis(50),
            back_off_max_delay: Duration::from_millis(1_000),
            back_off_chances: 10,
        }
    }
}

#[derive(Clone)]
pub struct Network {
    sto: RaftStore,

    backoff: Backoff,
}

impl Network {
    pub fn new(sto: RaftStore) -> Network {
        Network {
            sto,
            backoff: Backoff::default(),
        }
    }
}

pub struct NetworkConnection {
    /// This node id
    id: NodeId,

    /// The node id to send message to.
    target: NodeId,

    /// The node info to send message to.
    ///
    /// This is not used, because meta-service does not store node info in membership.
    target_node: MembershipNode,

    /// The endpoint of the target node.
    endpoint: Endpoint,

    client: Mutex<Option<RaftClient>>,

    sto: RaftStore,

    backoff: Backoff,
}

impl NetworkConnection {
    /// Create a new RaftClient to the specified target node.
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    pub async fn new_client(&self, addr: &str) -> Result<RaftClient, tonic::transport::Error> {
        info!(id = self.id; "Raft NetworkConnection connect: target={}: {}", self.target, addr);

        let channel = tonic::transport::Endpoint::new(addr.to_string())?
            .connect()
            .await?;

        let client = RaftClientApi::new(self.target, self.endpoint.clone(), channel);

        info!(
            "Raft NetworkConnection connected to: target={}: {}",
            self.target, addr
        );

        Ok(client)
    }

    /// Take the last used client or create a new one.
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn take_client(&mut self) -> Result<RaftClient, Unreachable> {
        let mut client = self.client.lock().await;

        if let Some(c) = client.take() {
            return Ok(c);
        }

        let n = 3;
        for _i in 0..n {
            let endpoint = self.lookup_target_address().await.map_err(|e| {
                let any_err = AnyError::new(&e).add_context(|| {
                    format!(
                        "Raft NetworkConnection fail to lookup target address: target={}",
                        self.target
                    )
                });
                warn!("{}", any_err);
                Unreachable::new(&any_err)
            })?;

            self.endpoint = endpoint;

            let addr = format!("http://{}", self.endpoint);

            let res = self.new_client(&addr).await;
            match res {
                Ok(c) => {
                    return Ok(c);
                }
                Err(e) => {
                    warn!(
                        "Raft NetworkConnection fail to connect: target={}: addr={}: {}",
                        self.target, &addr, e
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        let any_err = AnyError::error(format!(
            "Raft NetworkConnection fail to connect: target={}, retry={}",
            self.target, n
        ));
        error!("{}", any_err);

        Err(Unreachable::new(&any_err))
    }

    async fn lookup_target_address(&self) -> Result<Endpoint, MetaNetworkError> {
        let endpoint = self.sto.get_node_raft_endpoint(&self.target).await?;

        Ok(endpoint)
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

    /// Create a new RaftRequest for AppendEntriesRequest,
    /// if it is too large, return `PayloadTooLarge` error
    /// to tell Openraft to split it in to smaller chunks.
    fn new_append_entries_raft_req<E>(
        &self,
        rpc: &AppendEntriesRequest,
    ) -> Result<RaftRequest, RPCError<E>>
    where
        E: std::error::Error,
    {
        let raft_req = GrpcHelper::encode_raft_request(rpc).map_err(|e| Unreachable::new(&e))?;

        if raft_req.data.len() <= GrpcConfig::advisory_encoding_size() {
            return Ok(raft_req);
        }

        // data.len() is too large

        let l = rpc.entries.len();
        if l == 0 {
            // impossible.
            Ok(raft_req)
        } else if l == 1 {
            warn!(
                "append_entries req too large: target={}, len={}, can not split",
                self.target,
                raft_req.data.len()
            );
            // can not split, just try to send this big request
            Ok(raft_req)
        } else {
            // l > 1
            let n = std::cmp::max(1, l / 2);
            warn!(
                "append_entries req too large: target={}, len={}, reduce NO entries from {} to {}",
                self.target,
                raft_req.data.len(),
                l,
                n
            );
            Err(PayloadTooLarge::new_entries_hint(n as u64).into())
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

    fn parse_grpc_resp<R, E>(
        &self,
        grpc_res: Result<tonic::Response<RaftReply>, tonic::Status>,
    ) -> Result<R, RPCError<RaftError<E>>>
    where
        R: serde::de::DeserializeOwned + 'static,
        E: serde::de::DeserializeOwned + 'static,
        E: std::error::Error,
    {
        // Return status error
        let resp = grpc_res.map_err(|e| self.status_to_unreachable(e))?;

        // Parse serialized response into `Result<RaftReply.data, RaftReply.error>`
        let raft_res = GrpcHelper::parse_raft_reply::<R, E>(resp).map_err(|serde_err| {
            new_net_err(&serde_err, || {
                let t = std::any::type_name::<R>();
                format!("parse reply for {}", t)
            })
        })?;

        // Wrap RaftError with RPCError
        raft_res.map_err(|e| self.to_rpc_err(e))
    }

    /// Convert gRPC status to `RPCError`
    fn status_to_unreachable<E>(&self, status: tonic::Status) -> RPCError<RaftError<E>>
    where E: std::error::Error {
        warn!(
            "target={}, endpoint={} gRPC error: {:?}",
            self.target, self.endpoint, status
        );

        let any_err = AnyError::new(&status)
            .add_context(|| format!("gRPC target={}, endpoint={}", self.target, self.endpoint));

        RPCError::Unreachable(Unreachable::new(&any_err))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError<RaftError>> {
        debug!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_append_entries",
        );

        let raft_req = self.new_append_entries_raft_req(&rpc)?;
        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let mut client = self.take_client().await?;

        let grpc_res = client
            .append_entries(req)
            .timed(observe_append_send_spent(self.target))
            .await;
        debug!(
            "append_entries resp from: target={}: {:?}",
            self.target, grpc_res
        );

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!(target = self.target, rpc = rpc.summary(); "append_entries failed: {}", e);
            }
        }

        self.parse_grpc_resp(grpc_res)
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError<Fatal>> {
        // This implementation just delegates to `Chunked::send_snapshot`,
        // which depends on `Self::install_snapshot()` to send chunks.
        use openraft::network::snapshot_transport::Chunked;
        use openraft::network::snapshot_transport::SnapshotTransport;

        let resp = Chunked::send_snapshot(self, vote, snapshot, cancel, option).await?;
        Ok(resp)
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse, RPCError<RaftError<InstallSnapshotError>>> {
        info!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_install_snapshot"
        );

        let _g = snapshot_send_inflight(self.target).counter_guard();

        let bytes = rpc.data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let v1_req = SnapshotChunkRequest::new_v1(rpc);
        let req = databend_common_tracing::inject_span_to_tonic_request(v1_req);

        let mut client = self.take_client().await?;

        let grpc_res = client
            .install_snapshot_v1(req)
            .timed(observe_snapshot_send_spent(self.target))
            .await;

        info!(
            "install_snapshot resp target={}: {:?}",
            self.target, grpc_res
        );

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!(target = self.target; "install_snapshot failed: {}", e);
            }
        }

        let res = self.parse_grpc_resp(grpc_res);
        self.report_metrics_snapshot(res.is_ok());

        res
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn vote(
        &mut self,
        rpc: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError<RaftError>> {
        info!(id = self.id, target = self.target, rpc = rpc.summary(); "send_vote");

        let raft_req = GrpcHelper::encode_raft_request(&rpc).map_err(|e| Unreachable::new(&e))?;

        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let mut client = self.take_client().await?;

        let grpc_res = client.vote(req).await;
        info!("vote: resp from target={} {:?}", self.target, grpc_res);

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!(target = self.target, rpc = rpc.summary(); "vote failed: {}", e);
            }
        }

        self.parse_grpc_resp(grpc_res)
    }

    /// When a `Unreachable` error is returned from the `Network`,
    /// Openraft will call this method to build a backoff instance.
    fn backoff(&self) -> openraft::network::Backoff {
        warn!("backoff is required: target={}", self.target);
        openraft::network::Backoff::new(self.back_off())
    }
}

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
            sto: self.sto.clone(),
            backoff: self.backoff.clone(),
            endpoint: Default::default(),
            client: Default::default(),
        }
    }
}

fn new_net_err<D: Display>(
    e: &(impl std::error::Error + 'static),
    msg: impl FnOnce() -> D,
) -> NetworkError {
    NetworkError::new(&AnyError::new(e).add_context(msg))
}

/// Create a function record the time cost of append sending.
fn observe_append_send_spent(target: NodeId) -> impl Fn(Duration, Duration) {
    move |t, _b| {
        raft_metrics::network::observe_append_sendto_spent(&target, t.as_secs() as f64);
    }
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
