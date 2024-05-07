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
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anyerror::func_name;
use anyerror::AnyError;
use async_trait::async_trait;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::containers::ItemManager;
use databend_common_base::containers::Pool;
use databend_common_base::future::TimingFutureExt;
use databend_common_base::runtime;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::error::PayloadTooLarge;
use databend_common_meta_sled_store::openraft::error::ReplicationClosed;
use databend_common_meta_sled_store::openraft::error::Unreachable;
use databend_common_meta_sled_store::openraft::network::RPCOption;
use databend_common_meta_sled_store::openraft::MessageSummary;
use databend_common_meta_sled_store::openraft::RaftNetworkFactory;
use databend_common_meta_sled_store::openraft::StorageError;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::RaftRequest;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_common_meta_types::AppendEntriesRequest;
use databend_common_meta_types::AppendEntriesResponse;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::Fatal;
use databend_common_meta_types::GrpcConfig;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::MembershipNode;
use databend_common_meta_types::NetworkError;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::RPCError;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::RemoteError;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotResponse;
use databend_common_meta_types::StorageIOError;
use databend_common_meta_types::StreamingError;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use databend_common_meta_types::VoteRequest;
use databend_common_meta_types::VoteResponse;
use databend_common_metrics::count::Count;
use futures::FutureExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use openraft::RaftNetwork;
use tokio_stream::wrappers::ReceiverStream;
use tonic::client::GrpcService;
use tonic::transport::channel::Channel;

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
pub struct NetworkFactory {
    sto: RaftStore,

    conn_pool: Arc<Pool<ChannelManager>>,

    backoff: Backoff,
}

impl NetworkFactory {
    pub fn new(sto: RaftStore) -> NetworkFactory {
        let mgr = ChannelManager {};
        NetworkFactory {
            sto,
            conn_pool: Arc::new(Pool::new(mgr, Duration::from_millis(50))),
            backoff: Backoff::default(),
        }
    }
}

pub struct Network {
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

    sto: RaftStore,

    conn_pool: Arc<Pool<ChannelManager>>,

    backoff: Backoff,
}

impl Network {
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    pub async fn make_client(&mut self) -> Result<RaftClient, Unreachable> {
        let target = self.target;

        let endpoint = self
            .sto
            .get_node_raft_endpoint(&target)
            .await
            .map_err(|e| {
                let any_err = AnyError::new(&e)
                    .add_context(|| format!("{} target: {}", func_name!(), self.target));
                Unreachable::new(&any_err)
            })?;

        let addr = format!("http://{}", endpoint);

        debug!(id = self.id; "connect: target={}: {}", target, addr);

        match self.conn_pool.get(&addr).await {
            Ok(channel) => {
                let client = RaftClientApi::new(target, endpoint.clone(), channel);
                debug!("connected: target={}: {}", target, addr);

                self.endpoint = endpoint;

                Ok(client)
            }
            Err(err) => {
                raft_metrics::network::incr_connect_failure(&target, &endpoint.to_string());
                let any_err = AnyError::new(&err).add_context(|| {
                    format!("{} target: {}, addr: {}", func_name!(), self.target, addr)
                });

                Err(Unreachable::new(&any_err))
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

    /// Wrap an error with [`RemoteError`], when building return value for an RPC method.
    pub(crate) fn to_remote_err<E: Error>(&self, e: E) -> RemoteError<E> {
        RemoteError::new_with_node(self.target, self.target_node.clone(), e)
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

    /// Convert gRPC status to `Unreachable`
    fn status_to_unreachable(&self, status: tonic::Status) -> Unreachable {
        warn!(
            "target={}, endpoint={} gRPC error: {:?}",
            self.target, self.endpoint, status
        );

        let any_err = AnyError::new(&status)
            .add_context(|| format!("gRPC target={}, endpoint={}", self.target, self.endpoint));

        Unreachable::new(&any_err)
    }

    #[futures_async_stream::try_stream(boxed, ok = SnapshotChunkRequestV003, error = StreamingError<Fatal>)]
    async fn snapshot_chunk_stream_v003(
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) {
        let chunk_size = option.snapshot_chunk_size().unwrap_or(1024 * 1024);

        let snapshot_meta = snapshot.meta;
        let db = snapshot.snapshot;

        info!(
            "start to transmit snapshot: {}; db.file_size: {}; db.stat: {}; chunk size:{}",
            snapshot_meta,
            db.file_size(),
            db.stat(),
            chunk_size
        );

        let mut bf = db.open_file().map_err(|e| {
            let io_err = StorageIOError::read_snapshot(Some(snapshot_meta.signature()), &e);
            StorageError::from(io_err)
        })?;

        let mut c = std::pin::pin!(cancel);

        let mut buf = Vec::with_capacity(chunk_size);
        unsafe {
            buf.set_len(chunk_size);
        }

        loop {
            // If canceled, return at once
            if let Some(err) = c.as_mut().now_or_never() {
                return Err(err.into());
            }

            let mut offset = 0;
            while offset < buf.len() {
                let n_read = bf.read(&mut buf[offset..]).map_err(|e| {
                    let io_err = StorageIOError::read_snapshot(Some(snapshot_meta.signature()), &e);
                    StorageError::from(io_err)
                })?;

                debug!("offset: {}, n_read: {}", offset, n_read);
                if n_read == 0 {
                    break;
                }
                offset += n_read;
            }

            debug!("buf len: {}", buf.len());

            if offset == 0 {
                break;
            }

            debug!("build snapshot chunk len: {}", offset);

            let chunk = SnapshotChunkRequestV003::new_chunk((&buf[..offset]).to_vec());
            yield chunk;
        }

        debug!("build snapshot end chunk");

        let end = SnapshotChunkRequestV003::new_end_chunk(vote, snapshot_meta.clone());
        yield end;
    }
}

impl RaftNetwork<TypeConfig> for Network {
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

        let mut client = self.make_client().await?;

        let raft_req = self.new_append_entries_raft_req(&rpc)?;
        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let grpc_res = client
            .append_entries(req)
            .timed(observe_append_send_spent(self.target))
            .await;
        debug!(
            "append_entries resp from: target={}: {:?}",
            self.target, grpc_res
        );

        self.parse_grpc_resp(grpc_res)
    }

    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError<Fatal>> {
        info!(id = self.id, target = self.target; "{}", func_name!());

        let _g = snapshot_send_inflight(self.target).counter_guard();

        let mut client = self.make_client().await?;

        // Using strm of type `Pin<Box<Stream + Send + 'static>>` result in a higher rank lifetime error
        // See:
        // - https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=8c382b5a6d932aaf81815f3825efd5ed
        // - https://github.com/rust-lang/rust/issues/87425
        //
        // Here we convert it to a concrete type `ReceiverStream` to avoid the error.
        let mut res_strm = Self::snapshot_chunk_stream_v003(vote, snapshot, cancel, option);

        let (tx, rx) = mpsc::channel(16);

        let strm = ReceiverStream::new(rx);

        let target = self.target;

        let _forward_handle = runtime::spawn(async move {
            while let Some(x) = res_strm.try_next().await? {
                raft_metrics::network::incr_sendto_bytes(&target, x.chunk.len() as u64);

                let send_res = tx.send(x).await;

                if let Err(e) = send_res {
                    error!("{} error sending to snapshot stream: {}", func_name!(), e);
                }
            }

            Ok::<_, StreamingError<Fatal>>(())
        });

        let res: Result<SnapshotResponse, StreamingError<Fatal>> = try {
            let grpc_res = client
                .install_snapshot_v003(strm)
                .timed(observe_snapshot_send_spent(self.target))
                .await;

            info!(
                "{} resp from: target={}: grpc_result: {:?}",
                func_name!(),
                self.target,
                grpc_res,
            );

            let grpc_response = grpc_res.map_err(|e| self.status_to_unreachable(e))?;

            let remote_result: Result<SnapshotResponse, Fatal> =
                GrpcHelper::parse_raft_reply_generic(grpc_response.into_inner())
                    .map_err(|serde_err| new_net_err(&serde_err, || "parse full_snapshot reply"))?;

            remote_result.map_err(|e| self.to_remote_err(e))?
        };

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

        let mut client = self.make_client().await?;

        let raft_req = GrpcHelper::encode_raft_request(&rpc).map_err(|e| Unreachable::new(&e))?;

        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let grpc_res = client.vote(req).await;
        info!("vote: resp from target={} {:?}", self.target, grpc_res);

        self.parse_grpc_resp(grpc_res)
    }

    /// When a `Unreachable` error is returned from the `Network`,
    /// Openraft will call this method to build a backoff instance.
    fn backoff(&self) -> openraft::network::Backoff {
        warn!("backoff is required: target={}", self.target);
        openraft::network::Backoff::new(self.back_off())
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(
        self: &mut NetworkFactory,
        target: NodeId,
        node: &MembershipNode,
    ) -> Self::Network {
        info!(
            "new raft communication client: id:{}, target:{}, node:{}",
            self.sto.id, target, node
        );

        Network {
            id: self.sto.id,
            target,
            target_node: node.clone(),
            sto: self.sto.clone(),
            conn_pool: self.conn_pool.clone(),
            backoff: self.backoff.clone(),
            endpoint: Default::default(),
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
