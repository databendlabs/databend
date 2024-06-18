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
use std::time::Duration;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::time::Instant;
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
use databend_common_meta_types::MetaNetworkError;
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
use log::debug;
use log::error;
use log::info;
use log::warn;
use minitrace::func_name;
use openraft::RaftNetwork;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;

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
pub struct NetworkFactory {
    sto: RaftStore,

    backoff: Backoff,
}

impl NetworkFactory {
    pub fn new(sto: RaftStore) -> NetworkFactory {
        NetworkFactory {
            sto,
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

    client: Mutex<Option<RaftClient>>,

    sto: RaftStore,

    backoff: Backoff,
}

impl Network {
    /// Create a new RaftClient to the specified target node.
    #[logcall::logcall(err = "debug")]
    #[minitrace::trace]
    pub async fn new_client(&self, addr: &str) -> Result<RaftClient, tonic::transport::Error> {
        info!(id = self.id; "Raft NetworkConnection connect: target={}: {}", self.target, addr);

        let channel = tonic::transport::Endpoint::new(addr.to_string())?
            .connect()
            .debug_elapsed(format!(
                "Raft NetworkConnection new_client: connect target: {}",
                self.target
            ))
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
            let endpoint = self
                .lookup_target_address()
                .debug_elapsed(format!(
                    "Raft NetworkConnection take_client lookup_target_address: target: {}",
                    self.target
                ))
                .await
                .map_err(|e| {
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
        debug!(
            "Raft NetworkConnection lookup target address: start: target={}",
            self.target
        );
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
        let start = Instant::now();
        let raft_req = GrpcHelper::encode_raft_request(rpc).map_err(|e| Unreachable::new(&e))?;
        debug!(
            "Raft NetworkConnection: new_append_entries_raft_req() encode_raft_request: target={}, elapsed={:?}",
            self.target,
            start.elapsed()
        );

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

    /// Split V003 snapshot `DB` into chunks and send them via the given channel.
    fn snapshot_chunk_stream_v003(
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
        target: NodeId,
        tx: mpsc::Sender<SnapshotChunkRequestV003>,
    ) -> Result<(), StreamingError<Fatal>> {
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

        #[allow(clippy::uninit_vec)]
        let mut buf = {
            let mut b = Vec::with_capacity(chunk_size);
            unsafe {
                b.set_len(chunk_size);
            }
            b
        };

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

            debug!("Build snapshot chunk len: {}", offset);

            let chunk = SnapshotChunkRequestV003::new_chunk((buf[..offset]).to_vec());
            let len = chunk.chunk.len() as u64;

            let send_res = tx.blocking_send(chunk);
            if let Err(e) = send_res {
                error!("{} error sending to snapshot stream: {}", func_name!(), e);
                return Ok(());
            }
            raft_metrics::network::incr_sendto_bytes(&target, len);
        }

        info!("build snapshot end chunk");

        let end = SnapshotChunkRequestV003::new_end_chunk(vote, snapshot_meta.clone());
        let send_res = tx.blocking_send(end);
        if let Err(e) = send_res {
            error!(
                "{} error sending end chunk to snapshot stream: {}",
                func_name!(),
                e
            );
        }

        Ok(())
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

        let raft_req = self.new_append_entries_raft_req(&rpc)?;
        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let mut client = self
            .take_client()
            .debug_elapsed("Raft NetworkConnection append_entries take_client()")
            .await?;

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
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError<Fatal>> {
        info!(id = self.id, target = self.target; "{}", func_name!());

        let _g = snapshot_send_inflight(self.target).counter_guard();

        let target = self.target;
        let (tx, rx) = mpsc::channel(16);
        let strm = ReceiverStream::new(rx);

        // Using strm of type `Pin<Box<Stream + Send + 'static>>` result in a higher rank lifetime error
        // See:
        // - https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=8c382b5a6d932aaf81815f3825efd5ed
        // - https://github.com/rust-lang/rust/issues/87425
        //
        // Here we convert it to a concrete type `ReceiverStream` to avoid the error.

        let strm_handle = runtime::spawn_blocking(move || {
            Self::snapshot_chunk_stream_v003(vote, snapshot, cancel, option, target, tx)
        });

        let mut client = self
            .take_client()
            .debug_elapsed("Raft NetworkConnection install_snapshot take_client()")
            .await?;

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

            match &grpc_res {
                Ok(_) => {
                    self.client.lock().await.replace(client);
                }
                Err(e) => {
                    warn!(target = self.target; "install_snapshot failed: {}", e);
                }
            }

            let join_res = strm_handle.await;
            match join_res {
                Err(e) => {
                    warn!("Snapshot sending thread error: {}", e);
                }
                Ok(strm_res) => {
                    if let Err(e) = strm_res {
                        warn!("Snapshot sending thread error: {}", e);
                        return Err(e);
                    }
                }
            }

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

        let raft_req = GrpcHelper::encode_raft_request(&rpc).map_err(|e| Unreachable::new(&e))?;

        let req = GrpcHelper::traced_req(raft_req);

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

        let mut client = self
            .take_client()
            .debug_elapsed("Raft NetworkConnection vote take_client()")
            .await?;

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
