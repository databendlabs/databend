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
use std::marker::PhantomData;
use std::time::Duration;

use anyerror::AnyError;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use databend_base::counter::Counter;
use databend_base::futures::ElapsedFutureExt;
use databend_common_meta_raft_store::leveled_store::persisted_codec::PersistedCodec;
use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_sled_store::openraft;
use databend_common_meta_sled_store::openraft::MessageSummary;
use databend_common_meta_sled_store::openraft::RaftNetworkFactory;
use databend_common_meta_sled_store::openraft::error::ReplicationClosed;
use databend_common_meta_sled_store::openraft::network::RPCOption;
use databend_common_meta_sled_store::openraft::network::v2::RaftNetworkV2;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::GrpcHelper;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::protobuf::InstallEntryV004;
use databend_common_meta_types::protobuf::RaftReply;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_common_meta_types::raft_types::AppendEntriesRequest;
use databend_common_meta_types::raft_types::AppendEntriesResponse;
use databend_common_meta_types::raft_types::MembershipNode;
use databend_common_meta_types::raft_types::NetworkError;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::RPCError;
use databend_common_meta_types::raft_types::RaftError;
use databend_common_meta_types::raft_types::Snapshot;
use databend_common_meta_types::raft_types::SnapshotResponse;
use databend_common_meta_types::raft_types::StorageError;
use databend_common_meta_types::raft_types::StreamingError;
use databend_common_meta_types::raft_types::TransferLeaderRequest;
use databend_common_meta_types::raft_types::TypeConfig;
use databend_common_meta_types::raft_types::Unreachable;
use databend_common_meta_types::raft_types::Vote;
use databend_common_meta_types::raft_types::VoteRequest;
use databend_common_meta_types::raft_types::VoteResponse;
use fastrace::func_name;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use seq_marked::SeqData;
use seq_marked::SeqV;
use state_machine_api::MetaValue;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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
pub struct NetworkFactory<SP> {
    sto: RaftStore<SP>,

    backoff: Backoff,

    _phantom: PhantomData<SP>,
}

impl<SP: SpawnApi> NetworkFactory<SP> {
    pub fn new(sto: RaftStore<SP>) -> Self {
        Self {
            sto,
            backoff: Backoff::default(),
            _phantom: PhantomData,
        }
    }
}

pub struct Network<SP> {
    /// This node id
    id: NodeId,

    /// The node id to send message to.
    target: NodeId,

    /// The endpoint of the target node.
    endpoint: Endpoint,

    client: Mutex<Option<RaftClient>>,

    sto: RaftStore<SP>,

    backoff: Backoff,

    _phantom: PhantomData<SP>,
}

impl<SP: SpawnApi> Network<SP> {
    /// Create a new RaftClient to the specified target node.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    pub async fn new_client(&self, addr: &str) -> Result<RaftClient, tonic::transport::Error> {
        info!(id = self.id; "Raft NetworkConnection connect: target={}: {}", self.target, addr);

        let channel = tonic::transport::Endpoint::new(addr.to_string())?
            .connect()
            .log_elapsed_debug(format!(
                "Raft NetworkConnection new_client: connect target: {}",
                self.target
            ))
            .await?;

        let client = RaftClientApi::new(
            self.target,
            self.endpoint.clone(),
            channel,
            &self.sto.config,
        );

        info!(
            "Raft NetworkConnection connected to: target={}: {}",
            self.target, addr
        );

        Ok(client)
    }

    /// Take the last used client or create a new one.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn take_client(&mut self) -> Result<RaftClient, Unreachable> {
        let mut client = self.client.lock().await;

        if let Some(c) = client.take() {
            return Ok(c);
        }

        let n = 3;
        for _i in 0..n {
            let endpoint = self
                .lookup_target_address()
                .log_elapsed_debug(format!(
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
                        "Raft NetworkConnection fail to connect: target={}: addr={}: {:?}",
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

        let endpoint = self
            .sto
            .get_node_raft_endpoint(&self.target)
            .await
            .ok_or_else(|| {
                MetaNetworkError::GetNodeAddrError(format!(
                    "Node {} not found in state machine",
                    self.target
                ))
            })?;

        Ok(endpoint)
    }

    pub(crate) fn report_metrics_snapshot(&self, success: bool) {
        raft_metrics::network::incr_sendto_result(&self.target, success);
        raft_metrics::network::incr_snapshot_sendto_result(&self.target, success);
    }

    /// Wrap a RaftError with RPCError
    pub(crate) fn to_rpc_err<E: Error + 'static>(&self, e: RaftError<E>) -> RPCError {
        RPCError::Unreachable(Unreachable::new(&e))
    }

    /// Build a partial AppendEntriesRequest with only the first `n` entries.
    fn build_partial_append_request(
        original: &AppendEntriesRequest,
        n: usize,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            vote: original.vote,
            prev_log_id: original.prev_log_id,
            leader_commit: original.leader_commit,
            entries: original.entries[..n].to_vec(),
        }
    }

    /// Reduce entry count by half. Returns `None` if already at minimum.
    fn try_reduce_entries(&self, current: usize, reason: &str) -> Option<usize> {
        if current <= 1 {
            return None;
        }

        let new_count = current / 2;
        warn!(
            "append_entries: target={}, {}, reducing entries {} -> {}",
            self.target, reason, current, new_count
        );
        Some(new_count)
    }

    pub(crate) fn back_off(&self) -> impl Iterator<Item = Duration> + use<SP> {
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
    ) -> Result<R, RPCError>
    where
        R: serde::de::DeserializeOwned + 'static,
        E: serde::de::DeserializeOwned + 'static,
        E: std::error::Error,
    {
        // Return status error
        let resp = grpc_res.map_err(|e| RPCError::Unreachable(self.status_to_unreachable(e)))?;

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
    ) -> Result<(), StreamingError> {
        let chunk_size = option.snapshot_chunk_size().unwrap_or(1024 * 1024);

        let snapshot_meta = snapshot.meta;
        let db = snapshot.snapshot;

        info!(
            "start to transmit snapshot via v003: {}; db.file_size: {}; db.stat: {}; chunk size:{}",
            snapshot_meta,
            db.file_size(),
            db.stat(),
            chunk_size
        );

        let mut bf = db.open_file().map_err(|e| {
            StorageError::read_snapshot(Some(snapshot_meta.signature()), (&e).into())
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
                    StorageError::read_snapshot(Some(snapshot_meta.signature()), (&e).into())
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

    /// Stream all KV entries from snapshot DB for V004 replication.
    /// Converts SeqMarked entries to protobuf format and sends via channel.
    /// Skips tombstones.
    async fn send_snapshot_in_stream_v004(
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
        target: NodeId,
        tx: mpsc::Sender<InstallEntryV004>,
    ) -> Result<(), StreamingError> {
        let snapshot_meta = snapshot.meta;
        let db = snapshot.snapshot;

        info!(
            "start to transmit snapshot via v004: {}; db.file_size: {}; db.stat: {}",
            snapshot_meta,
            db.file_size(),
            db.stat()
        );

        let mut c = std::pin::pin!(cancel);

        // Stream KV data from the snapshot DB
        let strm = db.inner_range();

        // Discard tombstones and convert SeqMarked to SeqData and then to protobuf SeqV
        let strm = strm.try_filter_map(|(k, v)| async move {
            let seq_data: Option<SeqData<_>> = v.into();
            let Some(seq_data) = seq_data else {
                // Tombstone, skip
                return Ok(None);
            };

            let seq_data = SeqData::<MetaValue>::decode_from(seq_data)?;
            let seq_v = SeqV::from(seq_data);
            let pb_seq_v = pb::SeqV::from(seq_v);
            let item = pb::StreamItem::new(k, Some(pb_seq_v));
            Ok(Some(item))
        });

        // Chunk the stream into batches of 64 items for efficiency
        let mut strm = strm.try_chunks(64).boxed();

        let mut kv_count = 0u64;

        while let Some(chunk) = strm.try_next().await.map_err(|err| {
            StorageError::read_snapshot(Some(snapshot_meta.signature()), (&err.1).into())
        })? {
            // Check for cancellation
            if let Some(err) = c.as_mut().now_or_never() {
                return Err(err.into());
            }

            // Total length of keys and values in this chunk
            let total_kv_len = chunk
                .iter()
                .map(|item| item.key.len() + item.value.as_ref().map(|v| v.data.len()).unwrap_or(0))
                .sum::<usize>();

            kv_count += chunk.len() as u64;

            // Send KV entry
            let kv_entry = InstallEntryV004 {
                version: 4,
                key_values: chunk,
                commit: None,
            };

            let send_res = tx.send(kv_entry).await;
            if let Err(e) = send_res {
                warn!("error sending to snapshot stream: {}, maybe closed", e);
                return Ok(());
            }

            if kv_count % 10000 == 0 {
                info!("V004 snapshot streaming: sent {} KV entries", kv_count);
            }

            raft_metrics::network::incr_sendto_bytes(&target, total_kv_len as u64);
        }

        info!("V004 snapshot streaming: completed {} KV entries", kv_count);

        // Send commit entry
        let sys_data_json = serde_json::to_string(db.sys_data()).map_err(|e| {
            StorageError::read_snapshot(Some(snapshot_meta.signature()), (&e).into())
        })?;

        // Convert Vote to protobuf Vote using existing conversion
        let pb_vote = pb::Vote::from(vote);

        let commit = pb::Commit {
            snapshot_id: snapshot_meta.snapshot_id.to_string(),
            sys_data: sys_data_json,
            vote: Some(pb_vote),
        };

        let final_entry = InstallEntryV004 {
            version: 4,
            key_values: vec![],
            commit: Some(commit),
        };

        let send_res = tx.send(final_entry).await;
        if let Err(e) = send_res {
            error!("error sending commit entry to snapshot stream: {}", e);
        }

        Ok(())
    }

    /// Send snapshot using V004 streaming protocol.
    ///
    /// Creates streaming connection and sends KV entries followed by commit message.
    /// More memory efficient than V003 as it doesn't buffer entire snapshot.
    async fn send_snapshot_via_v004(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        let ctx = format!(
            "send_snapshot_via_v004 id={} target={}, snapshot={}",
            self.id, self.target, snapshot.meta
        );

        info!("{}", ctx);

        let target = self.target;
        let (tx, rx) = mpsc::channel(64);
        let strm = ReceiverStream::new(rx);

        let strm_handle = SP::spawn(
            Self::send_snapshot_in_stream_v004(vote, snapshot, cancel, option, target, tx),
            Some("send_snapshot_via_v004".into()),
        );

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection install_snapshot_v004 take_client()")
            .await?;

        let grpc_res = client
            .install_snapshot_v004(strm)
            .inspect_elapsed(observe_snapshot_send_spent(target))
            .await;

        info!("{}: grpc_result: {:?}", ctx, grpc_res,);

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!("{} failed: {}", ctx, e);
            }
        }

        // if ensure_not_unimplemented(&grpc_res).is_err() {
        // }

        let res: Result<SnapshotResponse, StreamingError> = try {
            let join_res = strm_handle.await;
            match join_res {
                Err(e) => {
                    warn!("{} Snapshot sending thread error: {}", ctx, e);
                }
                Ok(strm_res) => {
                    if let Err(e) = strm_res {
                        warn!("{} Snapshot sending thread error: {}", ctx, e);
                        Err(e)?;
                    }
                }
            }
            let grpc_response =
                grpc_res.map_err(|e| StreamingError::Unreachable(self.status_to_unreachable(e)))?;
            let snapshot_response = grpc_response.into_inner();

            // Convert protobuf Vote back to internal Vote
            let proto_vote = snapshot_response.vote.ok_or_else(|| {
                StreamingError::Network(NetworkError::new(&AnyError::error(
                    "Missing vote in response",
                )))
            })?;
            let vote = Vote::from(proto_vote);
            SnapshotResponse { vote }
        };

        self.report_metrics_snapshot(res.is_ok());
        res
    }

    /// Send snapshot in stream of binary bytes chunks
    async fn send_snapshot_via_v003(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        info!(id = self.id, target = self.target; "{}", func_name!());

        let target = self.target;
        let (tx, rx) = mpsc::channel(16);
        let strm = ReceiverStream::new(rx);

        // Using strm of type `Pin<Box<Stream + Send + 'static>>` result in a higher rank lifetime error
        // See:
        // - https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=8c382b5a6d932aaf81815f3825efd5ed
        // - https://github.com/rust-lang/rust/issues/87425
        //
        // Here we convert it to a concrete type `ReceiverStream` to avoid the error.

        let strm_handle = SP::spawn_blocking(move || {
            Self::snapshot_chunk_stream_v003(vote, snapshot, cancel, option, target, tx)
        });

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection install_snapshot take_client()")
            .await?;

        let grpc_res = client
            .install_snapshot_v003(strm)
            .inspect_elapsed(observe_snapshot_send_spent(target))
            .await;

        info!(
            "{} resp from: target={}: grpc_result: {:?}",
            func_name!(),
            target,
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

        let res: Result<SnapshotResponse, StreamingError> = try {
            let join_res = strm_handle.await;
            match join_res {
                Err(e) => {
                    warn!("Snapshot sending thread error: {}", e);
                }
                Ok(strm_res) => {
                    if let Err(e) = strm_res {
                        warn!("Snapshot sending thread error: {}", e);
                        Err(e)?;
                    }
                }
            }

            let grpc_response =
                grpc_res.map_err(|e| StreamingError::Unreachable(self.status_to_unreachable(e)))?;
            let snapshot_response = grpc_response.into_inner();
            let vote = snapshot_response
                .to_vote()
                .map_err(|e| StreamingError::Network(e))?;

            SnapshotResponse { vote }
        };

        self.report_metrics_snapshot(res.is_ok());
        res
    }
}

impl<SP: SpawnApi> RaftNetworkV2<TypeConfig> for Network<SP> {
    /// Send AppendEntries RPC with automatic payload size management.
    ///
    /// If the payload exceeds gRPC size limit, reduces entry count and retries.
    /// Returns error if a single entry exceeds the limit.
    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        debug!(
            id = self.id,
            target = self.target,
            rpc = rpc.summary();
            "send_append_entries",
        );

        let total = rpc.entries.len();
        let mut entries_to_send = rpc.entries.len();

        loop {
            let partial_rpc = Self::build_partial_append_request(&rpc, entries_to_send);
            let raft_req =
                GrpcHelper::encode_raft_request(&partial_rpc).map_err(|e| Unreachable::new(&e))?;
            let payload_size = raft_req.data.len();

            // Check size before sending
            if payload_size > self.sto.config.raft_grpc_advisory_message_size() {
                let reason = format!("payload too large: {} bytes", payload_size);
                match self.try_reduce_entries(entries_to_send, &reason) {
                    Some(n) => {
                        entries_to_send = n;
                        continue;
                    }
                    None => {
                        let err = AnyError::error(reason);
                        return Err(RPCError::Unreachable(Unreachable::new(&err)));
                    }
                }
            }

            // Send the request
            let req = SP::prepare_request(tonic::Request::new(raft_req));
            raft_metrics::network::incr_sendto_bytes(&self.target, req.get_ref().data.len() as u64);

            let mut client = self
                .take_client()
                .log_elapsed_debug("Raft NetworkConnection append_entries take_client()")
                .await?;

            let grpc_res = client
                .append_entries(req)
                .inspect_elapsed(observe_append_send_spent(self.target))
                .await;

            debug!(
                "append_entries resp from: target={}: {:?}",
                self.target, grpc_res
            );

            match &grpc_res {
                Ok(_) => {
                    self.client.lock().await.replace(client);

                    // If we sent partial entries, return PartialSuccess
                    if entries_to_send < total {
                        let last_log_id = partial_rpc.entries.last().map(|e| e.log_id);
                        return Ok(AppendEntriesResponse::PartialSuccess(last_log_id));
                    }

                    return self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res);
                }
                Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                    match self.try_reduce_entries(entries_to_send, "ResourceExhausted") {
                        Some(n) => {
                            entries_to_send = n;
                            continue;
                        }
                        None => {
                            let err = AnyError::error("ResourceExhausted: single entry too large");
                            return Err(RPCError::Unreachable(Unreachable::new(&err)));
                        }
                    }
                }
                Err(e) => {
                    warn!(target = self.target, rpc = partial_rpc.summary(); "append_entries failed: {}", e);
                    return self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res);
                }
            }
        }
    }

    /// Send snapshot to target node. Currently uses V004 streaming protocol.
    /// TODO: Add version negotiation to choose between V003/V004 based on target capabilities.
    #[logcall::logcall(err = "error", input = "")]
    #[fastrace::trace]
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        debug!(id = self.id, target = self.target; "{}", func_name!());

        let _g = snapshot_send_inflight(self.target).counted_guard();

        // Clone the cancel
        let (tx1, cancel1) = oneshot::channel();
        let (tx2, cancel2) = oneshot::channel();

        #[allow(unused_must_use)]
        SP::spawn(
            async move {
                let got = cancel.await;
                tx1.send(got.clone()).ok();
                tx2.send(got).ok();
            },
            Some("snapshot_cancel_watch".into()),
        );

        // TODO: Add proper version negotiation or configuration
        // For now, use V004 for testing the new KV streaming implementation
        let res = self
            .send_snapshot_via_v004(
                vote,
                snapshot.clone(),
                async move {
                    let _ = cancel1.await;
                    ReplicationClosed::new("snapshot cancelled")
                },
                option.clone(),
            )
            .await;

        let err = match res {
            Ok(resp) => {
                return Ok(resp);
            }
            Err(e) => e,
        };

        warn!(
            "id={} target={} send_snapshot_via_v004 failed: {}",
            self.id, self.target, err
        );

        if let StreamingError::Unreachable(_unreachable) = &err {
            let resp = self
                .send_snapshot_via_v003(
                    vote,
                    snapshot,
                    async move {
                        let _ = cancel2.await;
                        ReplicationClosed::new("snapshot cancelled")
                    },
                    option.clone(),
                )
                .await?;
            Ok(resp)
        } else {
            Err(err)
        }
    }

    #[logcall::logcall(err = "debug")]
    #[fastrace::trace]
    async fn vote(
        &mut self,
        rpc: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        info!(id = self.id, target = self.target, rpc = rpc.summary(); "send_vote");

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection vote take_client()")
            .await?;

        // First, try VoteV001 with native protobuf types
        let vote_req_pb = pb::VoteRequest::from(rpc.clone());
        let req_v001 = SP::prepare_request(tonic::Request::new(vote_req_pb));

        let grpc_res_v001 = client.vote_v001(req_v001).await;
        info!(
            "vote_v001: resp from target={} {:?}",
            self.target, grpc_res_v001
        );

        match grpc_res_v001 {
            Ok(response) => {
                // VoteV001 succeeded, parse the VoteResponse directly
                self.client.lock().await.replace(client);
                let vote_response = response.into_inner();
                let vote_resp: VoteResponse = vote_response.into();
                return Ok(vote_resp);
            }
            Err(e) => {
                // Only fall back for specific status codes indicating method not implemented
                if e.code() == tonic::Code::Unimplemented || e.code() == tonic::Code::NotFound {
                    warn!(target = self.target, rpc = rpc.summary(); "vote_v001 not implemented, falling back to vote: {}", e);
                } else {
                    // For other errors, don't fall back - return the error
                    return Err(RPCError::Unreachable(self.status_to_unreachable(e.clone())));
                }
            }
        }

        // Fallback to old Vote RPC using RaftRequest
        let raft_req = GrpcHelper::encode_raft_request(&rpc).map_err(|e| Unreachable::new(&e))?;
        let req = SP::prepare_request(tonic::Request::new(raft_req));

        let bytes = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sendto_bytes(&self.target, bytes);

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

        self.parse_grpc_resp::<_, openraft::error::Infallible>(grpc_res)
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest,
        _option: RPCOption,
    ) -> Result<(), RPCError> {
        info!(id = self.id, target = self.target, req :? = req; "{}", func_name!());

        let r = pb::TransferLeaderRequest::from(req);

        let req = SP::prepare_request(tonic::Request::new(r));

        let mut client = self
            .take_client()
            .log_elapsed_debug("Raft NetworkConnection transfer_leader take_client()")
            .await?;

        let grpc_res = client.transfer_leader(req).await;
        info!(
            "{}: resp from target={} {:?}",
            func_name!(),
            self.target,
            grpc_res
        );

        match &grpc_res {
            Ok(_) => {
                self.client.lock().await.replace(client);
            }
            Err(e) => {
                warn!(target = self.target; "{} failed: {}", func_name!(), e);
            }
        }

        grpc_res.map_err(|e| RPCError::Unreachable(self.status_to_unreachable(e)))?;
        Ok(())
    }

    /// When a `Unreachable` error is returned from the `Network`,
    /// Openraft will call this method to build a backoff instance.
    fn backoff(&self) -> openraft::network::Backoff {
        warn!("backoff is required: target={}", self.target);
        openraft::network::Backoff::new(self.back_off())
    }
}

impl<SP: SpawnApi> RaftNetworkFactory<TypeConfig> for NetworkFactory<SP> {
    type Network = Network<SP>;

    async fn new_client(
        self: &mut NetworkFactory<SP>,
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
            sto: self.sto.clone(),
            backoff: self.backoff.clone(),
            endpoint: Default::default(),
            client: Default::default(),
            _phantom: PhantomData,
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
fn observe_append_send_spent<T>(target: NodeId) -> impl Fn(&T, Duration, Duration) {
    move |_output, t, _b| {
        raft_metrics::network::observe_append_sendto_spent(&target, t.as_secs() as f64);
    }
}

/// Create a function record the time cost of snapshot sending.
fn observe_snapshot_send_spent<T>(target: NodeId) -> impl Fn(&T, Duration, Duration) {
    move |_output, t, _b| {
        raft_metrics::network::observe_snapshot_sendto_spent(&target, t.as_secs() as f64);
    }
}

/// Create a function that increases metric value of inflight snapshot sending.
fn snapshot_send_inflight(target: NodeId) -> impl FnMut(i64) {
    move |i: i64| raft_metrics::network::incr_snapshot_sendto_inflight(&target, i)
}

#[allow(dead_code)]
fn ensure_not_unimplemented<T>(res: &Result<T, tonic::Status>) -> Result<(), tonic::Status> {
    match res {
        Err(e) if e.code() == tonic::Code::Unimplemented || e.code() == tonic::Code::NotFound => {
            Err(e.clone())
        }
        _ => Ok(()),
    }
}
