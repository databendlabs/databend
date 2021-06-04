// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::config::Config;
use async_raft::raft::AppendEntriesRequest;
use async_raft::raft::AppendEntriesResponse;
use async_raft::raft::ClientWriteRequest;
use async_raft::raft::Entry;
use async_raft::raft::EntryPayload;
use async_raft::raft::InstallSnapshotRequest;
use async_raft::raft::InstallSnapshotResponse;
use async_raft::raft::MembershipConfig;
use async_raft::raft::VoteRequest;
use async_raft::raft::VoteResponse;
use async_raft::storage::CurrentSnapshotData;
use async_raft::storage::HardState;
use async_raft::storage::InitialState;
use async_raft::AppData;
use async_raft::AppDataResponse;
use async_raft::NodeId;
use async_raft::Raft;
use async_raft::RaftMetrics;
use async_raft::RaftNetwork;
use async_raft::RaftStorage;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tokio::task::JoinHandle;
use tonic::transport::channel::Channel;

use crate::meta_service::Meta;
use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::meta_service::Node;
use crate::meta_service::RaftMes;

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

/// Cmd is an action a client wants to take.
/// A Cmd is committed by raft leader before being applied.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Cmd {
    /// AKA put-if-absent. add a key-value record only when key is absent.
    AddFile { key: String, value: String },

    /// Override the record with key.
    SetFile { key: String, value: String },

    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq { key: String },

    /// Add node if absent
    AddNode { node_id: NodeId, node: Node },
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::AddFile { key, value } => {
                write!(f, "addfile:{}={}", key, value)
            }
            Cmd::SetFile { key, value } => {
                write!(f, "setfile:{}={}", key, value)
            }
            Cmd::IncrSeq { key } => {
                write!(f, "incr_seq:{}", key)
            }
            Cmd::AddNode { node_id, node } => {
                write!(f, "addnode:{}={}", node_id, node)
            }
        }
    }
}

/// RaftTxId is the essential info to identify an write operation to raft.
/// Logs with the same RaftTxId are considered the same and only the first of them will be applied.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RaftTxId {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    /// TODO(xp): a client must generate consistent `client` and globally unique serial.
    /// TODO(xp): in this impl the state machine records only one serial, which implies serial must be monotonic incremental for every client.
    pub serial: u64,
}

impl RaftTxId {
    pub fn new(client: &str, serial: u64) -> Self {
        Self {
            client: client.to_string(),
            serial,
        }
    }
}

/// The application data request type which the `MemStore` works with.
///
/// The client and the serial together provides external consistency:
/// If a client failed to recv the response, it  re-send another ClientRequest with the same
/// "client" and "serial", thus the raft engine is able to distinguish if a request is duplicated.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// When not None, it is used to filter out duplicated logs, which are caused by retries by client.
    pub txid: Option<RaftTxId>,

    /// The action a client want to take.
    pub cmd: Cmd,
}

impl AppData for ClientRequest {}

impl tonic::IntoRequest<RaftMes> for ClientRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}
impl tonic::IntoRequest<RaftMes> for AppendEntriesRequest<ClientRequest> {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}
impl tonic::IntoRequest<RaftMes> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}
impl tonic::IntoRequest<RaftMes> for VoteRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_string(&self).expect("fail to serialize"),
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftMes> for ClientRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftMes) -> Result<Self, Self::Error> {
        let req: ClientRequest =
            serde_json::from_str(&mes.data).map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(req)
    }
}

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ClientResponse {
    String {
        // The value before applying a ClientRequest.
        prev: Option<String>,
        // The value after applying a ClientRequest.
        result: Option<String>,
    },
    Seq {
        seq: u64,
    },
    Node {
        prev: Option<Node>,
        result: Option<Node>,
    },
}

impl AppDataResponse for ClientResponse {}

impl From<ClientResponse> for RaftMes {
    fn from(msg: ClientResponse) -> Self {
        let data = serde_json::to_string(&msg).expect("fail to serialize");
        RaftMes { data }
    }
}

impl From<RaftMes> for ClientResponse {
    fn from(msg: RaftMes) -> Self {
        let resp: ClientResponse = serde_json::from_str(&msg.data).expect("fail to deserialize");
        resp
    }
}

impl From<tonic::Response<RaftMes>> for ClientResponse {
    fn from(v: tonic::Response<RaftMes>) -> Self {
        let mes = v.into_inner();
        let resp: ClientResponse = serde_json::from_str(&mes.data).expect("fail to deserialize");
        resp
    }
}

impl From<(Option<String>, Option<String>)> for ClientResponse {
    fn from(v: (Option<String>, Option<String>)) -> Self {
        ClientResponse::String {
            prev: v.0,
            result: v.1,
        }
    }
}

impl From<u64> for ClientResponse {
    fn from(seq: u64) -> Self {
        ClientResponse::Seq { seq }
    }
}

impl From<(Option<Node>, Option<Node>)> for ClientResponse {
    fn from(v: (Option<Node>, Option<Node>)) -> Self {
        ClientResponse::Node {
            prev: v.0,
            result: v.1,
        }
    }
}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}

/// The application snapshot type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemStoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The state machine of the `MemStore`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: u64,
    /// A mapping of client IDs to their state info:
    /// (serial, ClientResponse)
    pub client_serial_responses: HashMap<String, (u64, ClientResponse)>,

    pub meta: Meta,
}

impl MemStoreStateMachine {
    /// Apply an log entry to state machine.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> Result<ClientResponse> {
        self.last_applied_log = index;
        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_serial_responses.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let resp = self.meta.apply(data)?;

        if let Some(ref txid) = data.txid {
            self.client_serial_responses
                .insert(txid.client.clone(), (txid.serial, resp.clone()));
        }
        Ok(resp)
    }
}

/// An in-memory storage system implementing the `async_raft::RaftStorage` trait.
pub struct MemStore {
    /// The ID of the Raft node for which this memory storage instances is configured.
    id: NodeId,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    /// The Raft state machine.
    sm: RwLock<MemStoreStateMachine>,
    /// The current hard state.
    hs: RwLock<Option<HardState>>,
    /// The current snapshot.
    current_snapshot: RwLock<Option<MemStoreSnapshot>>,
}

impl MemStore {
    /// Create a new `MemStore` instance.
    pub fn new(id: NodeId) -> Self {
        let log = RwLock::new(BTreeMap::new());
        let sm = RwLock::new(MemStoreStateMachine::default());
        let hs = RwLock::new(None);
        let current_snapshot = RwLock::new(None);

        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
        }
    }

    /// Create a new `MemStore` instance with some existing state (for testing).
    #[cfg(test)]
    pub fn new_with_state(
        id: NodeId,
        log: BTreeMap<u64, Entry<ClientRequest>>,
        sm: MemStoreStateMachine,
        hs: Option<HardState>,
        current_snapshot: Option<MemStoreSnapshot>,
    ) -> Self {
        let log = RwLock::new(log);
        let sm = RwLock::new(sm);
        let hs = RwLock::new(hs);
        let current_snapshot = RwLock::new(current_snapshot);
        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
        }
    }

    /// Get a handle to the log for testing purposes.
    pub async fn get_log(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, Entry<ClientRequest>>> {
        self.log.write().await
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, MemStoreStateMachine> {
        self.sm.write().await
    }

    /// Get a handle to the current hard state for testing purposes.
    pub async fn read_hard_state(&self) -> RwLockReadGuard<'_, Option<HardState>> {
        self.hs.read().await
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for MemStore {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                let st = InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                };
                tracing::info!("build initial state from storage: {:?}", st);
                Ok(st)
            }
            None => {
                let new = InitialState::new_initial(self.id);
                tracing::info!("create initial state: {:?}", new);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self, hs), fields(myid=self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, entry), fields(myid=self.id))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, entries), fields(myid=self.id))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut sm = self.sm.write().await;
        let resp = sm.apply(*index, data)?;
        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self, entries), fields(myid=self.id))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.apply(**index, data)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = MemStoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::info!(
            { snapshot_size = snapshot_bytes.len() },
            "log compaction complete"
        );
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "info", skip(self, snapshot), fields(myid=self.id))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: MemStoreSnapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        // Update the state machine.
        {
            let new_sm: MemStoreStateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(myid=self.id))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}

pub struct Network {
    sto: Arc<MemStore>,
}

impl Network {
    pub fn new(sto: Arc<MemStore>) -> Network {
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
impl RaftNetwork<ClientRequest> for Network {
    #[tracing::instrument(level = "info", skip(self), fields(myid=self.sto.id))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
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
    ) -> Result<InstallSnapshotResponse> {
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
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
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

// MetaRaft is a impl of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<ClientRequest, ClientResponse, Network, MemStore>;

// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    // metrics subscribes raft state changes. The most important field is the leader node id, to which all write operations should be forward.
    pub metrics_rx: watch::Receiver<RaftMetrics>,
    pub sto: Arc<MemStore>,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<anyhow::Result<()>>>>,
}

impl MemStore {
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.sm.read().await;

        sm.meta.get_node(node_id)
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> anyhow::Result<String> {
        let addr = self
            .get_node(node_id)
            .await
            .map(|n| n.address)
            .ok_or_else(|| anyhow::anyhow!("node not found {}", node_id))?;
        Ok(addr)
    }

    /// A non-voter is a node stored in raft store, but is not configured as a voter in the raft group.
    pub async fn list_non_voters(&self) -> HashSet<NodeId> {
        let mut rst = HashSet::new();
        let sm = self.sm.read().await;
        let ms = self
            .get_membership_config()
            .await
            .expect("fail to get membership");

        for i in sm.meta.nodes.keys() {
            // it has been added into this cluster and is not a voter.
            if !ms.contains(i) {
                rst.insert(*i);
            }
        }
        rst
    }
}

pub struct MetaNodeBuilder {
    node_id: Option<NodeId>,
    config: Option<Config>,
    sto: Option<Arc<MemStore>>,
    monitor_metrics: bool,
    start_grpc_service: bool,
}

impl MetaNodeBuilder {
    pub async fn build(mut self) -> anyhow::Result<Arc<MetaNode>> {
        let node_id = self
            .node_id
            .ok_or_else(|| anyhow::anyhow!("node_id is not set"))?;

        let config = self
            .config
            .take()
            .ok_or_else(|| anyhow::anyhow!("config is not set"))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| anyhow::anyhow!("sto is not set"))?;

        let net = Network::new(sto.clone());

        let raft = MetaRaft::new(node_id, Arc::new(config), Arc::new(net), sto.clone());
        let metrics_rx = raft.metrics();

        let (tx, rx) = watch::channel::<()>(());

        let mn = Arc::new(MetaNode {
            metrics_rx: metrics_rx.clone(),
            sto: sto.clone(),
            raft,
            running_tx: tx,
            running_rx: rx,
            join_handles: Mutex::new(Vec::new()),
        });

        if self.monitor_metrics {
            tracing::info!("about to subscribe raft metrics");
            MetaNode::subscribe_metrics(mn.clone(), metrics_rx).await;
        }

        if self.start_grpc_service {
            let addr = sto.get_node_addr(&node_id).await?;
            tracing::info!("about to start grpc on {}", addr);
            MetaNode::start_grpc(mn.clone(), &addr).await?;
        }
        Ok(mn)
    }

    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }
    pub fn sto(mut self, sto: Arc<MemStore>) -> Self {
        self.sto = Some(sto);
        self
    }
    pub fn start_grpc_service(mut self, b: bool) -> Self {
        self.start_grpc_service = b;
        self
    }
    pub fn monitor_metrics(mut self, b: bool) -> Self {
        self.monitor_metrics = b;
        self
    }
}

impl MetaNode {
    pub fn builder() -> MetaNodeBuilder {
        // Set heartbeat interval to a reasonable value.
        // The election_timeout should tolerate several heartbeat loss.
        let heartbeat = 500; // ms
        MetaNodeBuilder {
            node_id: None,
            config: Some(
                Config::build("foo_cluster".into())
                    .heartbeat_interval(heartbeat)
                    .election_timeout_min(heartbeat * 4)
                    .election_timeout_max(heartbeat * 8)
                    .validate()
                    .expect("fail to build raft config"),
            ),
            sto: None,
            monitor_metrics: true,
            start_grpc_service: true,
        }
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[tracing::instrument(level = "info", skip(mn))]
    pub async fn start_grpc(mn: Arc<MetaNode>, addr: &str) -> anyhow::Result<()> {
        let mut rx = mn.running_rx.clone();

        let meta_srv_impl = MetaServiceImpl::create(mn.clone());
        let meta_srv = MetaServiceServer::new(meta_srv_impl);

        let addr_str = addr.to_string();
        let addr = addr.parse::<std::net::SocketAddr>()?;
        let node_id = mn.sto.id;

        let srv = tonic::transport::Server::builder().add_service(meta_srv);

        let h = tokio::spawn(async move {
            srv.serve_with_shutdown(addr, async move {
                let _ = rx.changed().await;
                tracing::info!(
                    "signal received, shutting down: id={} {} ",
                    node_id,
                    addr_str
                );
            })
            .await
            .map_err(|e| anyhow::anyhow!("MetaNode service error: {:?}", e))?;
            Ok::<(), anyhow::Error>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Start a MetaStore node from initialized store.
    #[tracing::instrument(level = "info")]
    pub async fn new(node_id: NodeId) -> Arc<MetaNode> {
        let b = MetaNode::builder()
            .node_id(node_id)
            .sto(Arc::new(MemStore::new(node_id)));

        b.build().await.expect("can not fail")
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> anyhow::Result<i32> {
        // TODO need to be reentrant.

        let mut rx = self.raft.metrics();

        self.raft.shutdown().await?;
        self.running_tx.send(()).unwrap();

        // wait for raft to close the metrics tx
        loop {
            let r = rx.changed().await;
            if r.is_err() {
                break;
            }
            tracing::info!("waiting for raft to shutdown, metrics: {:?}", rx.borrow());
        }
        tracing::info!("shutdown raft");

        // raft counts 1
        let mut joined = 1;
        for j in self.join_handles.lock().await.iter_mut() {
            let _rst = j.await?;
            joined += 1;
        }

        tracing::info!("shutdown: myid={}", self.sto.id);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        //TODO: return a handle for join
        // TODO: every state change triggers add_non_voter!!!
        let mut rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;

        // TODO: reduce dependency: it does not need all of the fields in MetaNode
        let mn = mn.clone();

        let h = tokio::task::spawn(async move {
            loop {
                let changed = tokio::select! {
                    _ = rx.changed() => {
                       return Ok::<(), anyhow::Error>(());
                    }
                    changed = metrics_rx.changed() => {
                        changed
                    }
                };
                if changed.is_ok() {
                    let mm = metrics_rx.borrow().clone();
                    if let Some(cur) = mm.current_leader {
                        if cur == mn.sto.id {
                            // TODO: check result
                            let _rst = mn.add_configured_non_voters().await;

                            if _rst.is_err() {
                                tracing::info!(
                                    "fail to add non-voter: my id={}, rst:{:?}",
                                    mn.sto.id,
                                    _rst
                                );
                            }
                        }
                    }
                } else {
                    // shutting down
                    break;
                }
            }

            Ok::<(), anyhow::Error>(())
        });
        jh.push(h);
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    /// When a node is initialized with boot or boot_non_voter, start it with MetaStore::new().
    #[tracing::instrument(level = "info")]
    pub async fn boot(node_id: NodeId, addr: String) -> anyhow::Result<Arc<MetaNode>> {
        let mn = MetaNode::boot_non_voter(node_id, &addr).await?;

        let mut cluster_node_ids = HashSet::new();
        cluster_node_ids.insert(node_id);

        let rst = mn
            .raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| anyhow::anyhow!("{:?}", x))?;

        tracing::info!("booted, rst: {:?}", rst);
        mn.add_node(node_id, addr).await?;

        Ok(mn)
    }

    /// Boot a node that is going to join an existent cluster.
    /// For every node this should be called exactly once.
    /// When successfully initialized(e.g. received logs from raft leader), a node should be started with MetaNode::new().
    #[tracing::instrument(level = "info")]
    pub async fn boot_non_voter(node_id: NodeId, addr: &str) -> anyhow::Result<Arc<MetaNode>> {
        // TODO test MetaNode::new() on a booted store.
        // TODO: Before calling this func, the node should be added as a non-voter to leader.
        // TODO: check raft initialState to see if the store is clean.

        // When booting, there is addr stored in local store.
        // Thus we need to start grpc manually.
        let sto = MemStore::new(node_id);

        let b = MetaNode::builder()
            .node_id(node_id)
            .start_grpc_service(false)
            .sto(Arc::new(sto));

        let mn = b.build().await?;

        // Manually start the grpc, since no addr is stored yet.
        // We can not use the startup routine for initialized node.
        MetaNode::start_grpc(mn.clone(), addr).await?;

        tracing::info!("booted non-voter: {}={}", node_id, addr);

        Ok(mn)
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// async-raft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_configured_non_voters(&self) -> anyhow::Result<()> {
        // TODO after leader established, add non-voter through apis
        let node_ids = self.sto.list_non_voters().await;
        for i in node_ids.iter() {
            let x = self.raft.add_non_voter(*i).await;

            tracing::info!("add_non_voter result: {:?}", x);
            if x.is_ok() {
                tracing::info!("non-voter is added: {}", i);
            } else {
                tracing::info!("non-voter already exist: {}", i);
            }
        }
        Ok(())
    }

    // get a file from local meta state, most business logic without strong consistency requirement should use this to access meta.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_file(&self, key: &str) -> Option<String> {
        // inconsistent get: from local state machine

        let sm = self.sto.sm.read().await;
        sm.meta.get_file(key)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        // inconsistent get: from local state machine

        let sm = self.sto.sm.read().await;
        sm.meta.get_node(node_id)
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_node(&self, node_id: NodeId, addr: String) -> anyhow::Result<ClientResponse> {
        // TODO: use txid?
        let _resp = self
            .write(ClientRequest {
                txid: None,
                cmd: Cmd::AddNode {
                    node_id,
                    node: Node {
                        name: "".to_string(),
                        address: addr,
                    },
                },
            })
            .await?;
        Ok(_resp)
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write(&self, req: ClientRequest) -> anyhow::Result<ClientResponse> {
        // TODO handle ForwardToLeader error if leader changed.
        let leader = self.get_leader().await;

        if leader == self.sto.id {
            return self.write_to_local_leader(req).await;
        }

        // forward to leader

        let addr = self.sto.get_node_addr(&leader).await?;
        let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
        let resp = client.write(req).await?;
        let resp = resp.into();
        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_leader(&self) -> NodeId {
        // try to get the leader from the latest metrics.
        // If leader is absent, wait for an update in which a leader is set.

        if let Some(l) = self.metrics_rx.borrow().current_leader {
            return l;
        }

        // slow path: wait loop

        let mut rx = self.metrics_rx.clone();

        loop {
            // If changed() is called before clone(),
            // we may miss the changed message.
            // Thus we need to re-check the cloned rx.
            if let Some(l) = rx.borrow().current_leader {
                return l;
            }

            let changed = rx.changed().await;
            if changed.is_err() {
                tracing::info!("raft metrics tx closed");
                return 0;
            }
        }
    }

    /// Write a meta record through local raft node.
    /// It works only when this node is the leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write_to_local_leader(
        &self,
        req: ClientRequest,
    ) -> anyhow::Result<ClientResponse> {
        // TODO: respond ForwardToLeader error
        let write_rst = self.raft.client_write(ClientWriteRequest::new(req)).await;

        tracing::info!("raft.client_write rst: {:?}", write_rst);

        let resp = match write_rst {
            Ok(v) => v,
            // ClientWriteError::RaftError(re)
            // ClientWriteError::ForwardToLeader(_req, _node_id)
            Err(e) => return Err(anyhow::anyhow!("{:}", e)),
        };

        Ok(resp.data)
    }
}
