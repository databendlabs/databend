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
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tonic::transport::channel::Channel;

use crate::meta_service::MetaServiceClient;
use crate::meta_service::RaftMes;

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

/// Cmd is an action a client wants to take.
/// A Cmd is committed by raft leader before being applied.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Cmd {
    // AKA put-if-absent. add a key-value record only when key is absent.
    Add { key: String, value: String },
    // Override the record with key.
    Set { key: String, value: String }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::Add { key, value } => {
                write!(f, "add:{}={}", key, value)
            }
            Cmd::Set { key, value } => {
                write!(f, "set:{}={}", key, value)
            }
        }
    }
}

/// RaftTxId is the enssential info to identify an write operation to raft.
/// Logs with the same RaftTxId are considered the same and only the first of them will be applied.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RaftTxId {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    /// TODO(xp): a client msut generate consistent `client` and globally unique serial.
    /// TODO(xp): in this impl the state machine records only one serial, which implies serial must be monotonic incremental for every client.
    pub serial: u64
}

impl RaftTxId {
    pub fn new(client: &str, serial: u64) -> Self {
        Self {
            client: client.to_string(),
            serial
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
    pub cmd: Cmd
}

impl AppData for ClientRequest {}

impl tonic::IntoRequest<RaftMes> for ClientRequest {
    fn into_request(self) -> tonic::Request<RaftMes> {
        let mes = RaftMes {
            data: serde_json::to_vec(&self).expect("fail to serialize")
        };
        tonic::Request::new(mes)
    }
}

impl TryFrom<RaftMes> for ClientRequest {
    type Error = tonic::Status;

    fn try_from(mes: RaftMes) -> Result<Self, Self::Error> {
        let req: ClientRequest = serde_json::from_slice(&mes.data)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(req)
    }
}

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClientResponse {
    // The value before applying a ClientRequest.
    pub prev: Option<String>,
    // The value after applying a ClientRequest.
    pub result: Option<String>
}

impl AppDataResponse for ClientResponse {}

impl From<ClientResponse> for RaftMes {
    fn from(msg: ClientResponse) -> Self {
        let data = serde_json::to_vec(&msg).expect("fail to serialize");
        RaftMes { data }
    }
}
impl From<RaftMes> for ClientResponse {
    fn from(msg: RaftMes) -> Self {
        let resp: ClientResponse = serde_json::from_slice(&msg.data).expect("fail to deserialize");
        resp
    }
}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError
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
    pub data: Vec<u8>
}

/// The state machine of the `MemStore`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: u64,
    /// A mapping of client IDs to their state info:
    /// (serial, ClientResponse)
    pub client_serial_responses: HashMap<String, (u64, ClientResponse)>,
    pub kvs: HashMap<String, String>
}

impl MemStoreStateMachine {
    /// Apply an log entry to state machine.
    ///
    /// For an `Add` command, if the specified key presents, the returned ClientResponse set result
    /// to None and prev to the previous value.
    ///
    /// For an `Set` command, it always succeeds. and returns the previous value and the value
    /// client specified.
    ///
    /// If a duplicated log entry is detected by checking data.txid, no update
    /// will be made and the previous resp is returned. In this way a client is able to re-send a
    /// command safely in case of network failure etc.
    pub fn apply(&mut self, index: u64, data: &ClientRequest) -> Result<ClientResponse> {
        self.last_applied_log = index;

        if let Some(ref txid) = data.txid {
            if let Some((serial, resp)) = self.client_serial_responses.get(&txid.client) {
                if serial == &txid.serial {
                    return Ok(resp.clone());
                }
            }
        }

        let (prev, rst): (Option<String>, Option<String>) = match data.cmd {
            Cmd::Add { ref key, ref value } => {
                if self.kvs.contains_key(key) {
                    let prev = self.kvs.get(key);
                    (prev.cloned(), None)
                } else {
                    let prev = self.kvs.insert(key.clone(), value.clone());
                    (prev, Some(value.clone()))
                }
            }

            Cmd::Set { ref key, ref value } => {
                let prev = self.kvs.insert(key.clone(), value.clone());
                (prev, Some(value.clone()))
            }
        };

        let resp = ClientResponse { prev, result: rst };

        if let Some(ref txid) = data.txid {
            self.client_serial_responses
                .insert(txid.client.clone(), (txid.serial, resp.clone()));
        }
        Ok(resp)
    }
}

/// An in-memory storage system implementing the `async_raft::RaftStorage` trait.
///
/// TODO: support at least 3 data set: nodes in cluster, block distribution and dfs-file metas.
///       See: meta::Meta
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
    current_snapshot: RwLock<Option<MemStoreSnapshot>>
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
            current_snapshot
        }
    }

    /// Create a new `MemStore` instance with some existing state (for testing).
    #[cfg(test)]
    pub fn new_with_state(
        id: NodeId,
        log: BTreeMap<u64, Entry<ClientRequest>>,
        sm: MemStoreStateMachine,
        hs: Option<HardState>,
        current_snapshot: Option<MemStoreSnapshot>
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
            current_snapshot
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

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id)
        })
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0)
                };
                let last_applied_log = sm.last_applied_log;
                let st = InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership
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

    #[tracing::instrument(level = "info", skip(self, hs))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = "info", skip(self))]
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

    #[tracing::instrument(level = "info", skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, entries))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest
    ) -> Result<ClientResponse> {
        let mut sm = self.sm.write().await;
        sm.apply(*index, data)
    }

    #[tracing::instrument(level = "info", skip(self, entries))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.apply(**index, data)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
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
                    _ => None
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
                    membership_config.clone()
                )
            );

            let snapshot = MemStoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data
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
            snapshot: Box::new(Cursor::new(snapshot_bytes))
        })
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "info", skip(self, snapshot))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>
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
                    _ => None
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear()
            }
            log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config)
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

    #[tracing::instrument(level = "info", skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader))
                }))
            }
            None => Ok(None)
        }
    }
}

pub struct Network {
    sto: Arc<MemStore>
}

impl Network {
    pub fn new(m: Arc<MemStore>) -> Network {
        Network { sto: m }
    }
    pub async fn make_client(&self, addr: String) -> anyhow::Result<MetaServiceClient<Channel>> {
        let client = MetaServiceClient::connect(format!("http://{}", addr)).await?;
        Ok(client)
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for Network {
    #[tracing::instrument(level = "info", skip(self))]
    async fn append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<ClientRequest>
    ) -> Result<AppendEntriesResponse> {
        let addr = self.sto.get_node_addr(target).await?;
        let data = serde_json::to_vec(&rpc)?;

        let req = tonic::Request::new(RaftMes { data });

        let mut client = self.make_client(addr).await?;
        let resp = client.append_entries(req).await?;
        let mes = resp.into_inner();
        let resp = serde_json::from_slice(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest
    ) -> Result<InstallSnapshotResponse> {
        let addr = self.sto.get_node_addr(target).await?;
        let data = serde_json::to_vec(&rpc)?;

        let req = tonic::Request::new(RaftMes { data });

        let mut client = self.make_client(addr).await?;
        let resp = client.install_snapshot(req).await?;
        let mes = resp.into_inner();
        let resp = serde_json::from_slice(&mes.data)?;

        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let addr = self.sto.get_node_addr(target).await?;
        let data = serde_json::to_vec(&rpc)?;

        let req = tonic::Request::new(RaftMes { data });

        let mut client = self.make_client(addr).await?;
        let resp = client.vote(req).await?;
        let mes = resp.into_inner();
        let resp = serde_json::from_slice(&mes.data)?;

        Ok(resp)
    }
}

// MetaRaft is a impl of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<ClientRequest, ClientResponse, Network, MemStore>;

// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    // metrics subscribes raft state changes. The most important field is the leader node id, to which all write operations should be forward.
    pub metrics: Arc<RwLock<RaftMetrics>>,
    pub sto: Arc<MemStore>,
    pub raft: MetaRaft
}

impl MemStore {
    // build a meta data key of node in the cluster.
    pub fn node_key(&self, node_id: NodeId) -> String {
        format!("node/{:}", node_id)
    }

    pub async fn get_node_addr(&self, node_id: NodeId) -> anyhow::Result<String> {
        let key = self.node_key(node_id);
        let ns = self.sm.read().await;

        let addr = ns
            .kvs
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("node not found: {:}", key))?;
        Ok(addr.clone())
    }

    pub async fn list_added_non_voters(&self) -> HashSet<NodeId> {
        // TODO: impl a hierarchical structure in storage.

        let mut rst = HashSet::new();
        let sm = self.sm.read().await;
        let ms = self
            .get_membership_config()
            .await
            .expect("fail to get membership");
        // TODO: there is no iteration in store. Using a for loop to iterate nodes for now.
        for i in 0..100 {
            let key = self.node_key(i);
            // it has been added into this cluster and is not a voter.
            if sm.kvs.contains_key(&key) && !ms.contains(&i) {
                rst.insert(i);
            }
        }
        rst
    }
}

impl MetaNode {
    pub async fn new(node_id: NodeId) -> Arc<MetaNode> {
        let config = Config::build("foo_cluster".into())
            .validate()
            .expect("fail to build raft config");

        let sto = Arc::new(MemStore::new(node_id));
        let net = Network::new(sto.clone());

        let raft = MetaRaft::new(node_id, Arc::new(config), Arc::new(net), sto.clone());
        let metrics_rx = raft.metrics();
        let metrics = metrics_rx.borrow().clone();
        let metrics = Arc::new(RwLock::new(metrics));

        let mn = Arc::new(MetaNode { metrics, sto, raft });

        Self::subscribe_metrics(mn.clone(), metrics_rx);

        mn
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and feed the changes to a local cache.
    pub fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        tokio::task::spawn(async move {
            loop {
                let changed = metrics_rx.changed().await;
                if changed.is_ok() {
                    let mm = metrics_rx.borrow().clone();
                    *mn.metrics.write().await = mm;
                    // TODO: check result
                    let _rst = mn
                        .add_configured_non_voters()
                        .await
                        .expect("fail to update non-voter");
                } else {
                    // shutting down
                    break;
                }
            }
        });
    }

    /// boot is called only once when booting up a cluster.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn boot(&self, addr: String) -> anyhow::Result<()> {
        let node_id = self.sto.id;
        let mut cluster_node_ids = HashSet::new();
        cluster_node_ids.insert(node_id);

        let rst = self
            .raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| anyhow::anyhow!("{:?}", x))?;

        tracing::info!("booted, rst: {:?}", rst);

        let key = self.sto.node_key(self.sto.id);
        // TODO: use txid?
        let _resp = self
            .local_set(ClientRequest {
                txid: None,
                cmd: Cmd::Add { key, value: addr }
            })
            .await
            .expect("fail to add myself");
        Ok(())
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// async-raft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_configured_non_voters(&self) -> anyhow::Result<()> {
        // TODO after leader established, add non-voter through apis
        let node_ids = self.sto.list_added_non_voters().await;
        for i in node_ids.iter() {
            self.raft
                .add_non_voter(*i)
                .await
                .expect("fail to add non-voter");
        }
        Ok(())
    }

    // get meta data from local state, most business logic without strong consistency requirement should use this to access meta.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn local_get(&self, key: String) -> anyhow::Result<String> {
        // inconsistent get: from local state machine

        let sm = self.sto.sm.read().await;
        let v = sm
            .kvs
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("meta data key not found: {:}", key))?;
        Ok(v.clone())
    }

    /// Write a meta record through local raft node.
    /// It works only when this node is the leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn local_set(&self, req: ClientRequest) -> anyhow::Result<ClientResponse> {
        // TODO: respond ForwardToLeader error
        let write_rst = self.raft.client_write(ClientWriteRequest::new(req)).await;

        tracing::info!("raft.client_write rst: {:?}", write_rst);

        let resp = match write_rst {
            Ok(v) => v,
            // ClientWriteError::RaftError(re)
            // ClientWriteError::ForwardToLeader(_req, _node_id)
            Err(e) => return Err(anyhow::anyhow!("{:}", e))
        };

        Ok(resp.data)
    }
}
