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

use std::collections::BTreeSet;
use std::collections::HashSet;
use std::io::Cursor;
use std::ops::Bound;
use std::sync::Arc;

use async_raft::async_trait::async_trait;
use async_raft::config::Config;
use async_raft::raft::ClientWriteRequest;
use async_raft::raft::Entry;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::storage::CurrentSnapshotData;
use async_raft::storage::HardState;
use async_raft::storage::InitialState;
use async_raft::ClientWriteError;
use async_raft::Raft;
use async_raft::RaftMetrics;
use async_raft::RaftStorage;
use async_raft::SnapshotMeta;
use async_raft::SnapshotPolicy;
use common_base::tokio;
use common_base::tokio::sync::watch;
use common_base::tokio::sync::Mutex;
use common_base::tokio::sync::RwLock;
use common_base::tokio::sync::RwLockReadGuard;
use common_base::tokio::sync::RwLockWriteGuard;
use common_base::tokio::task::JoinHandle;
use common_exception::prelude::ErrorCode;
use common_exception::prelude::ToErrorCode;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_raft_store::state_machine::SerializableSnapshot;
use common_meta_raft_store::state_machine::Snapshot;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_raft_store::state_machine::TableLookupKey;
use common_meta_raft_store::state_machine::TableLookupValue;
use common_meta_sled_store::get_sled_db;
use common_meta_types::Cmd;
use common_meta_types::LogEntry;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::SeqV;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;

use crate::meta_service::MetaServiceClient;
use crate::meta_service::MetaServiceImpl;
use crate::meta_service::MetaServiceServer;
use crate::meta_service::Network;
use crate::meta_service::RetryableError;
use crate::meta_service::ShutdownError;

/// An storage system implementing the `async_raft::RaftStorage` trait.
///
/// Trees:
///   state:
///       id
///       hard_state
///   log
///   state_machine
/// TODO(xp): MetaNode recovers persisted state when restarted.
/// TODO(xp): move metasrv to a standalone file.
pub struct MetaRaftStore {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from disk) or created.
    is_open: bool,

    /// The sled db for log and raft_state.
    /// state machine is stored in another sled db since it contains user data and needs to be export/import as a whole.
    /// This db is also used to generate a locally unique id.
    /// Currently the id is used to create a unique snapshot id.
    _db: sled::Db,

    // Raft state includes:
    // id: NodeId,
    //     current_term,
    //     voted_for
    pub raft_state: RaftState,

    pub log: RaftLog,

    /// The Raft state machine.
    ///
    /// sled db has its own concurrency control, e.g., batch or transaction.
    /// But we still need a lock, when installing a snapshot, which is done by replacing the state machine:
    ///
    /// - Acquire a read lock to WRITE or READ. Transactional RW relies on sled concurrency control.
    /// - Acquire a write lock before installing a snapshot, to prevent any write to the db.
    pub state_machine: RwLock<StateMachine>,

    /// The current snapshot.
    pub current_snapshot: RwLock<Option<Snapshot>>,
}

// TODO(xp): the following is a draft struct when meta storage is migrated to sled based impl.
//           keep it until the migration is done.
// /// Impl a raft store.
// /// Includes:
// /// - raft state, e.g., node-id, current_term and which node it has voted-for.
// /// - raft log: distributed logs
// /// - raft state machine.
// pub struct C {
//     /// The Raft log.
//     pub log: sled::Tree,
//
//     /// The Raft state machine.
//     /// State machine is a relatively standalone component in raft.
//     /// In our impl a state machine has its own sled db.
//     pub state_machine: RwLock<StateMachine>,
//
//     /// The current snapshot of the state machine.
//     /// Currently snapshot data is a complete backup of the state machine and is not persisted on disk.
//     /// When server restarts, a new snapshot is created on demand.
//     pub current_snapshot: RwLock<Option<Snapshot>>,
// }

impl MetaRaftStore {
    /// If the instance is opened(true) from an existent state(e.g. load from disk) or created(false).
    /// TODO(xp): introduce a trait to define this behavior?
    pub fn is_open(&self) -> bool {
        self.is_open
    }
}

impl MetaRaftStore {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[tracing::instrument(level = "info", skip(config), fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> common_exception::Result<MetaRaftStore> {
        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        tracing::info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        tracing::info!("RaftLog opened");

        let (sm_id, prev_sm_id) = raft_state.read_state_machine_id()?;

        // There is a garbage state machine need to be cleaned.
        if sm_id != prev_sm_id {
            StateMachine::clean(config, prev_sm_id)?;
            raft_state.write_state_machine_id(&(sm_id, sm_id)).await?;
        }

        let sm = RwLock::new(StateMachine::open(config, sm_id).await?);
        let current_snapshot = RwLock::new(None);

        Ok(Self {
            id: raft_state.id,
            config: config.clone(),
            is_open,
            _db: db,
            raft_state,
            log,
            state_machine: sm,
            current_snapshot,
        })
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, StateMachine> {
        self.state_machine.write().await
    }

    pub async fn read_hard_state(&self) -> common_exception::Result<Option<HardState>> {
        self.raft_state.read_hard_state()
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[tracing::instrument(level = "debug", skip(self, data))]
    pub async fn install_snapshot(&self, data: &[u8]) -> common_exception::Result<()> {
        let mut sm = self.state_machine.write().await;

        let (sm_id, prev_sm_id) = self.raft_state.read_state_machine_id()?;
        if sm_id != prev_sm_id {
            return Err(ErrorCode::ConcurrentSnapshotInstall(format!(
                "another snapshot install is not finished yet: {} {}",
                sm_id, prev_sm_id
            )));
        }

        let new_sm_id = sm_id + 1;

        tracing::debug!("snapshot data len: {}", data.len());

        let snap: SerializableSnapshot = serde_json::from_slice(data)?;

        // If not finished, clean up the new tree.
        self.raft_state
            .write_state_machine_id(&(sm_id, new_sm_id))
            .await?;

        let new_sm = StateMachine::open(&self.config, new_sm_id).await?;
        tracing::info!(
            "insert all key-value into new state machine, n={}",
            snap.kvs.len()
        );

        let tree = &new_sm.sm_tree.tree;
        let nkvs = snap.kvs.len();
        for x in snap.kvs.into_iter() {
            let k = &x[0];
            let v = &x[1];
            tree.insert(k, v.clone())
                .map_err_to_code(ErrorCode::MetaStoreDamaged, || "fail to insert snapshot")?;
        }

        tracing::info!(
            "installed state machine from snapshot, no_kvs: {} last_applied: {}",
            nkvs,
            new_sm.get_last_applied()?,
        );

        tree.flush_async()
            .await
            .map_err_to_code(ErrorCode::MetaStoreDamaged, || "fail to flush snapshot")?;

        tracing::info!("flushed tree, no_kvs: {}", nkvs);

        // Start to use the new tree, the old can be cleaned.
        self.raft_state
            .write_state_machine_id(&(new_sm_id, sm_id))
            .await?;

        tracing::info!(
            "installed state machine from snapshot, last_applied: {}",
            new_sm.get_last_applied()?,
        );

        StateMachine::clean(&self.config, sm_id)?;

        self.raft_state
            .write_state_machine_id(&(new_sm_id, new_sm_id))
            .await?;

        // TODO(xp): use checksum to check consistency?

        *sm = new_sm;
        Ok(())
    }

    /// Go backwards through the log to find the most recent membership config <= `upto_index`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_membership_from_log(
        &self,
        upto_index: Option<u64>,
    ) -> anyhow::Result<MembershipConfig> {
        let right_bound = match upto_index {
            Some(upto) => Bound::Included(upto),
            None => Bound::Unbounded,
        };

        let it = self.log.range((Bound::Included(0), right_bound))?.rev();

        for rkv in it {
            let (_log_index, ent) = rkv?;
            match &ent.payload {
                EntryPayload::ConfigChange(cfg) => {
                    return Ok(cfg.membership.clone());
                }
                EntryPayload::SnapshotPointer(snap_ptr) => {
                    return Ok(snap_ptr.membership.clone());
                }
                _ => {}
            }
        }

        Ok(MembershipConfig::new_initial(self.id))
    }
}

#[async_trait]
impl RaftStorage<LogEntry, AppliedState> for MetaRaftStore {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn get_membership_config(&self) -> anyhow::Result<MembershipConfig> {
        self.get_membership_from_log(None).await
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn get_initial_state(&self) -> anyhow::Result<InitialState> {
        let hard_state = self.raft_state.read_hard_state()?;

        let membership = self.get_membership_config().await?;
        let sm = self.state_machine.read().await;

        match hard_state {
            Some(inner) => {
                let last = self.log.last()?;
                let last_log_id = match last {
                    Some((_index, ent)) => ent.log_id,
                    None => (0, 0).into(),
                };

                let last_applied_log = sm.get_last_applied()?;

                let st = InitialState {
                    last_log_id,
                    last_applied_log,
                    hard_state: inner,
                    membership,
                };
                tracing::info!("build initial state from storage: {:?}", st);
                Ok(st)
            }
            None => {
                let new = InitialState::new_initial(self.id);
                tracing::info!("create initial state: {:?}", new);
                self.raft_state.write_hard_state(&new.hard_state).await?;
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self, hs), fields(id=self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> anyhow::Result<()> {
        self.raft_state.write_hard_state(hs).await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> anyhow::Result<Vec<Entry<LogEntry>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!(
                "get_log_entries: invalid request, start({}) > stop({})",
                start,
                stop
            );
            return Ok(vec![]);
        }

        Ok(self.log.range_values(start..stop)?)
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> anyhow::Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!(
                "delete_logs_from: invalid request, start({}) > stop({:?})",
                start,
                stop
            );
            return Ok(());
        }

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            self.log.range_remove(start..*stop).await?;
        } else {
            self.log.range_remove(start..).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, entry), fields(id=self.id))]
    async fn append_entry_to_log(&self, entry: &Entry<LogEntry>) -> anyhow::Result<()> {
        self.log.insert(entry).await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self, entries), fields(id=self.id))]
    async fn replicate_to_log(&self, entries: &[Entry<LogEntry>]) -> anyhow::Result<()> {
        // TODO(xp): replicated_to_log should not block. Do the actual work in another task.
        self.log.append(entries).await?;
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn apply_entry_to_state_machine(
        &self,
        entry: &Entry<LogEntry>,
    ) -> anyhow::Result<AppliedState> {
        let mut sm = self.state_machine.write().await;
        let resp = sm.apply(entry).await?;
        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self, entries), fields(id=self.id))]
    async fn replicate_to_state_machine(&self, entries: &[&Entry<LogEntry>]) -> anyhow::Result<()> {
        let mut sm = self.state_machine.write().await;
        for entry in entries {
            sm.apply(*entry).await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn do_log_compaction(&self) -> anyhow::Result<CurrentSnapshotData<Self::Snapshot>> {
        // NOTE: do_log_compaction is guaranteed to be serialized called by RaftCore.

        // TODO(xp): add test of small chunk snapshot transfer and installation

        // TODO(xp): disallow to install a snapshot with smaller last_applied_log

        // 1. Take a serialized snapshot

        let (view, last_applied_log, last_membership, snapshot_id) =
            self.state_machine.write().await.snapshot()?;

        let data = StateMachine::serialize_snapshot(view)?;
        let snapshot_size = data.len();

        let snap_meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
            membership: last_membership.clone(),
        };

        let snapshot = Snapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };

        // 2. Remove logs that are included in snapshot.

        // When encountered a snapshot pointer, raft replication is switched to snapshot replication.
        self.log
            .insert(&Entry::new_snapshot_pointer(&snapshot.meta))
            .await?;

        self.log.range_remove(0..last_applied_log.index).await?;

        tracing::debug!("log range_remove complete");

        // Update the snapshot first.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        tracing::debug!(snapshot_size = snapshot_size, "log compaction complete");

        Ok(CurrentSnapshotData {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn create_snapshot(&self) -> anyhow::Result<Box<Self::Snapshot>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "info", skip(self, snapshot), fields(id=self.id))]
    async fn finalize_snapshot_installation(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::Snapshot>,
    ) -> anyhow::Result<()> {
        // TODO(xp): disallow installing a snapshot with smaller last_applied.

        tracing::debug!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = Snapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        tracing::debug!("SNAP META:{:?}", meta);

        // Replace state machine with the new one
        let res = self.install_snapshot(&new_snapshot.data).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("error: {:?} when install_snapshot", e);
            }
        };

        // When encountered a snapshot pointer, raft replication is switched to snapshot replication.
        self.log
            .insert(&Entry::new_snapshot_pointer(&new_snapshot.meta))
            .await?;

        self.log.range_remove(0..meta.last_log_id.index).await?;

        // Update current snapshot.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self), fields(id=self.id))]
    async fn get_current_snapshot(
        &self,
    ) -> anyhow::Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        tracing::info!("got snapshot start");
        let snap = match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(CurrentSnapshotData {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        };

        tracing::info!("got snapshot complete");

        snap
    }
}

// MetaRaft is a impl of the generic Raft handling meta data R/W.
pub type MetaRaft = Raft<LogEntry, AppliedState, Network, MetaRaftStore>;

// MetaNode is the container of meta data related components and threads, such as storage, the raft node and a raft-state monitor.
pub struct MetaNode {
    // metrics subscribes raft state changes. The most important field is the leader node id, to which all write operations should be forward.
    pub metrics_rx: watch::Receiver<RaftMetrics>,
    pub sto: Arc<MetaRaftStore>,
    pub raft: MetaRaft,
    pub running_tx: watch::Sender<()>,
    pub running_rx: watch::Receiver<()>,
    pub join_handles: Mutex<Vec<JoinHandle<common_exception::Result<()>>>>,
}

impl MetaRaftStore {
    pub async fn get_node(&self, node_id: &NodeId) -> common_exception::Result<Option<Node>> {
        let sm = self.state_machine.read().await;

        sm.get_node(node_id)
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> common_exception::Result<String> {
        let addr = self
            .get_node(node_id)
            .await?
            .map(|n| n.address)
            .ok_or_else(|| ErrorCode::UnknownNode(format!("node id: {}", node_id)))?;

        Ok(addr)
    }

    /// A non-voter is a node stored in raft store, but is not configured as a voter in the raft group.
    pub async fn list_non_voters(&self) -> HashSet<NodeId> {
        // TODO(xp): consistency
        let mut rst = HashSet::new();
        let ms = self
            .get_membership_config()
            .await
            .expect("fail to get membership");

        let node_ids = {
            let sm = self.state_machine.read().await;
            let sm_nodes = sm.nodes();
            sm_nodes.range_keys(..).expect("fail to list nodes")
        };
        for node_id in node_ids {
            // it has been added into this cluster and is not a voter.
            if !ms.contains(&node_id) {
                rst.insert(node_id);
            }
        }
        rst
    }
}

pub struct MetaNodeBuilder {
    node_id: Option<NodeId>,
    raft_config: Option<Config>,
    sto: Option<Arc<MetaRaftStore>>,
    monitor_metrics: bool,
    addr: Option<String>,
}

impl MetaNodeBuilder {
    pub async fn build(mut self) -> common_exception::Result<Arc<MetaNode>> {
        let node_id = self
            .node_id
            .ok_or_else(|| ErrorCode::InvalidConfig("node_id is not set"))?;

        let config = self
            .raft_config
            .take()
            .ok_or_else(|| ErrorCode::InvalidConfig("config is not set"))?;

        let sto = self
            .sto
            .take()
            .ok_or_else(|| ErrorCode::InvalidConfig("sto is not set"))?;

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

        let addr = if let Some(a) = self.addr.take() {
            a
        } else {
            sto.get_node_addr(&node_id).await?
        };
        tracing::info!("about to start grpc on {}", addr);

        MetaNode::start_grpc(mn.clone(), &addr).await?;

        Ok(mn)
    }

    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }
    pub fn sto(mut self, sto: Arc<MetaRaftStore>) -> Self {
        self.sto = Some(sto);
        self
    }
    pub fn addr(mut self, a: String) -> Self {
        self.addr = Some(a);
        self
    }
    pub fn monitor_metrics(mut self, b: bool) -> Self {
        self.monitor_metrics = b;
        self
    }
}

impl MetaNode {
    pub fn builder(config: &RaftConfig) -> MetaNodeBuilder {
        let raft_config = MetaNode::new_raft_config(config);

        MetaNodeBuilder {
            node_id: None,
            raft_config: Some(raft_config),
            sto: None,
            monitor_metrics: true,
            addr: None,
        }
    }

    pub fn new_raft_config(config: &RaftConfig) -> Config {
        // TODO(xp): configure cluster name.

        let hb = config.heartbeat_interval;

        Config::build("foo_cluster".into())
            .heartbeat_interval(hb)
            // Choose a rational value for election timeout.
            .election_timeout_min(hb * 8)
            .election_timeout_max(hb * 12)
            .install_snapshot_timeout(config.install_snapshot_timeout)
            .snapshot_policy(SnapshotPolicy::LogsSinceLast(
                config.snapshot_logs_since_last,
            ))
            .validate()
            .expect("building raft Config from databend-metasrv config")
    }

    /// Start the grpc service for raft communication and meta operation API.
    #[tracing::instrument(level = "info", skip(mn))]
    pub async fn start_grpc(mn: Arc<MetaNode>, addr: &str) -> common_exception::Result<()> {
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
            .map_err_to_code(ErrorCode::MetaServiceError, || "fail to serve")?;

            Ok::<(), common_exception::ErrorCode>(())
        });

        let mut jh = mn.join_handles.lock().await;
        jh.push(h);
        Ok(())
    }

    /// Start a metasrv node from initialized store.
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn open(config: &RaftConfig) -> common_exception::Result<Arc<MetaNode>> {
        let (mn, _is_open) = Self::open_create_boot(config, Some(()), None, None).await?;
        Ok(mn)
    }

    /// Open or create a metasrv node.
    /// Optionally boot a single node cluster.
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create an one in non-voter mode.
    /// 3. If `boot` is `Some` and it is just created, try to initialize a single-node cluster.
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn open_create_boot(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
        boot: Option<()>,
    ) -> common_exception::Result<(Arc<MetaNode>, bool)> {
        let sto = MetaRaftStore::open_create(config, open, create).await?;
        let is_open = sto.is_open();
        let sto = Arc::new(sto);
        let mut b = MetaNode::builder(config).sto(sto.clone());

        if is_open {
            // read id from sto, read listening addr from sto
            b = b.node_id(sto.id);
        } else {
            // read id from config, read listening addr from config.
            b = b.node_id(config.id).addr(config.raft_api_addr());
        }

        let mn = b.build().await?;

        tracing::info!("MetaNode started: {:?}", config);

        if !is_open && boot.is_some() {
            mn.init_cluster(config.raft_api_addr()).await?;
        }

        Ok((mn, is_open))
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn stop(&self) -> common_exception::Result<i32> {
        // TODO(xp): need to be reentrant.

        let mut rx = self.raft.metrics();

        self.raft
            .shutdown()
            .await
            .map_err_to_code(ErrorCode::MetaServiceError, || "fail to stop raft")?;
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
            let _rst = j
                .await
                .map_err_to_code(ErrorCode::MetaServiceError, || "fail to join")?;
            joined += 1;
        }

        tracing::info!("shutdown: id={}", self.sto.id);
        Ok(joined)
    }

    // spawn a monitor to watch raft state changes such as leader changes,
    // and manually add non-voter to cluster so that non-voter receives raft logs.
    pub async fn subscribe_metrics(mn: Arc<Self>, mut metrics_rx: watch::Receiver<RaftMetrics>) {
        //TODO: return a handle for join
        // TODO: every state change triggers add_non_voter!!!
        let mut running_rx = mn.running_rx.clone();
        let mut jh = mn.join_handles.lock().await;

        // TODO: reduce dependency: it does not need all of the fields in MetaNode
        let mn = mn.clone();

        let span = tracing::span!(tracing::Level::INFO, "watch-metrics");

        let h = tokio::task::spawn(
            {
                async move {
                    loop {
                        let changed = tokio::select! {
                            _ = running_rx.changed() => {
                               return Ok::<(), common_exception::ErrorCode>(());
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

                    Ok::<(), common_exception::ErrorCode>(())
                }
            }
            .instrument(span),
        );
        jh.push(h);
    }

    /// Boot up the first node to create a cluster.
    /// For every cluster this func should be called exactly once.
    /// When a node is initialized with boot or boot_non_voter, start it with databend_meta::new().
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn boot(
        node_id: NodeId,
        config: &RaftConfig,
    ) -> common_exception::Result<Arc<MetaNode>> {
        // 1. Bring a node up as non voter, start the grpc service for raft communication.
        // 2. Initialize itself as leader, because it is the only one in the new cluster.
        // 3. Add itself to the cluster storage by committing an `add-node` log so that the cluster members(only this node) is persisted.

        let mn = MetaNode::boot_non_voter(node_id, config).await?;
        mn.init_cluster(config.raft_api_addr()).await?;

        Ok(mn)
    }

    // Initialized a single node cluster by:
    // - Initializing raft membership.
    // - Adding current node into the meta data.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn init_cluster(&self, addr: String) -> common_exception::Result<()> {
        let node_id = self.sto.id;

        let mut cluster_node_ids = BTreeSet::new();
        cluster_node_ids.insert(node_id);

        let rst = self
            .raft
            .initialize(cluster_node_ids)
            .await
            .map_err(|x| ErrorCode::MetaServiceError(format!("{:?}", x)))?;

        tracing::info!("initialized cluster, rst: {:?}", rst);

        self.add_node(node_id, addr).await?;

        Ok(())
    }

    /// Boot a node that is going to join an existent cluster.
    /// For every node this should be called exactly once.
    /// When successfully initialized(e.g. received logs from raft leader), a node should be started with MetaNode::open().
    #[tracing::instrument(level = "info", skip(config), fields(config_id=config.config_id.as_str()))]
    pub async fn boot_non_voter(
        node_id: NodeId,
        config: &RaftConfig,
    ) -> common_exception::Result<Arc<MetaNode>> {
        // TODO(xp): what if fill in the node info into an empty state-machine, then MetaNode can be started without delaying grpc.

        let mut config = config.clone();
        config.id = node_id;

        let (mn, _is_open) = Self::open_create_boot(&config, None, Some(()), None).await?;

        tracing::info!("booted non-voter: {:?}", config);

        Ok(mn)
    }

    /// When a leader is established, it is the leader's responsibility to setup replication from itself to non-voters, AKA learners.
    /// async-raft does not persist the node set of non-voters, thus we need to do it manually.
    /// This fn should be called once a node found it becomes leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn add_configured_non_voters(&self) -> common_exception::Result<()> {
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_node(&self, node_id: &NodeId) -> common_exception::Result<Option<Node>> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_node(node_id)
    }

    /// Add a new node into this cluster.
    /// The node info is committed with raft, thus it must be called on an initialized node.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn add_node(
        &self,
        node_id: NodeId,
        addr: String,
    ) -> common_exception::Result<AppliedState> {
        // TODO: use txid?
        let _resp = self
            .write(LogEntry {
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

    pub async fn get_state_machine(&self) -> RwLockReadGuard<'_, StateMachine> {
        self.sto.state_machine.read().await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn lookup_table_id(
        &self,
        db_id: u64,
        name: &str,
    ) -> Result<Option<SeqV<TableLookupValue>>, ErrorCode> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.table_lookup().get(
            &(TableLookupKey {
                database_id: db_id,
                table_name: name.to_string(),
            }),
        )
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_tables(&self, db_name: &str) -> Result<Vec<TableInfo>, ErrorCode> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_tables(db_name)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_table_by_id(&self, tid: &u64) -> Result<Option<SeqV<TableMeta>>, ErrorCode> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_table_by_id(tid)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_kv(&self, key: &str) -> common_exception::Result<Option<SeqV<Vec<u8>>>> {
        // inconsistent get: from local state machine

        let sm = self.sto.state_machine.read().await;
        sm.get_kv(key)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn mget_kv(
        &self,
        keys: &[impl AsRef<str> + std::fmt::Debug],
    ) -> common_exception::Result<Vec<Option<SeqV<Vec<u8>>>>> {
        // inconsistent get: from local state machine
        let sm = self.sto.state_machine.read().await;
        sm.mget_kv(keys)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn prefix_list_kv(
        &self,
        prefix: &str,
    ) -> common_exception::Result<Vec<(String, SeqV<Vec<u8>>)>> {
        // inconsistent get: from local state machine
        let sm = self.sto.state_machine.read().await;
        sm.prefix_list_kv(prefix)
    }

    /// Submit a write request to the known leader. Returns the response after applying the request.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write(&self, req: LogEntry) -> common_exception::Result<AppliedState> {
        let mut curr_leader = self.get_leader().await;
        loop {
            let rst = if curr_leader == self.sto.id {
                self.write_to_local_leader(req.clone()).await?
            } else {
                // forward to leader

                let addr = self.sto.get_node_addr(&curr_leader).await?;

                // TODO: retry
                let mut client = MetaServiceClient::connect(format!("http://{}", addr))
                    .await
                    .map_err(|e| ErrorCode::CannotConnectNode(e.to_string()))?;
                let resp = client.write(req.clone()).await?;
                let rst: Result<AppliedState, RetryableError> = resp.into_inner().into();
                rst
            };

            match rst {
                Ok(resp) => return Ok(resp),
                Err(write_err) => match write_err {
                    RetryableError::ForwardToLeader { leader } => curr_leader = leader,
                },
            }
        }
    }

    /// Try to get the leader from the latest metrics of the local raft node.
    /// If leader is absent, wait for an metrics update in which a leader is set.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_leader(&self) -> NodeId {
        // fast path: there is a known leader

        if let Some(l) = self.metrics_rx.borrow().current_leader {
            return l;
        }

        // slow path: wait loop

        // Need to clone before calling changed() on it.
        // Otherwise other thread waiting on changed() may not receive the change event.
        let mut rx = self.metrics_rx.clone();

        loop {
            // NOTE:
            // The metrics may have already changed before we cloning it.
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

    /// Write a meta log through local raft node.
    /// It works only when this node is the leader,
    /// otherwise it returns ClientWriteError::ForwardToLeader error indicating the latest leader.
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn write_to_local_leader(
        &self,
        req: LogEntry,
    ) -> common_exception::Result<Result<AppliedState, RetryableError>> {
        let write_rst = self.raft.client_write(ClientWriteRequest::new(req)).await;

        tracing::debug!("raft.client_write rst: {:?}", write_rst);

        match write_rst {
            Ok(resp) => Ok(Ok(resp.data)),
            Err(cli_write_err) => match cli_write_err {
                // fatal error
                ClientWriteError::RaftError(raft_err) => {
                    Err(ErrorCode::MetaServiceError(raft_err.to_string()))
                }
                // retryable error
                ClientWriteError::ForwardToLeader(_, leader) => match leader {
                    Some(id) => Ok(Err(RetryableError::ForwardToLeader { leader: id })),
                    None => Err(ErrorCode::MetaServiceUnavailable(
                        "no leader to write".to_string(),
                    )),
                },
            },
        }
    }
}
