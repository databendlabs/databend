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

use std::collections::HashSet;
use std::io::Cursor;

use async_raft::async_trait::async_trait;
use async_raft::raft::Entry;
use async_raft::raft::EntryPayload;
use async_raft::raft::MembershipConfig;
use async_raft::storage::CurrentSnapshotData;
use async_raft::storage::HardState;
use async_raft::storage::InitialState;
use async_raft::RaftStorage;
use async_raft::SnapshotMeta;
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::ops::Bound;
use common_base::tokio::sync::RwLock;
use common_base::tokio::sync::RwLockWriteGuard;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::AppliedState;
use common_meta_raft_store::state_machine::SerializableSnapshot;
use common_meta_raft_store::state_machine::Snapshot;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::get_sled_db;
use common_meta_types::LogEntry;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_tracing::tracing;

use crate::errors::ShutdownError;
use crate::Opened;

/// An storage implementing the `async_raft::RaftStorage` trait.
///
/// It is the stateful part in a raft impl.
/// This store is backed by a sled db, contents are stored in 3 trees:
///   state:
///       id
///       hard_state
///   log
///   state_machine
pub struct MetaRaftStore {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from disk) or created.
    is_opened: bool,

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

impl Opened for MetaRaftStore {
    /// If the instance is opened(true) from an existent state(e.g. load from disk) or created(false).
    fn is_opened(&self) -> bool {
        self.is_opened
    }
}

impl MetaRaftStore {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[tracing::instrument(level = "info", skip(config,open,create), fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> common_exception::Result<MetaRaftStore> {
        tracing::info!("open: {:?}, create: {:?}", open, create);

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
            is_opened: is_open,
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
        let right_bound = upto_index.map_or(Bound::Unbounded, Bound::Included);

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
        let sm = self.state_machine.write().await;
        let resp = sm.apply(entry).await?;
        Ok(resp)
    }

    #[tracing::instrument(level = "info", skip(self, entries), fields(id=self.id))]
    async fn replicate_to_state_machine(&self, entries: &[&Entry<LogEntry>]) -> anyhow::Result<()> {
        let sm = self.state_machine.write().await;
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
