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

use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Cursor;
use std::io::ErrorKind;
use std::ops::RangeBounds;

use anyerror::AnyError;
use common_base::tokio::sync::RwLock;
use common_base::tokio::sync::RwLockWriteGuard;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::SerializableSnapshot;
use common_meta_raft_store::state_machine::Snapshot;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft;
use common_meta_sled_store::openraft::storage::LogState;
use common_meta_sled_store::openraft::EffectiveMembership;
use common_meta_sled_store::openraft::ErrorSubject;
use common_meta_sled_store::openraft::ErrorVerb;
use common_meta_sled_store::openraft::StateMachineChanges;
use common_meta_types::error_context::WithContext;
use common_meta_types::AppliedState;
use common_meta_types::LogEntry;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaResult;
use common_meta_types::MetaStorageError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_tracing::tracing;
use openraft::async_trait::async_trait;
use openraft::raft::Entry;
use openraft::storage::HardState;
use openraft::LogId;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StorageError;

use crate::export::exported_line_to_json;
use crate::store::ToStorageError;
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
    ) -> MetaResult<MetaRaftStore> {
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

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[tracing::instrument(level = "debug", skip(self, data))]
    pub async fn install_snapshot(&self, data: &[u8]) -> Result<(), MetaStorageError> {
        let mut sm = self.state_machine.write().await;

        let (sm_id, prev_sm_id) = self.raft_state.read_state_machine_id()?;
        if sm_id != prev_sm_id {
            return Err(MetaStorageError::SnapshotError(AnyError::error(format!(
                "another snapshot install is not finished yet: {} {}",
                sm_id, prev_sm_id
            ))));
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
            tree.insert(k, v.clone()).context(|| "insert snapshot")?;
        }

        tracing::info!(
            "installed state machine from snapshot, no_kvs: {} last_applied: {:?}",
            nkvs,
            new_sm.get_last_applied()?,
        );

        tree.flush_async().await.context(|| "flush snapshot")?;

        tracing::info!("flushed tree, no_kvs: {}", nkvs);

        // Start to use the new tree, the old can be cleaned.
        self.raft_state
            .write_state_machine_id(&(new_sm_id, sm_id))
            .await?;

        tracing::info!(
            "installed state machine from snapshot, last_applied: {:?}",
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

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn export(&self) -> Result<Vec<String>, std::io::Error> {
        let mut res = vec![];

        let state_kvs = self.raft_state.inner.export()?;
        let log_kvs = self.log.inner.export()?;
        let sm_kvs = self.state_machine.write().await.sm_tree.export()?;

        for kv in state_kvs.iter() {
            let line = exported_line_to_json("state", kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }
        for kv in log_kvs.iter() {
            let line = exported_line_to_json("log", kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }
        for kv in sm_kvs.iter() {
            let line = exported_line_to_json("sm", kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        Ok(res)
    }
}

#[async_trait]
impl RaftStorage<LogEntry, AppliedState> for MetaRaftStore {
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "debug", skip(self, hs), fields(id=self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<(), StorageError> {
        self.raft_state
            .write_hard_state(hs)
            .await
            .map_to_sto_err(ErrorSubject::HardState, ErrorVerb::Write)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RB,
    ) -> Result<Vec<Entry<LogEntry>>, StorageError> {
        let entries = self
            .log
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        Ok(entries)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn delete_conflict_logs_since(&self, log_id: LogId) -> Result<(), StorageError> {
        self.log
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn purge_logs_upto(&self, log_id: LogId) -> Result<(), StorageError> {
        self.log
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)?;
        self.log
            .range_remove(..=log_id.index)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
    async fn append_to_log(&self, entries: &[&Entry<LogEntry>]) -> Result<(), StorageError> {
        // TODO(xp): replicated_to_log should not block. Do the actual work in another task.
        let entries = entries.iter().map(|x| (*x).clone()).collect::<Vec<_>>();
        self.log
            .append(&entries)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<LogEntry>],
    ) -> Result<Vec<AppliedState>, StorageError> {
        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;
        for entry in entries {
            let r = sm
                .apply(*entry)
                .await
                .map_to_sto_err(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)?;
            res.push(r);
        }
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn build_snapshot(
        &self,
    ) -> Result<openraft::storage::Snapshot<Self::SnapshotData>, StorageError> {
        // NOTE: do_log_compaction is guaranteed to be serialized called by RaftCore.

        // TODO(xp): add test of small chunk snapshot transfer and installation

        // TODO(xp): disallow to install a snapshot with smaller last_applied_log

        // 1. Take a serialized snapshot

        let (view, last_applied_log, snapshot_id) = self
            .state_machine
            .write()
            .await
            .snapshot()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;

        let data = StateMachine::serialize_snapshot(view)
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;
        let snapshot_size = data.len();

        let snap_meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = Snapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };

        // Update the snapshot first.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        tracing::debug!(snapshot_size = snapshot_size, "log compaction complete");

        Ok(openraft::storage::Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn begin_receiving_snapshot(&self) -> Result<Box<Self::SnapshotData>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "debug", skip(self, snapshot), fields(id=self.id))]
    async fn install_snapshot(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges, StorageError> {
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

        // Update current snapshot.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<openraft::storage::Snapshot<Self::SnapshotData>>, StorageError> {
        tracing::info!("got snapshot start");
        let snap = match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(openraft::storage::Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        };

        tracing::info!("got snapshot complete");

        snap
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_hard_state(&self) -> Result<Option<HardState>, StorageError> {
        let hard_state = self
            .raft_state
            .read_hard_state()
            .map_to_sto_err(ErrorSubject::HardState, ErrorVerb::Read)?;
        Ok(hard_state)
    }

    async fn get_log_state(&self) -> Result<LogState, StorageError> {
        let last_purged_log_id = self
            .log
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        let last = self
            .log
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x.1.log_id),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn last_applied_state(
        &self,
    ) -> Result<(Option<LogId>, Option<EffectiveMembership>), StorageError> {
        let sm = self.state_machine.read().await;
        let last_applied = sm
            .get_last_applied()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;
        let last_membership = sm
            .get_membership()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;
        Ok((last_applied, last_membership))
    }
}

impl MetaRaftStore {
    pub async fn get_node(&self, node_id: &NodeId) -> MetaResult<Option<Node>> {
        let sm = self.state_machine.read().await;

        sm.get_node(node_id)
    }

    pub async fn get_voters(&self) -> MetaResult<Vec<Node>> {
        let sm = self.state_machine.read().await;
        let ms = self.get_membership().await.expect("get membership config");

        match ms {
            Some(membership) => {
                let nodes = sm.nodes().range_kvs(..).expect("get nodes failed");
                let voters = nodes
                    .into_iter()
                    .filter(|(node_id, _)| membership.membership.contains(node_id))
                    .map(|(_, node)| node)
                    .collect();
                Ok(voters)
            }
            None => Ok(vec![]),
        }
    }

    pub async fn get_non_voters(&self) -> MetaResult<Vec<Node>> {
        let sm = self.state_machine.read().await;
        let ms = self.get_membership().await.expect("get membership config");

        match ms {
            Some(membership) => {
                let nodes = sm.nodes().range_kvs(..).expect("get nodes failed");
                let non_voters = nodes
                    .into_iter()
                    .filter(|(node_id, _)| !membership.membership.contains(node_id))
                    .map(|(_, node)| node)
                    .collect();
                Ok(non_voters)
            }
            None => Ok(vec![]),
        }
    }

    pub async fn get_node_addr(&self, node_id: &NodeId) -> MetaResult<String> {
        let addr = self
            .get_node(node_id)
            .await?
            .map(|n| n.address)
            .ok_or_else(|| MetaNetworkError::GetNodeAddrError(format!("node id: {}", node_id)))?;

        Ok(addr)
    }

    /// A non-voter is a node stored in raft store, but is not configured as a voter in the raft group.
    pub async fn list_non_voters(&self) -> HashSet<NodeId> {
        // TODO(xp): consistency
        let mut rst = HashSet::new();
        let membership = self.get_membership().await.expect("fail to get membership");

        let membership = match membership {
            None => {
                return HashSet::new();
            }
            Some(x) => x,
        };

        let node_ids = {
            let sm = self.state_machine.read().await;
            let sm_nodes = sm.nodes();
            sm_nodes.range_keys(..).expect("fail to list nodes")
        };
        for node_id in node_ids {
            // it has been added into this cluster and is not a voter.
            if !membership.membership.contains(&node_id) {
                rst.insert(node_id);
            }
        }
        rst
    }
}
