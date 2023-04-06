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

use std::io::Cursor;
use std::io::ErrorKind;

use anyerror::AnyError;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::sync::RwLockWriteGuard;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state_machine::SerializableSnapshot;
use common_meta_raft_store::state_machine::StateMachine;
use common_meta_raft_store::state_machine::StoredSnapshot;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft::ErrorSubject;
use common_meta_sled_store::openraft::ErrorVerb;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::Endpoint;
use common_meta_types::Membership;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaStartupError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Snapshot;
use common_meta_types::SnapshotMeta;
use common_meta_types::StorageError;
use tracing::info;

use crate::export::vec_kv_to_json;
use crate::metrics::raft_metrics;
use crate::store::ToStorageError;
use crate::Opened;

/// An storage implementing the `async_raft::RaftStorage` trait.
///
/// It is the stateful part in a raft implementation.
/// This store is backed by a sled db, contents are stored in 3 trees:
///   state:
///       id
///       vote
///   log
///   state_machine
pub struct StoreInner {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from fs) or created.
    is_opened: bool,

    /// The sled db for log and raft_state.
    /// state machine is stored in another sled db since it contains user data and needs to be export/import as a whole.
    /// This db is also used to generate a locally unique id.
    /// Currently the id is used to create a unique snapshot id.
    pub(crate) db: sled::Db,

    /// Raft state includes:
    /// id: NodeId,
    ///     current_term,
    ///     voted_for
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
    pub current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl AsRef<StoreInner> for StoreInner {
    fn as_ref(&self) -> &StoreInner {
        self
    }
}

impl Opened for StoreInner {
    /// If the instance is opened(true) from an existent state(e.g. load from fs) or created(false).
    fn is_opened(&self) -> bool {
        self.is_opened
    }
}

impl StoreInner {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[tracing::instrument(level = "debug", skip_all, fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<StoreInner, MetaStartupError> {
        info!("open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        info!("RaftLog opened");

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
            db,
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

    #[tracing::instrument(level = "debug", skip_all, fields(id=self.id))]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, StorageError> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!("log compaction start");

        // 1. Take a serialized snapshot

        let (snap, last_applied_log, last_membership, snapshot_id) = match self
            .state_machine
            .write()
            .await
            .build_snapshot()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("build_snapshot", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        info!("log compaction serialization start");

        let data = serde_json::to_vec(&snap)
            .map_err(MetaStorageError::from)
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)?;

        let snapshot_size = data.len();

        let snap_meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id: snapshot_id.to_string(),
        };

        let snapshot = StoredSnapshot {
            meta: snap_meta.clone(),
            data: data.clone(),
        };

        // Update the snapshot first.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        info!(snapshot_size = snapshot_size, "log compaction complete");

        Ok(Snapshot {
            meta: snap_meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[tracing::instrument(level = "debug", skip(self, data))]
    pub async fn do_install_snapshot(&self, data: &[u8]) -> Result<(), MetaStorageError> {
        let mut sm = self.state_machine.write().await;

        let (sm_id, prev_sm_id) = self.raft_state.read_state_machine_id()?;
        if sm_id != prev_sm_id {
            return Err(MetaStorageError::SnapshotError(AnyError::error(format!(
                "another snapshot install is not finished yet: {} {}",
                sm_id, prev_sm_id
            ))));
        }

        let new_sm_id = sm_id + 1;

        info!("snapshot data len: {}", data.len());

        let snap: SerializableSnapshot = serde_json::from_slice(data)?;

        // If not finished, clean up the new tree.
        self.raft_state
            .write_state_machine_id(&(sm_id, new_sm_id))
            .await?;

        let new_sm = StateMachine::open(&self.config, new_sm_id).await?;
        info!(
            "insert all key-value into new state machine, n={}",
            snap.kvs.len()
        );

        let tree = &new_sm.sm_tree.tree;
        let nkvs = snap.kvs.len();
        for x in snap.kvs.into_iter() {
            let k = &x[0];
            let v = &x[1];
            tree.insert(k, v.clone())?;
        }

        info!(
            "installed state machine from snapshot, no_kvs: {} last_applied: {:?}",
            nkvs,
            new_sm.get_last_applied()?,
        );

        tree.flush_async().await?;

        info!("flushed tree, no_kvs: {}", nkvs);

        // Start to use the new tree, the old can be cleaned.
        self.raft_state
            .write_state_machine_id(&(new_sm_id, sm_id))
            .await?;

        info!(
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

        let state_kvs = self
            .raft_state
            .inner
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        for kv in state_kvs.iter() {
            let line = vec_kv_to_json(&self.raft_state.inner.name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        let log_kvs = self
            .log
            .inner
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
        for kv in log_kvs.iter() {
            let line = vec_kv_to_json(&self.log.inner.name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        let name = self.state_machine.write().await.sm_tree.name.clone();
        let sm_kvs = self
            .state_machine
            .write()
            .await
            .sm_tree
            .export()
            .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;

        for kv in sm_kvs.iter() {
            let line = vec_kv_to_json(&name, kv)
                .map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))?;
            res.push(line);
        }

        Ok(res)
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, MetaError> {
        let sm = self.state_machine.read().await;

        let n = sm.get_node(node_id)?;
        Ok(n)
    }

    /// Return a list of nodes of the corresponding node-ids returned by `list_ids`.
    pub(crate) async fn get_nodes(
        &self,
        list_ids: impl Fn(&Membership) -> Vec<NodeId>,
    ) -> Result<Vec<Node>, MetaStorageError> {
        let sm = self.state_machine.read().await;
        let ms = sm.get_membership()?;

        tracing::debug!("in-statemachine membership: {:?}", ms);

        let membership = match &ms {
            Some(membership) => membership.membership(),
            None => return Ok(vec![]),
        };

        let ids = list_ids(membership);
        tracing::debug!("filtered node ids: {:?}", ids);
        let mut ns = vec![];

        for id in ids {
            let node = sm.get_node(&id)?;
            if let Some(x) = node {
                ns.push(x);
            }
        }

        Ok(ns)
    }

    pub async fn get_node_endpoint(&self, node_id: &NodeId) -> Result<Endpoint, MetaError> {
        let endpoint = self
            .get_node(node_id)
            .await?
            .map(|n| n.endpoint)
            .ok_or_else(|| {
                MetaNetworkError::GetNodeAddrError(format!(
                    "fail to get endpoint of node_id: {}",
                    node_id
                ))
            })?;

        Ok(endpoint)
    }
}
