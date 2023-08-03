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

use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use common_base::base::tokio;
use common_base::base::tokio::io::AsyncBufReadExt;
use common_base::base::tokio::io::BufReader;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::sync::RwLockWriteGuard;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::key_spaces::RaftStateKV;
use common_meta_raft_store::key_spaces::RaftStoreEntry;
use common_meta_raft_store::log::RaftLog;
use common_meta_raft_store::ondisk::DATA_VERSION;
use common_meta_raft_store::ondisk::TREE_HEADER;
use common_meta_raft_store::sm_v002::leveled_store::level_data::LevelData;
use common_meta_raft_store::sm_v002::sm_v002::SMV002;
use common_meta_raft_store::sm_v002::snapshot_store::SnapshotStoreError;
use common_meta_raft_store::sm_v002::snapshot_store::SnapshotStoreV002;
use common_meta_raft_store::state::RaftState;
use common_meta_raft_store::state::RaftStateKey;
use common_meta_raft_store::state::RaftStateValue;
use common_meta_raft_store::state_machine::MetaSnapshotId;
use common_meta_raft_store::state_machine::StoredSnapshot;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::openraft::ErrorSubject;
use common_meta_sled_store::openraft::ErrorVerb;
use common_meta_sled_store::openraft::RaftLogId;
use common_meta_sled_store::SledTree;
use common_meta_stoerr::MetaStorageError;
use common_meta_types::Endpoint;
use common_meta_types::LogId;
use common_meta_types::Membership;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::MetaStartupError;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Snapshot;
use common_meta_types::SnapshotData;
use common_meta_types::SnapshotMeta;
use common_meta_types::StorageError;
use common_meta_types::StorageIOError;
use common_meta_types::Vote;
use log::as_display;
use log::debug;
use log::info;
use log::warn;

use crate::export::vec_kv_to_json;
use crate::Opened;

/// This is the inner store that provides support utilities for implementing the raft storage API.
///
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

    pub(crate) config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from fs) or created.
    is_opened: bool,

    /// The sled db for log, raft_state and state machine.
    pub(crate) db: sled::Db,

    /// Raft state includes:
    /// id: NodeId,
    ///     current_term,
    ///     voted_for
    pub raft_state: RaftState,

    pub log: RaftLog,

    /// The Raft state machine.
    pub state_machine: Arc<RwLock<SMV002>>,

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
    #[minitrace::trace]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<StoreInner, MetaStartupError> {
        info!(config_id = as_display!(&config.config_id); "open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        info!("RaftLog opened");

        // TODO(1): remove read_state_machine_id();
        // TODO(1): StateMachine::clean()

        fn to_startup_err(e: impl std::error::Error + 'static) -> MetaStartupError {
            let ae = AnyError::new(&e);
            let store_err = MetaStorageError::SnapshotError(ae);
            MetaStartupError::StoreOpenError(store_err)
        }

        let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, config.clone());
        let last = snapshot_store
            .load_last_snapshot()
            .await
            .map_err(to_startup_err)?;

        let (sm, stored_snapshot) = if let Some((id, snapshot)) = last {
            let (sm, meta) = Self::rebuild_state_machine(&id, snapshot)
                .await
                .map_err(to_startup_err)?;

            info!(
                "rebuilt state machine from last snapshot({:?}), meta: {:?}",
                id, meta
            );
            (sm, Some(StoredSnapshot { meta }))
        } else {
            info!("No snapshot, skip rebuilding state machine");
            (Default::default(), None)
        };

        Ok(Self {
            id: raft_state.id,
            config: config.clone(),
            is_opened: is_open,
            db,
            raft_state,
            log,
            state_machine: sm,
            current_snapshot: RwLock::new(stored_snapshot),
        })
    }

    /// Return a snapshot store of this instance.
    pub fn snapshot_store(&self) -> SnapshotStoreV002 {
        SnapshotStoreV002::new(DATA_VERSION, self.config.clone())
    }

    async fn rebuild_state_machine(
        id: &MetaSnapshotId,
        snapshot: SnapshotData,
    ) -> Result<(Arc<RwLock<SMV002>>, SnapshotMeta), io::Error> {
        info!("rebuild state machine from last snapshot({:?})", id);

        let sm = Arc::new(RwLock::new(SMV002::default()));

        SMV002::install_snapshot(sm.clone(), Box::new(snapshot)).await?;

        let (last_applied, last_membership) = {
            let sm = sm.read().await;
            let last_applied = *sm.last_applied_ref();
            let last_membership = sm.last_membership_ref().clone();

            (last_applied, last_membership)
        };

        let meta = SnapshotMeta {
            snapshot_id: id.to_string(),
            last_log_id: last_applied,
            last_membership,
        };

        Ok((sm, meta))
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, SMV002> {
        self.state_machine.write().await
    }

    #[minitrace::trace]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, StorageError> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!(id = self.id; "do_build_snapshot start");

        let (mut snapshot_meta, data_entries) = self.build_compacted_snapshot().await;

        info!("do_build_snapshot writing snapshot start");

        let mut snapshot_store = self.snapshot_store();

        // Move heavy load to a blocking thread pool.
        let (snapshot_id, snapshot_size) = tokio::task::block_in_place({
            let sto = &mut snapshot_store;
            let meta = snapshot_meta.clone();
            let sleep = self.get_delay_config("write").await;

            move || {
                Self::testing_sleep("write", sleep);
                Self::write_snapshot(sto, meta, data_entries)
            }
        })?;

        snapshot_store.clean_old_snapshots().await?;

        assert_eq!(
            snapshot_id.last_applied, snapshot_meta.last_log_id,
            "snapshot_id.last_applied: {:?} must equal snapshot_meta.last_log_id: {:?}",
            snapshot_id.last_applied, snapshot_meta.last_log_id
        );
        snapshot_meta.snapshot_id = snapshot_id.to_string();

        info!(snapshot_size = as_display!(snapshot_size); "do_build_snapshot complete");

        {
            // TODO: replace StoredSnapshot with SnapshotMeta?
            let snapshot = StoredSnapshot {
                meta: snapshot_meta.clone(),
            };

            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        let r = snapshot_store
            .new_reader(&snapshot_meta.snapshot_id)
            .await
            .map_err(|e| {
                let e = StorageIOError::new(
                    ErrorSubject::Snapshot(Some(snapshot_meta.signature())),
                    ErrorVerb::Read,
                    &e,
                );
                StorageError::from(e)
            })?;

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(r),
        })
    }

    /// Return the delay config for testing.
    async fn get_delay_config(&self, key: &str) -> Duration {
        if cfg!(debug_assertions) {
            let sm = self.get_state_machine().await;
            let c = sm.blocking_config();
            match key {
                "write" => c.write_snapshot,
                "compact" => c.compact_snapshot,
                _ => {
                    unreachable!("unknown key: {}", key);
                }
            }
        } else {
            Duration::from_secs(0)
        }
    }

    /// Sleep in blocking mode for testing.
    fn testing_sleep(key: &str, sleep: Duration) {
        #[allow(clippy::collapsible_if)]
        if cfg!(debug_assertions) {
            if !sleep.is_zero() {
                warn!("start    {} sleep: {:?}", key, sleep);
                std::thread::sleep(sleep);
                warn!("finished {} sleep", key);
            }
        }
    }

    /// Compact a snapshot and return the meta and data entries.
    async fn build_compacted_snapshot(&self) -> (SnapshotMeta, Vec<RaftStoreEntry>) {
        let mut snapshot = {
            let mut s = self.state_machine.write().await;
            s.full_snapshot()
        };

        let snapshot_meta = Self::build_snapshot_meta(snapshot.top().data_ref());

        // Compact multi levels into one to get rid of tombstones
        // Move heavy load task to a blocking thread pool.
        let data_entries = tokio::task::block_in_place({
            let s = &mut snapshot;
            let sleep = self.get_delay_config("compact").await;

            move || {
                Self::testing_sleep("compact", sleep);

                s.compact();
                s.export().collect::<Vec<_>>()
            }
        });

        {
            let mut s = self.state_machine.write().await;
            s.replace_base(&snapshot);
        }

        (snapshot_meta, data_entries)
    }

    fn build_snapshot_meta(level_data: &LevelData) -> SnapshotMeta {
        let last_applied = *level_data.last_applied_ref();
        let last_membership = level_data.last_membership_ref().clone();

        let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);

        SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_id: last_applied,
            last_membership,
        }
    }

    fn write_snapshot(
        snapshot_store: &mut SnapshotStoreV002,
        snapshot_meta: SnapshotMeta,
        data: impl IntoIterator<Item = RaftStoreEntry>,
    ) -> Result<(MetaSnapshotId, u64), SnapshotStoreError> {
        let mut writer = snapshot_store.new_writer()?;

        writer.write_entries::<io::Error>(data).map_err(|e| {
            SnapshotStoreError::write(e).with_meta("serialize entries", &snapshot_meta)
        })?;

        let (snapshot_id, file_size) = writer
            .commit(None)
            .map_err(|e| SnapshotStoreError::write(e).with_meta("writer.commit", &snapshot_meta))?;

        Ok((snapshot_id, file_size))
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[minitrace::trace]
    pub async fn do_install_snapshot(
        &self,
        data: Box<SnapshotData>,
    ) -> Result<(), MetaStorageError> {
        //

        SMV002::install_snapshot(self.state_machine.clone(), data)
            .await
            .map_err(|e| {
                MetaStorageError::SnapshotError(
                    AnyError::new(&e).add_context(|| "replacing state-machine with snapshot"),
                )
            })?;

        // TODO(1): read_state_machine_id() and write_state_machine_id() is no longer used.
        // TODO(xp): use checksum to check consistency?

        Ok(())
    }

    /// Export data that can be used to restore a meta-service node.
    #[minitrace::trace]
    pub async fn export(&self) -> Result<Vec<String>, io::Error> {
        // NOTE:
        // Hold the snapshot lock to prevent snapshot from being replaced until exporting finished.
        // Holding this lock prevent logs from being purged.
        let current_snapshot = self.current_snapshot.read().await;

        let mut res = vec![];

        // Export data header first
        {
            let header_tree = SledTree::open(&self.db, TREE_HEADER, false)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

            let header_kvs = header_tree
                .export()
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

            for kv in header_kvs.iter() {
                let line = vec_kv_to_json(TREE_HEADER, kv)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                res.push(line);
            }
        }

        // Export RaftState
        //
        // Although vote and log must be consistent,
        // it is OK to export RaftState and logs without transaction protection,
        // if it guarantees no logs have a greater `vote` than `RaftState.HardState`.

        let exported_vote = {
            let tree_name = &self.raft_state.inner.name;

            let ks = self.raft_state.inner.key_space::<RaftStateKV>();

            let id = ks
                .get(&RaftStateKey::Id)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
                .map(NodeId::from);
            if let Some(id) = id {
                let ent_id = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::Id,
                    value: RaftStateValue::NodeId(id),
                };

                let s = serde_json::to_string(&(tree_name, ent_id))
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                res.push(s);
            }

            let vote = ks
                .get(&RaftStateKey::HardState)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
                .map(Vote::from);
            if let Some(vote) = vote {
                let ent_vote = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::HardState,
                    value: RaftStateValue::HardState(vote),
                };

                let s = serde_json::to_string(&(tree_name, ent_vote))
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                res.push(s);
            }

            let committed = ks
                .get(&RaftStateKey::Committed)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
                .and_then(Option::<LogId>::from);

            let ent_committed = RaftStoreEntry::RaftStateKV {
                key: RaftStateKey::Committed,
                value: RaftStateValue::Committed(committed),
            };

            let s = serde_json::to_string(&(tree_name, ent_committed))
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

            res.push(s);

            // NOTE: `vote` is used in the following code.
            vote.unwrap_or_default()
        };

        // Export logs that has smaller or equal leader id as `vote`
        {
            let tree_name = &self.log.inner.name;

            let log_kvs = self
                .log
                .inner
                .export()
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
            for kv in log_kvs.iter() {
                let kv_entry = RaftStoreEntry::deserialize(&kv[0], &kv[1])
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                if let RaftStoreEntry::Logs { ref value, .. } = kv_entry {
                    if value.get_log_id().leader_id > exported_vote.leader_id {
                        warn!(
                            "found log({}) with greater leader id than vote({}), skip exporting logs",
                            value.get_log_id(),
                            exported_vote
                        );
                        break;
                    }
                }

                let tree_kv = (tree_name, kv_entry);
                let line = serde_json::to_string(&tree_kv)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                res.push(line);
            }
        }

        // Export state machine
        {
            // NOTE:
            // This name had been used by the sled tree based sm.
            // Do not change it for keeping compatibility.
            let tree_name = "state_machine/0";

            let snapshot = current_snapshot.clone();
            if let Some(s) = snapshot {
                let meta = s.meta;

                let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, self.config.clone());

                let f = snapshot_store
                    .new_reader(&meta.snapshot_id)
                    .await
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                let bf = BufReader::new(f);
                let mut lines = AsyncBufReadExt::lines(bf);

                while let Some(l) = lines.next_line().await? {
                    let ent: RaftStoreEntry = serde_json::from_str(&l)
                        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                    let named_entry = (tree_name, ent);

                    let l = serde_json::to_string(&named_entry)
                        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

                    res.push(l);
                }
            }
        }

        Ok(res)
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.state_machine.read().await;
        let n = sm.nodes_ref().get(node_id).cloned();
        n
    }

    /// Return a list of nodes of the corresponding node-ids returned by `list_ids`.
    pub(crate) async fn get_nodes(
        &self,
        list_ids: impl Fn(&Membership) -> Vec<NodeId>,
    ) -> Vec<Node> {
        let sm = self.state_machine.read().await;
        let membership = sm.last_membership_ref().membership();

        debug!("in-statemachine membership: {:?}", membership);

        let ids = list_ids(membership);
        debug!("filtered node ids: {:?}", ids);
        let mut ns = vec![];

        for id in ids {
            let node = sm.nodes_ref().get(&id).cloned();
            if let Some(x) = node {
                ns.push(x);
            }
        }

        ns
    }

    pub async fn get_node_endpoint(&self, node_id: &NodeId) -> Result<Endpoint, MetaError> {
        let endpoint = self
            .get_node(node_id)
            .await
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
