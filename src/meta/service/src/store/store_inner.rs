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

use anyerror::AnyError;
use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::base::tokio::sync::RwLockWriteGuard;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStateKV;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::leveled_store::db::DBExporter;
use databend_common_meta_raft_store::log::RaftLog;
use databend_common_meta_raft_store::ondisk::TREE_HEADER;
use databend_common_meta_raft_store::sm_v002::SnapshotStoreV003;
use databend_common_meta_raft_store::sm_v002::WriteEntry;
use databend_common_meta_raft_store::sm_v002::SMV002;
use databend_common_meta_raft_store::state::RaftState;
use databend_common_meta_raft_store::state::RaftStateKey;
use databend_common_meta_raft_store::state::RaftStateValue;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_sled_store::get_sled_db;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Membership;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StorageIOError;
use databend_common_meta_types::Vote;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;

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
    ///     vote,      // the last `Vote`
    ///     committed, // last `LogId` that is known committed
    pub raft_state: RwLock<RaftState>,

    /// A series of raft logs.
    pub log: RwLock<RaftLog>,

    /// The Raft state machine.
    pub state_machine: Arc<RwLock<SMV002>>,
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
        info!(config_id :% =(&config.config_id); "open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        info!("RaftLog opened");

        fn to_startup_err(e: impl std::error::Error + 'static) -> MetaStartupError {
            let ae = AnyError::new(&e);
            let store_err = MetaStorageError::SnapshotError(ae);
            MetaStartupError::StoreOpenError(store_err)
        }

        let ss_store = SnapshotStoreV003::new(config.clone());
        let loader = ss_store.new_loader();
        let last = loader.load_last_snapshot().await.map_err(to_startup_err)?;

        let sm = if let Some((id, snapshot)) = last {
            let sm = Self::rebuild_state_machine(&id, snapshot)
                .await
                .map_err(to_startup_err)?;

            let db_opt = sm.get_snapshot();
            let snapshot_meta = db_opt.map(|x| x.snapshot_meta().clone());

            info!(
                "rebuilt state machine from last snapshot({:?}), meta: {:?}",
                id, snapshot_meta
            );

            sm
        } else {
            info!("No snapshot, skip rebuilding state machine");
            Default::default()
        };

        let store = Self {
            id: raft_state.id,
            config: config.clone(),
            is_opened: is_open,
            db,
            raft_state: RwLock::new(raft_state),
            log: RwLock::new(log),
            state_machine: Arc::new(RwLock::new(sm)),
        };

        Ok(store)
    }

    /// Return a snapshot store of this instance.
    pub fn snapshot_store(&self) -> SnapshotStoreV003 {
        SnapshotStoreV003::new(self.config.clone())
    }

    async fn rebuild_state_machine(id: &MetaSnapshotId, snapshot: DB) -> Result<SMV002, io::Error> {
        info!("rebuild state machine from last snapshot({:?})", id);

        let mut sm = SMV002::default();
        sm.install_snapshot_v003(snapshot).await?;

        Ok(sm)
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, SMV002> {
        self.state_machine.write().await
    }

    #[minitrace::trace]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, StorageError> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!(id = self.id; "do_build_snapshot start");

        let mut compactor = {
            let mut w = self.state_machine.write().await;
            w.freeze_writable();
            w.acquire_compactor().await
        };

        let (sys_data, mut strm) = compactor
            .compact()
            .await
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;

        let last_applied = *sys_data.last_applied_ref();
        let last_membership = sys_data.last_membership_ref().clone();
        let snapshot_id = MetaSnapshotId::new_with_epoch(last_applied);
        let snapshot_meta = SnapshotMeta {
            snapshot_id: snapshot_id.to_string(),
            last_log_id: last_applied,
            last_membership,
        };
        let signature = snapshot_meta.signature();

        let ss_store = self.snapshot_store();
        let writer = ss_store
            .new_writer()
            .map_err(|e| StorageIOError::write_snapshot(Some(signature.clone()), &e))?;

        let context = format!("build snapshot: {:?}", last_applied);
        let (tx, th) = writer.spawn_writer_thread(context);

        info!("do_build_snapshot writing snapshot start");

        // Pipe entries to the writer.
        {
            while let Some(ent) = strm
                .try_next()
                .await
                .map_err(|e| StorageIOError::read_snapshot(None, &e))?
            {
                tx.send(WriteEntry::Data(ent))
                    .await
                    .map_err(|e| StorageIOError::write_snapshot(Some(signature.clone()), &e))?;
            }

            tx.send(WriteEntry::Finish(sys_data))
                .await
                .map_err(|e| StorageIOError::write_snapshot(Some(signature.clone()), &e))?;
        }

        // Get snapshot write result
        let temp_snapshot_data = th
            .await
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?;

        let db = temp_snapshot_data
            .move_to_final_path(snapshot_id.to_string())
            .map_err(|e| {
                error!(error :% = e; "move temp snapshot to final path error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?;

        info!(
            snapshot_id :% = snapshot_id.to_string(),
            snapshot_file_size :% = db.file_size(),
            snapshot_stat :% = db.stat(); "do_build_snapshot complete");

        {
            let mut sm = self.state_machine.write().await;
            sm.levels_mut()
                .replace_with_compacted(compactor, db.clone());
        }

        // Clean old snapshot
        ss_store.new_loader().clean_old_snapshots().await?;
        info!("do_build_snapshot clean_old_snapshots complete");

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(db),
        })
    }

    /// Return snapshot id and meta of the last snapshot.
    ///
    /// It returns None if there is no snapshot or there is an error parsing snapshot meta or id.
    pub(crate) async fn try_get_snapshot_key_num(&self) -> Option<u64> {
        let sm = self.state_machine.read().await;
        let db = sm.levels().persisted()?;
        Some(db.stat().key_num)
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[minitrace::trace]
    pub async fn do_install_snapshot(&self, db: DB) -> Result<(), MetaStorageError> {
        let mut sm = self.state_machine.write().await;
        sm.install_snapshot_v003(db).await.map_err(|e| {
            MetaStorageError::SnapshotError(
                AnyError::new(&e).add_context(|| "replacing state-machine with snapshot"),
            )
        })?;

        Ok(())
    }

    /// Export all the data that can be used to restore a meta-service node.
    ///
    /// Returns a `BoxStream<'a, Result<String, io::Error>>` that yields a series of JSON strings.
    #[futures_async_stream::try_stream(boxed, ok = String, error = io::Error)]
    pub async fn export(self: Arc<StoreInner>) {
        // Convert an error occurred during export to `io::Error(InvalidData)`.
        fn invalid_data(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
            io::Error::new(ErrorKind::InvalidData, e)
        }

        // Lock all data components so that we have a consistent view.
        //
        // Hold the singleton compactor to prevent snapshot from being replaced until exporting finished.
        // Holding it prevent logs from being purged.
        //
        // Although vote and log must be consistent,
        // it is OK to export RaftState and logs without transaction protection(i.e. they do not share a lock),
        // if it guarantees no logs have a greater `vote` than `RaftState.HardState`.
        let compactor = {
            let mut sm = self.state_machine.write().await;
            sm.acquire_compactor().await
        };
        let raft_state = self.raft_state.read().await;
        let log = self.log.read().await;

        // Export data header first
        {
            let header_tree = SledTree::open(&self.db, TREE_HEADER, false).map_err(invalid_data)?;

            let header_kvs = header_tree.export()?;

            for kv in header_kvs.iter() {
                let line = vec_kv_to_json(TREE_HEADER, kv)?;
                yield line;
            }
        }

        // Export raft state
        {
            let tree_name = &raft_state.inner.name;

            let ks = raft_state.inner.key_space::<RaftStateKV>();

            let id = ks.get(&RaftStateKey::Id)?.map(NodeId::from);

            if let Some(id) = id {
                let ent_id = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::Id,
                    value: RaftStateValue::NodeId(id),
                };

                let s = serde_json::to_string(&(tree_name, ent_id)).map_err(invalid_data)?;
                yield s;
            }

            let vote = ks.get(&RaftStateKey::HardState)?.map(Vote::from);

            if let Some(vote) = vote {
                let ent_vote = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::HardState,
                    value: RaftStateValue::HardState(vote),
                };

                let s = serde_json::to_string(&(tree_name, ent_vote)).map_err(invalid_data)?;
                yield s;
            }

            let committed = ks
                .get(&RaftStateKey::Committed)?
                .and_then(Option::<LogId>::from);

            let ent_committed = RaftStoreEntry::RaftStateKV {
                key: RaftStateKey::Committed,
                value: RaftStateValue::Committed(committed),
            };

            let s = serde_json::to_string(&(tree_name, ent_committed)).map_err(invalid_data)?;
            yield s;
        };

        drop(raft_state);

        // Dump logs that has smaller or equal leader id as `vote`
        let log_tree_name = log.inner.name.clone();
        let log_kvs = log.inner.export()?;

        drop(log);

        // Dump snapshot of state machine

        // NOTE:
        // The name in form of "state_machine/[0-9]+" had been used by the sled tree based sm.
        // Do not change it for keeping compatibility.
        let sm_tree_name = "state_machine/0";
        let db = compactor.db().cloned();
        drop(compactor);

        for kv in log_kvs.iter() {
            let kv_entry = RaftStoreEntry::deserialize(&kv[0], &kv[1])?;

            let tree_kv = (&log_tree_name, kv_entry);
            let line = serde_json::to_string(&tree_kv).map_err(invalid_data)?;
            yield line;
        }

        if let Some(db) = db {
            let db_exporter = DBExporter::new(&db);
            let mut strm = db_exporter.export().await?;

            while let Some(ent) = strm.try_next().await? {
                let tree_kv = (sm_tree_name, ent);
                let line = serde_json::to_string(&tree_kv).map_err(invalid_data)?;
                yield line;
            }
        }
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.state_machine.read().await;
        let n = sm.sys_data_ref().nodes_ref().get(node_id).cloned();
        n
    }

    /// Return a list of nodes of the corresponding node-ids returned by `list_ids`.
    pub(crate) async fn get_nodes(
        &self,
        list_ids: impl Fn(&Membership) -> Vec<NodeId>,
    ) -> Vec<Node> {
        let sm = self.state_machine.read().await;
        let membership = sm.sys_data_ref().last_membership_ref().membership();

        debug!("in-statemachine membership: {:?}", membership);

        let ids = list_ids(membership);
        debug!("filtered node ids: {:?}", ids);
        let mut ns = vec![];

        for id in ids {
            let node = sm.sys_data_ref().nodes_ref().get(&id).cloned();
            if let Some(x) = node {
                ns.push(x);
            }
        }

        ns
    }

    pub async fn get_node_raft_endpoint(&self, node_id: &NodeId) -> Result<Endpoint, MetaError> {
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
