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

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::base::tokio::sync::RwLockWriteGuard;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::leveled_store::db_exporter::DBExporter;
use databend_common_meta_raft_store::ondisk::Header;
use databend_common_meta_raft_store::ondisk::TREE_HEADER;
use databend_common_meta_raft_store::raft_log_v004;
use databend_common_meta_raft_store::raft_log_v004::util;
use databend_common_meta_raft_store::raft_log_v004::Cw;
use databend_common_meta_raft_store::raft_log_v004::RaftLogV004;
use databend_common_meta_raft_store::sm_v003::write_entry::WriteEntry;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_common_meta_raft_store::sm_v003::SMV003;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::node::Node;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::raft_types::Snapshot;
use databend_common_meta_types::raft_types::SnapshotMeta;
use databend_common_meta_types::raft_types::StorageError;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::MetaStartupError;
use databend_common_metrics::count::Count;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use raft_log::api::raft_log_writer::RaftLogWriter;
use tokio::time::sleep;

use crate::metrics::raft_metrics;
use crate::metrics::SnapshotBuilding;

/// This is the inner store that implements the raft log storage API.
pub struct RaftStoreInner {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft-log.
    ///
    /// `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    pub(crate) config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from fs) or created.
    pub is_opened: bool,

    /// A series of raft logs.
    pub log: Arc<RwLock<RaftLogV004>>,

    /// The Raft state machine.
    pub state_machine: Arc<RwLock<SMV003>>,
}

impl AsRef<RaftStoreInner> for RaftStoreInner {
    fn as_ref(&self) -> &RaftStoreInner {
        self
    }
}

impl RaftStoreInner {
    /// Open an existent raft-store or create a new one.
    #[fastrace::trace]
    pub async fn open(config: &RaftConfig) -> Result<RaftStoreInner, MetaStartupError> {
        info!("open_or_create StoreInner: id={}", config.id);

        fn to_startup_err(e: impl std::error::Error + 'static) -> MetaStartupError {
            let ae = AnyError::new(&e);
            let store_err = MetaStorageError(ae);
            MetaStartupError::StoreOpenError(store_err)
        }

        let raft_log_config = Arc::new(config.to_raft_log_config());

        let dir = &raft_log_config.dir;

        fs::create_dir_all(dir).map_err(|e| {
            let err = io::Error::new(
                e.kind(),
                format!("{}; when:(create raft log dir: {}", e, dir),
            );
            to_startup_err(err)
        })?;

        let mut log = RaftLogV004::open(raft_log_config.clone()).map_err(to_startup_err)?;
        info!("RaftLog opened at: {}", raft_log_config.dir);

        let state = log.log_state();
        info!("log_state: {:?}", state);
        let stored_node_id = state.user_data.as_ref().and_then(|x| x.node_id);

        let is_open = stored_node_id.is_some();

        // If id is stored, ignore the id in config.
        let id = stored_node_id.unwrap_or(config.id);

        if !is_open {
            log.save_user_data(Some(raft_log_v004::log_store_meta::LogStoreMeta {
                node_id: Some(config.id),
            }))
            .map_err(to_startup_err)?;

            util::blocking_flush(&mut log)
                .await
                .map_err(to_startup_err)?;
        }

        let ss_store = SnapshotStoreV004::new(config.clone());
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
            id,
            config: config.clone(),
            is_opened: is_open,
            log: Arc::new(RwLock::new(log)),
            state_machine: Arc::new(RwLock::new(sm)),
        };

        Ok(store)
    }

    /// Return a snapshot store of this instance.
    pub fn snapshot_store(&self) -> SnapshotStoreV004 {
        SnapshotStoreV004::new(self.config.clone())
    }

    async fn rebuild_state_machine(id: &MetaSnapshotId, snapshot: DB) -> Result<SMV003, io::Error> {
        info!("rebuild state machine from last snapshot({:?})", id);

        let mut sm = SMV003::default();
        sm.install_snapshot_v003(snapshot).await?;

        Ok(sm)
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, SMV003> {
        self.state_machine.write().await
    }

    #[fastrace::trace]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, StorageError> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!(id = self.id; "do_build_snapshot start");

        let _guard = SnapshotBuilding::guard();

        let mut compactor = {
            let mut w = self.state_machine.write().await;
            w.freeze_writable();
            w.acquire_compactor().await
        };

        let (mut sys_data, mut strm) = compactor
            .compact()
            .await
            .map_err(|e| StorageError::read_snapshot(None, &e))?;

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
            .map_err(|e| StorageError::write_snapshot(Some(signature.clone()), &e))?;

        let context = format!("build snapshot: {:?}", last_applied);
        let (tx, th) = writer.spawn_writer_thread(context);

        info!("do_build_snapshot writing snapshot start");

        // Pipe entries to the writer.
        {
            while let Some(ent) = strm
                .try_next()
                .await
                .map_err(|e| StorageError::read_snapshot(None, &e))?
            {
                // The first 4 chars are key space, such as: "kv--/" or "exp-/"
                // Get the first 4 chars as key space.
                let prefix = &ent.0.as_str()[..4];
                let ks = sys_data.key_counts_mut();
                if let Some(count) = ks.get_mut(prefix) {
                    *count += 1;
                } else {
                    ks.insert(prefix.to_string(), 1);
                }

                tx.send(WriteEntry::Data(ent))
                    .await
                    .map_err(|e| StorageError::write_snapshot(Some(signature.clone()), &e))?;

                raft_metrics::storage::incr_snapshot_written_entries();
            }

            tx.send(WriteEntry::Finish((snapshot_id.clone(), sys_data)))
                .await
                .map_err(|e| StorageError::write_snapshot(Some(signature.clone()), &e))?;
        }

        // Get snapshot write result
        let db = th
            .await
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageError::write_snapshot(Some(signature.clone()), &e)
            })?
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageError::write_snapshot(Some(signature.clone()), &e)
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
            snapshot: db,
        })
    }

    /// Return snapshot id and meta of the last snapshot.
    ///
    /// It returns None if there is no snapshot or there is an error parsing snapshot meta or id.
    pub(crate) async fn try_get_snapshot_key_count(&self) -> Option<u64> {
        let sm = self.state_machine.read().await;
        let db = sm.levels().persisted()?;
        Some(db.stat().key_num)
    }

    /// Returns the count of keys in each key space from the snapshot data.
    ///
    /// Key spaces include:
    /// - `"exp-"`: expire index data
    /// - `"kv--"`: key-value data
    ///
    /// Returns an empty map if no snapshot exists.
    pub(crate) async fn get_snapshot_key_space_stat(&self) -> BTreeMap<String, u64> {
        let sm = self.state_machine.read().await;
        let Some(db) = sm.levels().persisted() else {
            return Default::default();
        };
        db.sys_data().key_counts().clone()
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[fastrace::trace]
    pub async fn do_install_snapshot(&self, db: DB) -> Result<(), MetaStorageError> {
        let mut sm = self.state_machine.write().await;
        sm.install_snapshot_v003(db).await.map_err(|e| {
            MetaStorageError(
                AnyError::new(&e).add_context(|| "replacing state-machine with snapshot"),
            )
        })?;

        Ok(())
    }

    /// Export all the data that can be used to restore a meta-service node.
    ///
    /// Returns a `BoxStream<'a, Result<String, io::Error>>` that yields a series of JSON strings.
    #[futures_async_stream::try_stream(boxed, ok = String, error = io::Error)]
    pub async fn export(self: Arc<RaftStoreInner>) {
        info!("StoreInner::export start");

        // Convert an error occurred during export to `io::Error(InvalidData)`.
        fn invalid_data(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
            io::Error::new(ErrorKind::InvalidData, e)
        }

        fn encode_entry(tree_name: &str, ent: &RaftStoreEntry) -> Result<String, io::Error> {
            let name_entry = (tree_name, ent);

            let line = serde_json::to_string(&name_entry)
                .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
            Ok(line)
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
            // If there is a compactor running,
            // and it will be installed back to SM with
            // `self.state_machine.write().await.levels_mut().replace_with_compacted()`.
            // This compactor can not block with self.state_machine lock held.
            // Otherwise, there is a deadlock:
            // - This thread holds self.state_machine lock, acquiring compactor.
            // - The other thread holds compactor, acquiring self.state_machine lock.
            loop {
                let got = {
                    let mut sm = self.state_machine.write().await;
                    sm.try_acquire_compactor()
                };
                if let Some(c) = got {
                    break c;
                }
                sleep(Duration::from_millis(10)).await;
            }
        };

        let mut dump = {
            let log = self.log.read().await;
            log.dump_data()
        };

        // Log is dumped thus there won't be a gap between sm and log.
        // It is now safe to release the compactor.
        let db = compactor.db().cloned();
        drop(compactor);

        // Export data header first
        {
            let entry = RaftStoreEntry::new_header(Header::this_version());
            yield encode_entry(TREE_HEADER, &entry)?;
        }

        let state = dump.state();

        // Export raft state
        {
            let tree_name = "raft_log";

            let node_id = state.user_data.as_ref().and_then(|ud| ud.node_id);
            let entry = RaftStoreEntry::NodeId(node_id);
            yield encode_entry(tree_name, &entry)?;

            let vote = state.vote().map(Cw::to_inner);
            let entry = RaftStoreEntry::Vote(vote);
            yield encode_entry(tree_name, &entry)?;

            let committed = state.committed().map(Cw::to_inner);
            let entry = RaftStoreEntry::Committed(committed);
            yield encode_entry(tree_name, &entry)?;
        };

        {
            let tree_name = "raft_log";

            let purged = state.purged().map(Cw::to_inner);
            let entry = RaftStoreEntry::Purged(purged);
            yield encode_entry(tree_name, &entry)?;

            for res in dump.iter() {
                let (log_id, payload) = res?;
                let log_id = log_id.unpack();
                let payload = payload.unpack();

                let log_entry = Entry { log_id, payload };

                let entry = RaftStoreEntry::LogEntry(log_entry);
                yield encode_entry(tree_name, &entry)?;
            }
        }

        // Dump snapshot of state machine

        // NOTE:
        // The name in form of "state_machine/[0-9]+" had been used by the sled tree based sm.
        // Do not change it for keeping compatibility.
        let sm_tree_name = "state_machine/0";

        info!("StoreInner::export db: {:?}", db);

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

    pub async fn get_node_raft_endpoint(
        &self,
        node_id: &NodeId,
    ) -> Result<Endpoint, MetaNetworkError> {
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
