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
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::leveled_store::db_exporter::DBExporter;
use databend_common_meta_raft_store::ondisk::Header;
use databend_common_meta_raft_store::ondisk::TREE_HEADER;
use databend_common_meta_raft_store::raft_log_v004;
use databend_common_meta_raft_store::raft_log_v004::util;
use databend_common_meta_raft_store::raft_log_v004::Cw;
use databend_common_meta_raft_store::raft_log_v004::RaftLogV004;
use databend_common_meta_raft_store::sm_v003::compactor_acquirer::CompactorAcquirer;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV004;
use databend_common_meta_raft_store::sm_v003::SMV003;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::raft_types::Entry;
use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::NodeId;
use databend_common_meta_types::snapshot_db::DBStat;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Node;
use futures::TryStreamExt;
use log::debug;
use log::info;
use raft_log::api::raft_log_writer::RaftLogWriter;

use crate::store::meta_raft_log::MetaRaftLog;
use crate::store::meta_raft_state_machine::MetaRaftStateMachine;

/// A store that contains raft-log store and state-machine.
///
/// It is designed to be cloneable in order to be shared by MetaNode and Raft.
#[derive(Clone)]
pub struct RaftStore {
    pub id: NodeId,

    pub(crate) config: Arc<RaftConfig>,

    log: MetaRaftLog,

    state_machine: MetaRaftStateMachine,
}

impl RaftStore {
    /// Open an existent raft-store or create a new one.
    #[fastrace::trace]
    pub async fn open(config: &RaftConfig) -> Result<Self, MetaStartupError> {
        let config = Arc::new(config.clone());

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

        let is_opened = stored_node_id.is_some();

        // If id is stored, ignore the id in config.
        let id = stored_node_id.unwrap_or(config.id);

        if !is_opened {
            log.save_user_data(Some(raft_log_v004::log_store_meta::LogStoreMeta {
                node_id: Some(config.id),
            }))
            .map_err(to_startup_err)?;

            util::blocking_flush(&mut log)
                .await
                .map_err(to_startup_err)?;
        }

        let ss_store = SnapshotStoreV004::new(config.as_ref().clone());
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

        info!("State machine built: {:?}", sm);

        let mut store = Self {
            id,
            config: config.clone(),
            log: MetaRaftLog::new(id, config.clone(), log),
            state_machine: MetaRaftStateMachine::new(id, config, Arc::new(sm)),
        };

        {
            let interval_ms = store.config.compact_immutables_ms.unwrap_or(1000);

            if interval_ms == 0 {
                info!("spawn in-memory compactor, is disabled: interval_ms: 0");
            } else {
                let interval = Duration::from_millis(interval_ms);
                info!("spawn in-memory compactor, interval: {:?}", interval);
                store.state_machine.spawn_in_memory_compactor(interval);
            }
        }

        Ok(store)
    }

    pub fn log(&self) -> &MetaRaftLog {
        &self.log
    }

    pub fn state_machine(&self) -> &MetaRaftStateMachine {
        &self.state_machine
    }

    pub fn get_sm_v003(&self) -> Arc<SMV003> {
        self.state_machine.get_inner()
    }

    async fn rebuild_state_machine(id: &MetaSnapshotId, snapshot: DB) -> Result<SMV003, io::Error> {
        info!("rebuild state machine from last snapshot({:?})", id);

        let sm = SMV003::default();
        let sm = sm.install_snapshot_v003(snapshot);

        Ok(sm)
    }

    /// Return snapshot id and meta of the last snapshot.
    ///
    /// It returns None if there is no snapshot or there is an error parsing snapshot meta or id.
    pub(crate) async fn try_get_snapshot_key_count(&self) -> Option<u64> {
        let sm = self.get_sm_v003();
        let db = sm.leveled_map().persisted()?;
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
        let sm = self.get_sm_v003();
        let Some(db) = sm.leveled_map().persisted() else {
            return Default::default();
        };
        db.sys_data().key_counts().clone()
    }

    /// Get the statistics of the snapshot database.
    pub(crate) async fn get_snapshot_db_stat(&self) -> DBStat {
        let sm = self.get_sm_v003();
        let Some(db) = sm.leveled_map().persisted() else {
            return Default::default();
        };
        db.db_stat()
    }

    /// Export all the data that can be used to restore a meta-service node.
    ///
    /// Returns a `BoxStream<Result<String, io::Error>>` that yields a series of JSON strings.
    #[futures_async_stream::try_stream(boxed, ok = String, error = io::Error)]
    pub async fn export(self) {
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

        let permit = self.new_compactor_acquirer("export").acquire().await;

        let mut dump = {
            let log = self.log().read().await;
            log.dump_data()
        };

        // Log is dumped thus there won't be a gap between sm and log.
        // It is now safe to release the compactor.
        let db = self.get_sm_v003().get_snapshot();
        drop(permit);

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

    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.get_sm_v003();
        let n = sm.sys_data().nodes_ref().get(node_id).cloned();
        n
    }

    /// Return a list of nodes of the corresponding node-ids returned by `list_ids`.
    pub(crate) async fn get_nodes(
        &self,
        list_ids: impl Fn(&Membership) -> Vec<NodeId>,
    ) -> Vec<Node> {
        let sm = self.get_sm_v003();
        let membership = sm.sys_data().last_membership_ref().membership().clone();

        debug!("in-statemachine membership: {:?}", membership);

        let ids = list_ids(&membership);
        debug!("filtered node ids: {:?}", ids);
        let mut ns = vec![];

        for id in ids {
            let node = sm.sys_data().nodes_ref().get(&id).cloned();
            if let Some(x) = node {
                ns.push(x);
            }
        }

        ns
    }
    fn new_compactor_acquirer(&self, name: impl ToString) -> CompactorAcquirer {
        let sm = self.get_sm_v003();
        sm.new_compactor_acquirer(name)
    }
}
