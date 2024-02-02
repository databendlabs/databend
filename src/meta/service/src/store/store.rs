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

use std::fmt::Debug;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_raft_store::sm_v002::leveled_store::sys_data_api::SysDataApiRO;
use databend_common_meta_raft_store::sm_v002::SnapshotStoreV002;
use databend_common_meta_raft_store::state_machine::StoredSnapshot;
use databend_common_meta_sled_store::openraft::ErrorSubject;
use databend_common_meta_sled_store::openraft::ErrorVerb;
use databend_common_meta_sled_store::openraft::LogState;
use databend_common_meta_sled_store::openraft::RaftLogReader;
use databend_common_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_common_meta_sled_store::openraft::RaftStorage;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use log::as_debug;
use log::debug;
use log::error;
use log::info;

use crate::metrics::raft_metrics;
use crate::metrics::server_metrics;
use crate::store::StoreInner;
use crate::store::ToStorageError;

/// A store that implements `RaftStorage` trait and provides full functions.
///
/// It is designed to be cloneable in order to be shared by MetaNode and Raft.
#[derive(Clone)]
pub struct RaftStore {
    pub(crate) inner: Arc<StoreInner>,
}

impl RaftStore {
    pub fn new(sto: StoreInner) -> Self {
        Self {
            inner: Arc::new(sto),
        }
    }

    #[minitrace::trace]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<Self, MetaStartupError> {
        let sto = StoreInner::open_create(config, open, create).await?;
        Ok(Self::new(sto))
    }

    pub fn inner(&self) -> Arc<StoreInner> {
        self.inner.clone()
    }
}

impl Deref for RaftStore {
    type Target = StoreInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RaftLogReader<TypeConfig> for RaftStore {
    #[minitrace::trace]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        debug!(
            "try_get_log_entries: self.id={}, range: {:?}",
            self.id, range
        );

        match self
            .log
            .read()
            .await
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Ok(entries) => Ok(entries),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("try_get_log_entries", false);
                Err(err)
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    #[minitrace::trace]
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        self.do_build_snapshot().await
    }
}

impl RaftStorage<TypeConfig> for RaftStore {
    type LogReader = RaftStore;
    type SnapshotBuilder = RaftStore;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.raft_state
            .write()
            .await
            .save_committed(committed)
            .await
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Write)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        self.raft_state
            .read()
            .await
            .read_committed()
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Read)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged_log_id = match self
            .log
            .read()
            .await
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last = match self
            .log
            .read()
            .await
            .logs()
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x.1.log_id),
        };

        debug!(
            "get_log_state: ({:?},{:?}]",
            last_purged_log_id, last_log_id
        );

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    #[minitrace::trace]
    async fn save_vote(&mut self, hs: &Vote) -> Result<(), StorageError> {
        info!(id = self.id, hs = as_debug!(hs); "save_vote");

        match self
            .raft_state
            .write()
            .await
            .save_vote(hs)
            .await
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Write)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("save_vote", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        }
    }

    #[minitrace::trace]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!(id = self.id; "delete_conflict_logs_since: {}", log_id);

        match self
            .log
            .write()
            .await
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            Ok(_) => Ok(()),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("delete_conflict_logs_since", true);
                Err(err)
            }
        }
    }

    #[minitrace::trace]
    async fn purge_logs_upto(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!(id = self.id, log_id = as_debug!(&log_id); "purge_logs_upto: start");

        if let Err(err) = self
            .log
            .write()
            .await
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        };

        info!(id = self.id, log_id = as_debug!(&log_id); "purge_logs_upto: Done: set_last_purged()");

        let log = self.log.write().await.clone();

        // Purge can be done in another task safely, because:
        //
        // - Next time when raft starts, it will read last_purged_log_id without examining the actual first log.
        //   And junk can be removed next time purge_logs_upto() is called.
        //
        // - Purging operates the start of the logs, and only committed logs are purged;
        //   while append and truncate operates on the end of the logs,
        //   it is safe to run purge && (append || truncate) concurrently.
        databend_common_base::runtime::spawn({
            let id = self.id;
            async move {
                info!(id = id, log_id = as_debug!(&log_id); "purge_logs_upto: Start: asynchronous range_remove()");

                let res = log.range_remove(..=log_id.index).await;

                if let Err(err) = res {
                    error!(id = id, log_id = as_debug!(&log_id); "purge_logs_upto: in asynchronous error: {}", err);
                    raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
                }

                info!(id = id, log_id = as_debug!(&log_id); "purge_logs_upto: Done: asynchronous range_remove()");
            }
        });

        Ok(())
    }

    #[minitrace::trace]
    async fn append_to_log<I: IntoIterator<Item = Entry> + Send>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError> {
        // TODO: it is bad: allocates a new vec.
        let entries = entries
            .into_iter()
            .map(|x| {
                info!("append_to_log: {}", x.log_id);
                x
            })
            .collect::<Vec<_>>();

        match self
            .log
            .write()
            .await
            .append(entries)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("append_to_log", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        }
    }

    #[minitrace::trace]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry],
    ) -> Result<Vec<AppliedState>, StorageError> {
        for ent in entries {
            info!("apply_to_state_machine: {}", ent.log_id);
        }

        let mut sm = self.state_machine.write().await;
        let res = sm.apply_entries(entries).await?;

        Ok(res)
    }

    #[minitrace::trace]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotData>, StorageError> {
        server_metrics::incr_applying_snapshot(1);

        let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, self.inner.config.clone());

        let temp = snapshot_store.new_temp().await.map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
        })?;

        Ok(Box::new(temp))
    }

    #[minitrace::trace]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError> {
        let data_size = snapshot.data_size().await.map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                e,
            )
        })?;

        info!(
            id = self.id,
            snapshot_size = data_size;
            "decoding snapshot for installation"
        );
        server_metrics::incr_applying_snapshot(-1);

        assert!(snapshot.is_temp());

        let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, self.inner.config.clone());

        let d = snapshot_store
            .commit_received(snapshot, meta)
            .await
            .map_err(|e| {
                e.with_context(format_args!(
                    "commit received snapshot: {:?}",
                    meta.signature()
                ))
            })?;

        let d = Box::new(d);

        info!("snapshot meta: {:?}", meta);

        // Replace state machine with the new one
        let res = self.do_install_snapshot(d).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                raft_metrics::storage::incr_raft_storage_fail("install_snapshot", true);
                error!("error: {:?} when install_snapshot", e);
            }
        };

        // Update current snapshot.
        let new_snapshot = StoredSnapshot { meta: meta.clone() };
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(())
    }

    #[minitrace::trace]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        info!(id = self.id; "get snapshot start");
        let p = self.current_snapshot.read().await;

        let snap = match &*p {
            Some(snapshot) => {
                let meta = &snapshot.meta;

                let snapshot_store =
                    SnapshotStoreV002::new(DATA_VERSION, self.inner.config.clone());
                let d = snapshot_store
                    .load_snapshot(&meta.snapshot_id)
                    .await
                    .map_err(|e| e.with_meta("get snapshot", meta))?;

                Ok(Some(Snapshot {
                    meta: meta.clone(),
                    snapshot: Box::new(d),
                }))
            }
            None => Ok(None),
        };

        info!("get snapshot complete");

        snap
    }

    #[minitrace::trace]
    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        match self
            .raft_state
            .read()
            .await
            .read_vote()
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("read_vote", false);
                Err(err)
            }
            Ok(vote) => Ok(vote),
        }
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let sm = self.state_machine.read().await;
        let last_applied = *sm.sys_data_ref().last_applied_ref();
        let last_membership = sm.sys_data_ref().last_membership_ref().clone();

        debug!(
            "last_applied_state: applied: {:?}, membership: {:?}",
            last_applied, last_membership
        );

        Ok((last_applied, last_membership))
    }
}
