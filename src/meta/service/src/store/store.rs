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
use std::io::Cursor;
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::state_machine::StoredSnapshot;
use common_meta_sled_store::openraft::ErrorSubject;
use common_meta_sled_store::openraft::ErrorVerb;
use common_meta_sled_store::openraft::LogState;
use common_meta_sled_store::openraft::RaftLogReader;
use common_meta_sled_store::openraft::RaftSnapshotBuilder;
use common_meta_sled_store::openraft::RaftStorage;
use common_meta_types::AppliedState;
use common_meta_types::Entry;
use common_meta_types::LogId;
use common_meta_types::MetaStartupError;
use common_meta_types::Snapshot;
use common_meta_types::SnapshotData;
use common_meta_types::SnapshotMeta;
use common_meta_types::StorageError;
use common_meta_types::StoredMembership;
use common_meta_types::TypeConfig;
use common_meta_types::Vote;
use tracing::debug;
use tracing::error;
use tracing::info;

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

    #[tracing::instrument(level = "debug", skip_all, fields(config_id=%config.config_id))]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<Self, MetaStartupError> {
        let sto = StoreInner::open_create(config, open, create).await?;
        Ok(Self::new(sto))
    }
}

impl Deref for RaftStore {
    type Target = StoreInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl RaftLogReader<TypeConfig> for RaftStore {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged_log_id = match self
            .log
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

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        debug!("try_get_log_entries: range: {:?}", range);

        match self
            .log
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Ok(entries) => return Ok(entries),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("try_get_log_entries", false);
                Err(err)
            }
        }
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        self.do_build_snapshot().await
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for RaftStore {
    type LogReader = RaftStore;
    type SnapshotBuilder = RaftStore;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "debug", skip(self, hs), fields(id=self.id))]
    async fn save_vote(&mut self, hs: &Vote) -> Result<(), StorageError> {
        info!("save_vote: {:?}", hs);

        match self
            .raft_state
            .save_vote(hs)
            .await
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Write)
        {
            Err(err) => {
                return {
                    raft_metrics::storage::incr_raft_storage_fail("save_vote", true);
                    Err(err)
                };
            }
            Ok(_) => return Ok(()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!("delete_conflict_logs_since: {}", log_id);

        match self
            .log
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("delete_conflict_logs_since", true);
                Err(err)
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn purge_logs_upto(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!("purge_logs_upto: {}", log_id);

        if let Err(err) = self
            .log
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        };
        if let Err(err) = self
            .log
            .range_remove(..=log_id.index)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
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
            .append(entries)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("append_to_log", true);
                Err(err)
            }
            Ok(_) => return Ok(()),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, entries), fields(id=self.id))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry],
    ) -> Result<Vec<AppliedState>, StorageError> {
        for ent in entries {
            info!("apply_to_state_machine: {}", ent.log_id);
        }

        let mut res = Vec::with_capacity(entries.len());

        let sm = self.state_machine.write().await;
        for entry in entries {
            let r = match sm
                .apply(entry)
                .await
                .map_to_sto_err(ErrorSubject::Apply(entry.log_id), ErrorVerb::Write)
            {
                Err(err) => {
                    raft_metrics::storage::incr_raft_storage_fail("apply_to_state_machine", true);
                    return Err(err);
                }
                Ok(r) => r,
            };
            res.push(r);
        }
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotData>, StorageError> {
        server_metrics::incr_applying_snapshot(1);
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "debug", skip(self, snapshot), fields(id=self.id))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError> {
        // TODO(xp): disallow installing a snapshot with smaller last_applied.

        info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
        server_metrics::incr_applying_snapshot(-1);

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        info!("snapshot meta: {:?}", meta);

        // Replace state machine with the new one
        let res = self.do_install_snapshot(&new_snapshot.data).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                raft_metrics::storage::incr_raft_storage_fail("install_snapshot", true);
                error!("error: {:?} when install_snapshot", e);
            }
        };

        // Update current snapshot.
        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self), fields(id=self.id))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        info!("get snapshot start");
        let snap = match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        };

        info!("get snapshot complete");

        snap
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        match self
            .raft_state
            .read_vote()
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("read_vote", false);
                return Err(err);
            }
            Ok(vote) => return Ok(vote),
        }
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let sm = self.state_machine.read().await;
        let last_applied = match sm
            .get_last_applied()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("last_applied_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };
        let last_membership = match sm
            .get_membership()
            .map_to_sto_err(ErrorSubject::StateMachine, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("last_applied_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        debug!(
            "last_applied_state: applied: {:?}, membership: {:?}",
            last_applied, last_membership
        );

        let last_membership = last_membership.unwrap_or_default();

        Ok((last_applied, last_membership))
    }
}
