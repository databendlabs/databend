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

use databend_common_meta_raft_store::sm_v003::open_snapshot::OpenSnapshot;
use databend_common_meta_raft_store::sm_v003::SnapshotStoreV003;
use databend_common_meta_sled_store::openraft::storage::RaftStateMachine;
use databend_common_meta_sled_store::openraft::OptionalSend;
use databend_common_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::TypeConfig;
use log::debug;
use log::error;
use log::info;

use crate::metrics::raft_metrics;
use crate::store::RaftStore;

impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    #[fastrace::trace]
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        self.do_build_snapshot().await
    }
}

impl RaftStateMachine<TypeConfig> for RaftStore {
    type SnapshotBuilder = RaftStore;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let sm = self.state_machine.read().await;
        let last_applied = *sm.sys_data_ref().last_applied_ref();
        let last_membership = sm.sys_data_ref().last_membership_ref().clone();

        debug!(
            "applied_state: applied: {:?}, membership: {:?}",
            last_applied, last_membership
        );

        Ok((last_applied, last_membership))
    }

    #[fastrace::trace]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppliedState>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut sm = self.state_machine.write().await;
        let res = sm.apply_entries(entries).await?;

        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    // This method is not used
    #[fastrace::trace]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<DB>, StorageError> {
        let ss_store = SnapshotStoreV003::new(self.inner.config.clone());
        let db = ss_store
            .new_temp()
            .map_err(|e| StorageError::write_snapshot(None, &e))?;
        Ok(Box::new(db))
    }

    #[fastrace::trace]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<DB>,
    ) -> Result<(), StorageError> {
        let data_size = snapshot.file_size();

        info!(
            id = self.id,
            snapshot_size = data_size;
            "decoding snapshot for installation"
        );

        let sig = meta.signature();

        let ss_store = SnapshotStoreV003::new(self.inner.config.clone());
        let final_path = ss_store
            .snapshot_config()
            .move_to_final_path(&snapshot.path, meta.snapshot_id.clone())
            .map_err(|e| StorageError::write_snapshot(Some(sig.clone()), &e))?;

        let db = DB::open_snapshot(final_path, meta.snapshot_id.clone(), &self.inner.config)
            .map_err(|e| StorageError::read_snapshot(Some(sig.clone()), &e))?;

        info!("snapshot meta: {:?}", meta);

        // Replace state machine with the new one
        let res = self.do_install_snapshot(db).await;
        match res {
            Ok(_) => {}
            Err(e) => {
                raft_metrics::storage::incr_raft_storage_fail("install_snapshot", true);
                error!("error: {:?} when install_snapshot", e);
            }
        };

        Ok(())
    }

    #[fastrace::trace]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        info!(id = self.id; "get snapshot start");

        let r = self.state_machine.read().await;
        let db = r.levels().persisted().cloned();

        let snapshot = db.map(|x| Snapshot {
            meta: x.snapshot_meta().clone(),
            snapshot: Box::new(x),
        });

        info!(
            "get snapshot complete: {:?}",
            snapshot.as_ref().map(|x| &x.meta)
        );
        Ok(snapshot)
    }
}
