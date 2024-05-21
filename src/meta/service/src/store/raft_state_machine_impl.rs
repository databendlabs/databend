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

use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_raft_store::sm_v002::leveled_store::sys_data_api::SysDataApiRO;
use databend_common_meta_raft_store::sm_v002::SnapshotStoreV002;
use databend_common_meta_sled_store::openraft::storage::RaftStateMachine;
use databend_common_meta_sled_store::openraft::ErrorVerb;
use databend_common_meta_sled_store::openraft::OptionalSend;
use databend_common_meta_sled_store::openraft::RaftSnapshotBuilder;
use databend_common_meta_types::AppliedState;
use databend_common_meta_types::Entry;
use databend_common_meta_types::ErrorSubject;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StoredMembership;
use databend_common_meta_types::TypeConfig;
use log::debug;
use log::error;
use log::info;

use crate::metrics::raft_metrics;
use crate::metrics::server_metrics;
use crate::store::RaftStore;

impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    #[minitrace::trace]
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

    #[minitrace::trace]
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

        self.set_snapshot(Some(meta.clone())).await;

        Ok(())
    }

    #[minitrace::trace]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<databend_common_meta_sled_store::openraft::Snapshot<TypeConfig>>, StorageError>
    {
        info!(id = self.id; "get snapshot start");
        let p = self.current_snapshot.read().await;

        let snap = match &*p {
            Some(meta) => {
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
}
