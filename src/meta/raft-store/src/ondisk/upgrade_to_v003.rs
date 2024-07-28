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

//! Provide upgrading to v003 and cleaning v002

use std::fs;
use std::io;

use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::SnapshotData;
use openraft::AnyError;

use crate::ondisk::DataVersion;
use crate::ondisk::OnDisk;
use crate::sm_v003::adapter::upgrade_snapshot_data_v002_to_v003;
use crate::sm_v003::SnapshotStoreV002;
use crate::sm_v003::SnapshotStoreV003;
use crate::state_machine::MetaSnapshotId;

impl OnDisk {
    /// Upgrade the on-disk data form [`DataVersion::V002`] to [`DataVersion::V003`].
    ///
    /// `V002` saves snapshot in a file instead of in sled db.
    /// `V003` saves snapshot in a `rotbl` file and uses rotbl to store the state machine.
    ///
    /// Upgrade will be skipped if:
    /// - there is no V002 snapshot files.
    ///
    /// Steps:
    /// - Build a V003 snapshot from V002 snapshot.
    /// - Remove the V002 snapshot.
    #[minitrace::trace]
    pub(crate) async fn upgrade_v002_to_v003(&mut self) -> Result<(), MetaStorageError> {
        self.begin_upgrading(DataVersion::V002).await?;

        let ss_store_v002 = SnapshotStoreV002::new(self.config.clone());

        let loaded = ss_store_v002.load_last_snapshot().await?;

        let Some((snapshot_id, snapshot_data)) = loaded else {
            self.progress(format_args!("No V002 snapshot, skip upgrade"));
            self.finish_upgrading().await?;
            return Ok(());
        };

        self.convert_snapshot_v002_to_v003(snapshot_id.clone(), snapshot_data)
            .await
            .map_err(|e| {
                MetaStorageError::snapshot_error(&e, || {
                    format!("convert v002 snapshot to v003 {}", snapshot_id)
                })
            })?;

        self.remove_v002_snapshot().await?;

        self.finish_upgrading().await?;

        Ok(())
    }

    /// Revert or finish the unfinished upgrade to v003.
    pub(crate) async fn clean_in_progress_v002_to_v003(&mut self) -> Result<(), MetaStorageError> {
        let snapshot_store = SnapshotStoreV003::new(self.config.clone());
        let loader = snapshot_store.new_loader();

        let last_snapshot = loader.load_last_snapshot().await.map_err(|e| {
            let ae = AnyError::new(&e).add_context(|| "load last snapshot");
            MetaStorageError::SnapshotError(ae)
        })?;

        if last_snapshot.is_some() {
            self.progress(format_args!(
                "There is V003 snapshot, upgrade is done; Finish upgrading"
            ));

            self.remove_v002_snapshot().await?;

            // Note that this will increase `header.version`.
            self.finish_upgrading().await?;
        }

        Ok(())
    }

    pub(crate) async fn convert_snapshot_v002_to_v003(
        &mut self,
        snapshot_id: MetaSnapshotId,
        snapshot_data: SnapshotData,
    ) -> Result<(), io::Error> {
        let ss_store_v003 = SnapshotStoreV003::new(self.config.clone());

        upgrade_snapshot_data_v002_to_v003(
            &ss_store_v003,
            Box::new(snapshot_data),
            snapshot_id.to_string(),
        )
        .await?;
        Ok(())
    }

    async fn remove_v002_snapshot(&mut self) -> Result<(), MetaStorageError> {
        let ss_store_v002 = SnapshotStoreV002::new(self.config.clone());
        let snapshot_config = ss_store_v002.snapshot_config();
        let dir = snapshot_config.snapshot_dir();

        fs::remove_dir_all(&dir).map_err(|e| {
            MetaStorageError::snapshot_error(&e, || format!("removing v002 snapshot dir: {}", dir))
        })?;
        Ok(())
    }
}
