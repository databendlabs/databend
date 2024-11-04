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

use std::fmt::Display;
use std::io;
use std::marker::PhantomData;
use std::str::FromStr;

use log::error;
use log::info;
use log::warn;
use openraft::SnapshotId;

use crate::sm_v003::open_snapshot::OpenSnapshot;
use crate::sm_v003::SnapshotStoreError;
use crate::snapshot_config::SnapshotConfig;
use crate::state_machine::MetaSnapshotId;

/// Load snapshot from persisted storage.
///
/// It has a generic parameter `SD` which is the type of snapshot data
/// that implements [`OpenSnapshot`].
pub struct SnapshotLoader<SD> {
    snapshot_config: SnapshotConfig,
    _p: PhantomData<SD>,
}

impl<SD> SnapshotLoader<SD>
where SD: OpenSnapshot
{
    pub(crate) fn new(snapshot_config: SnapshotConfig) -> Self {
        SnapshotLoader {
            snapshot_config,
            _p: PhantomData,
        }
    }

    /// Return a list of valid snapshot ids found in the snapshot directory.
    pub async fn load_last_snapshot(
        &self,
    ) -> Result<Option<(MetaSnapshotId, SD)>, SnapshotStoreError> {
        let (snapshot_ids, _invalid_files) = self.load_snapshot_ids().await?;

        info!("choose the latest from found snapshots: {:?}", snapshot_ids);

        let id = if let Some(id) = snapshot_ids.last().cloned() {
            id
        } else {
            return Ok(None);
        };

        let data = self.load_snapshot(&id.to_string()).await?;

        Ok(Some((id, data)))
    }

    /// Return a snapshot for async reading
    pub async fn load_snapshot(&self, snapshot_id: &SnapshotId) -> Result<SD, SnapshotStoreError> {
        self.snapshot_config
            .ensure_snapshot_dir()
            .map_err(SnapshotStoreError::write)?;

        let path = self.snapshot_config.snapshot_path(snapshot_id);

        let d = SD::open_snapshot(
            path.clone(),
            snapshot_id.clone(),
            self.snapshot_config.raft_config(),
        )
        .map_err(|e| {
            error!("failed to open snapshot file({}): {}", path, e);
            SnapshotStoreError::read(e).with_meta("opening snapshot file", path)
        })?;

        Ok(d)
    }

    /// Return a list of valid snapshot ids and invalid file names found in the snapshot directory.
    ///
    /// The valid snapshot ids are sorted, older first.
    pub async fn load_snapshot_ids(
        &self,
    ) -> Result<(Vec<MetaSnapshotId>, Vec<String>), SnapshotStoreError> {
        let mut snapshot_ids = vec![];
        let mut invalid_files = vec![];

        let dir = self
            .snapshot_config
            .ensure_snapshot_dir()
            .map_err(SnapshotStoreError::write)?;

        let mut read_dir = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| Self::make_err(e, format_args!("reading snapshot dir: {}", &dir)))?;

        while let Some(dent) = read_dir
            .next_entry()
            .await
            .map_err(|e| Self::make_err(e, format_args!("reading snapshot dir entry: {}", &dir)))?
        {
            let file_name = if let Some(x) = dent.file_name().to_str() {
                x.to_string()
            } else {
                continue;
            };

            if let Some(snapshot_id_str) = Self::extract_snapshot_id_from_fn(&file_name) {
                let meta_snap_id = if let Ok(x) = MetaSnapshotId::from_str(snapshot_id_str) {
                    x
                } else {
                    warn!("found invalid snapshot id: {}", file_name);
                    invalid_files.push(file_name);
                    continue;
                };

                snapshot_ids.push(meta_snap_id);
            }
        }

        snapshot_ids.sort();
        invalid_files.sort();

        info!("dir: {}; loaded snapshots: {:?}", dir, snapshot_ids);
        info!("dir: {}; invalid files: {:?}", dir, invalid_files);

        Ok((snapshot_ids, invalid_files))
    }

    /// Keep several latest snapshots and cleanup the rest.
    pub async fn clean_old_snapshots(&self) -> Result<(), SnapshotStoreError> {
        let dir = self
            .snapshot_config
            .ensure_snapshot_dir()
            .map_err(SnapshotStoreError::read)?;

        info!("cleaning old snapshots in {}", dir);

        let (snapshot_ids, mut invalid_files) = self.load_snapshot_ids().await?;

        // The last several temp files may be in use by snapshot transmitting.
        // And do not delete them at once.
        {
            let l = invalid_files.len();
            if l > 2 {
                invalid_files = invalid_files.into_iter().take(l - 2).collect();
            } else {
                invalid_files = vec![];
            }
        }

        for invalid_file in invalid_files {
            let path = format!("{}/{}", dir, invalid_file);

            warn!("removing invalid snapshot file: {}", path);

            tokio::fs::remove_file(&path).await.map_err(|e| {
                SnapshotStoreError::write(e).with_context(format_args!("removing {}", &path))
            })?;
        }

        // Keep the last several snapshots, remove others
        let n = 3;
        if snapshot_ids.len() <= n {
            info!(
                "no need to clean snapshots(keeps {} snapshots): {:?}",
                n, snapshot_ids
            );
            return Ok(());
        }

        info!("cleaning snapshots, keep last {}: {:?}", n, snapshot_ids);

        for snapshot_id in snapshot_ids.iter().take(snapshot_ids.len() - n) {
            let path = self.snapshot_config.snapshot_path(&snapshot_id.to_string());

            info!("removing old snapshot file: {}", path);

            tokio::fs::remove_file(&path).await.map_err(|e| {
                SnapshotStoreError::write(e).with_context(format_args!("removing {}", &path))
            })?;
        }

        Ok(())
    }

    fn extract_snapshot_id_from_fn(filename: &str) -> Option<&str> {
        if let Some(snapshot_id) = filename.strip_suffix(".snap") {
            Some(snapshot_id)
        } else {
            None
        }
    }

    /// Build a [`SnapshotStoreError`] from io::Error with context.
    fn make_err(e: io::Error, context: impl Display) -> SnapshotStoreError {
        let s = context.to_string();
        error!("{} while context: {}", e, s);
        SnapshotStoreError::read(e).with_context(context)
    }
}
