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
use std::fs;
use std::io;
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_meta_types::ErrorSubject;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StorageIOError;
use log::error;
use log::info;
use log::warn;
use openraft::AnyError;
use openraft::ErrorVerb;
use openraft::SnapshotId;

use crate::config::RaftConfig;
use crate::ondisk::DataVersion;
use crate::sm_v002::WriterV002;
use crate::state_machine::MetaSnapshotId;

/// Errors that occur when accessing snapshot store
#[derive(Debug, thiserror::Error)]
#[error("SnapshotStoreError({verb:?}: {source}, while: {context}")]
pub struct SnapshotStoreError {
    verb: ErrorVerb,

    #[source]
    source: io::Error,
    context: String,
}

impl SnapshotStoreError {
    pub fn read(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Read,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn write(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Write,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn add_context(&mut self, context: impl Display) {
        if self.context.is_empty() {
            self.context = context.to_string();
        } else {
            self.context = format!("{}; while {}", self.context, context);
        }
    }

    pub fn with_context(mut self, context: impl Display) -> Self {
        self.add_context(context);
        self
    }

    /// Add meta and context info to the error.
    ///
    /// meta is anything that can be displayed.
    pub fn with_meta(self, context: impl Display, meta: impl Display) -> Self {
        self.with_context(format_args!("{}: {}", context, meta))
    }
}

impl From<SnapshotStoreError> for StorageError {
    fn from(error: SnapshotStoreError) -> Self {
        let sto_io_err = StorageIOError::new(
            ErrorSubject::Snapshot(None),
            error.verb,
            AnyError::new(&error),
        );
        StorageError::IO { source: sto_io_err }
    }
}

#[derive(Debug)]
pub struct SnapshotStoreV002 {
    data_version: DataVersion,
    config: RaftConfig,
}

impl SnapshotStoreV002 {
    const TEMP_PREFIX: &'static str = "0.snap";

    pub fn new(data_version: DataVersion, config: RaftConfig) -> Self {
        SnapshotStoreV002 {
            data_version,
            config,
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version
    }

    pub fn snapshot_dir(&self) -> String {
        format!(
            "{}/df_meta/{}/snapshot",
            self.config.raft_dir, self.data_version
        )
    }

    pub fn snapshot_path(&self, snapshot_id: &SnapshotId) -> String {
        format!("{}/{}", self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    pub fn snapshot_fn(snapshot_id: &SnapshotId) -> String {
        format!("{}.snap", snapshot_id)
    }

    pub fn snapshot_temp_path(&self) -> String {
        // Sleep to avoid timestamp collision when this function is called twice in a short time.
        std::thread::sleep(std::time::Duration::from_millis(2));

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        format!("{}/{}-{}", self.snapshot_dir(), Self::TEMP_PREFIX, ts)
    }

    /// Return a list of valid snapshot ids found in the snapshot directory.
    pub async fn load_last_snapshot(
        &self,
    ) -> Result<Option<(MetaSnapshotId, SnapshotData)>, SnapshotStoreError> {
        let (snapshot_ids, _invalid_files) = self.load_snapshot_ids().await?;
        // dbg!(&snapshot_ids);

        info!("choose the latest from found snapshots: {:?}", snapshot_ids);

        let id = if let Some(id) = snapshot_ids.last().cloned() {
            id
        } else {
            return Ok(None);
        };

        let data = self.load_snapshot(&id.to_string()).await?;

        Ok(Some((id, data)))
    }

    /// Keep several latest snapshots and cleanup the rest.
    pub async fn clean_old_snapshots(&self) -> Result<(), SnapshotStoreError> {
        let dir = self.ensure_snapshot_dir()?;

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
            let path = self.snapshot_path(&snapshot_id.to_string());

            info!("removing old snapshot file: {}", path);

            tokio::fs::remove_file(&path).await.map_err(|e| {
                SnapshotStoreError::write(e).with_context(format_args!("removing {}", &path))
            })?;
        }

        Ok(())
    }

    /// Return a list of valid snapshot ids and invalid file names found in the snapshot directory.
    ///
    /// The valid snapshot ids are sorted, older first.
    pub async fn load_snapshot_ids(
        &self,
    ) -> Result<(Vec<MetaSnapshotId>, Vec<String>), SnapshotStoreError> {
        let mut snapshot_ids = vec![];
        let mut invalid_files = vec![];

        let dir = self.ensure_snapshot_dir()?;

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

    pub fn new_writer(&mut self) -> Result<WriterV002, SnapshotStoreError> {
        self.ensure_snapshot_dir()?;

        WriterV002::new(self)
            .map_err(|e| SnapshotStoreError::write(e).with_context("creating snapshot writer"))
    }

    /// Create a temp and empty snapshot data to receive snapshot from remote.
    pub async fn new_temp(&self) -> Result<SnapshotData, io::Error> {
        let p = self.snapshot_temp_path();

        SnapshotData::new_temp(p).await
    }

    /// Return a snapshot for async reading
    pub async fn load_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotData, SnapshotStoreError> {
        self.ensure_snapshot_dir()?;

        let path = self.snapshot_path(snapshot_id);

        let d = SnapshotData::open(path.clone()).map_err(|e| {
            error!("failed to open snapshot file({}): {}", path, e);
            SnapshotStoreError::read(e).with_meta("opening snapshot file", path)
        })?;

        Ok(d)
    }

    /// Finish receiving a snapshot.
    ///
    /// Move it from the temp path to the final path and make it visible.
    /// It returns the final snapshot for reading.
    pub async fn commit_received(
        &self,
        mut temp: Box<SnapshotData>,
        meta: &SnapshotMeta,
    ) -> Result<SnapshotData, SnapshotStoreError> {
        assert!(temp.is_temp());

        let src = temp.path().to_string();

        temp.sync_all()
            .await
            .map_err(|e| SnapshotStoreError::read(e).with_meta("temp.sync_all(): {}", &src))?;

        let dst = self.snapshot_path(&meta.snapshot_id);

        fs::rename(&src, &dst).map_err(|e| {
            SnapshotStoreError::read(e).with_context(format_args!("rename: {} to {}", &src, &dst))
        })?;

        let d = self.load_snapshot(&meta.snapshot_id).await?;

        Ok(d)
    }

    /// Make directory for snapshot if it does not exist and return the snapshot directory.
    fn ensure_snapshot_dir(&self) -> Result<String, SnapshotStoreError> {
        let dir = self.snapshot_dir();

        fs::create_dir_all(&dir)
            .map_err(|e| SnapshotStoreError::write(e).with_meta("creating snapshot dir", &dir))?;

        Ok(dir)
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

#[cfg(test)]
mod tests {
    use crate::config::RaftConfig;
    use crate::ondisk::DATA_VERSION;

    #[test]
    fn test_temp_path_no_dup() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let p = temp.path();
        let raft_config = RaftConfig {
            raft_dir: p.to_str().unwrap().to_string(),
            ..Default::default()
        };

        let store = super::SnapshotStoreV002::new(DATA_VERSION, raft_config);

        let mut prev = None;
        for _i in 0..10 {
            let path = store.snapshot_temp_path();
            assert_ne!(prev, Some(path.clone()), "dup: {}", path);
            prev = Some(path);
        }

        Ok(())
    }
}
