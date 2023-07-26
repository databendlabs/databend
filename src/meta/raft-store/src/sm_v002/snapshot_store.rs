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

use common_meta_types::ErrorSubject;
use common_meta_types::SnapshotData;
use common_meta_types::SnapshotMeta;
use common_meta_types::StorageError;
use common_meta_types::StorageIOError;
use openraft::AnyError;
use openraft::ErrorVerb;
use openraft::SnapshotId;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::config::RaftConfig;
use crate::ondisk::DataVersion;
use crate::sm_v002::writer_v002::WriterV002;
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
            error.verb.clone(),
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
    pub fn new(data_version: DataVersion, config: RaftConfig) -> Self {
        SnapshotStoreV002 {
            data_version,
            config,
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version
    }

    fn snapshot_dir(&self) -> String {
        format!(
            "{}/df_meta/{}/snapshot",
            self.config.raft_dir, self.data_version
        )
    }

    pub fn snapshot_path(&self, snapshot_id: &SnapshotId) -> String {
        format!("{}/{}", self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    fn snapshot_fn(snapshot_id: &SnapshotId) -> String {
        format!("{}.snap", snapshot_id)
    }

    fn extract_snapshot_id_from_fn(fn_: &str) -> Option<&str> {
        if fn_.ends_with(".snap") {
            let x = &fn_[..fn_.len() - ".snap".len()];
            Some(x)
        } else {
            None
        }
    }

    pub fn snapshot_temp_path(&self) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        format!("{}/0.snap-{}", self.snapshot_dir(), ts)
    }

    /// Make directory for snapshot if it does not exist.
    fn ensure_snapshot_dir(&self) -> Result<(), SnapshotStoreError> {
        let dir = self.snapshot_dir();

        fs::create_dir_all(&dir)
            .map_err(|e| SnapshotStoreError::write(e).with_meta("creating snapshot dir", &dir))?;

        Ok(())
    }

    /// Return a list of valid snapshot ids found in the snapshot directory.
    pub async fn load_last_snapshot(
        &self,
    ) -> Result<Option<(MetaSnapshotId, SnapshotData)>, SnapshotStoreError> {
        // TODO: cleanup old snapshot and temp file

        self.ensure_snapshot_dir()?;

        let dir = self.snapshot_dir();

        fn make_err(e: io::Error, context: impl Display) -> SnapshotStoreError {
            let s = context.to_string();
            error!("{} while context: {}", e, s);
            SnapshotStoreError::read(e).with_context(context)
        }

        let mut read_dir = tokio::fs::read_dir(&dir)
            .await
            .map_err(|e| make_err(e, format_args!("reading snapshot dir: {}", &dir)))?;

        let mut snapshot_ids = vec![];

        while let Some(dent) = read_dir
            .next_entry()
            .await
            .map_err(|e| make_err(e, format_args!("reading snapshot dir entry: {}", &dir)))?
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
                    continue;
                };

                snapshot_ids.push(meta_snap_id);
            }
        }

        snapshot_ids.sort();

        info!("choose the latest from found snapshots: {:?}", snapshot_ids);

        let id = if let Some(id) = snapshot_ids.last().cloned() {
            id
        } else {
            return Ok(None);
        };

        let data = self.new_reader(&id.to_string()).await?;

        Ok(Some((id, data)))
    }

    pub fn new_writer(&mut self) -> Result<WriterV002, SnapshotStoreError> {
        self.ensure_snapshot_dir()?;

        WriterV002::new(self)
            .map_err(|e| SnapshotStoreError::write(e).with_context("creating snapshot writer"))
    }

    pub async fn new_temp(&self) -> Result<SnapshotData, io::Error> {
        let p = self.snapshot_temp_path();

        SnapshotData::new_temp(p).await
    }

    /// Return a snapshot for async reading
    pub async fn new_reader(
        &self,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotData, SnapshotStoreError> {
        self.ensure_snapshot_dir()?;

        let path = self.snapshot_path(snapshot_id);

        let d = SnapshotData::open(path.clone()).await.map_err(|e| {
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

        let d = self.new_reader(&meta.snapshot_id).await?;

        Ok(d)
    }
}
