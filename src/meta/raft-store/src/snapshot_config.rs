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

//! Snapshot storage path configuration and management.

use std::fs;
use std::io;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use log::info;
use openraft::SnapshotId;

use crate::config::RaftConfig;
use crate::ondisk::DataVersion;

/// Path related config for Raft store.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    data_version: DataVersion,
    raft_config: RaftConfig,
}

impl SnapshotConfig {
    const TEMP_PREFIX: &'static str = "0.snap";

    pub fn new(data_version: DataVersion, config: RaftConfig) -> Self {
        SnapshotConfig {
            data_version,
            raft_config: config,
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version
    }

    pub fn raft_config(&self) -> &RaftConfig {
        &self.raft_config
    }

    pub fn version_dir(&self) -> String {
        format!(
            "{}/df_meta/{}",
            self.raft_config.raft_dir, self.data_version
        )
    }

    pub fn snapshot_dir(&self) -> String {
        format!(
            "{}/df_meta/{}/snapshot",
            self.raft_config.raft_dir, self.data_version
        )
    }

    /// Return a two element tuple of snapshot dir and fn
    pub fn snapshot_dir_fn(&self, snapshot_id: &SnapshotId) -> (String, String) {
        (self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    pub fn snapshot_path(&self, snapshot_id: &SnapshotId) -> String {
        format!("{}/{}", self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    pub fn snapshot_fn(snapshot_id: &SnapshotId) -> String {
        format!("{}.snap", snapshot_id)
    }

    // TODO: remove this
    /// Return a two elements tuple of snapshot dir and temp fn
    pub fn snapshot_temp_dir_fn(&self) -> (String, String) {
        let temp_snapshot_id = self.temp_snapshot_id();
        (self.snapshot_dir(), temp_snapshot_id)
    }

    // TODO: remove this
    pub fn snapshot_temp_path(&self) -> String {
        let temp_snapshot_id = self.temp_snapshot_id();
        format!("{}/{}", self.snapshot_dir(), temp_snapshot_id)
    }

    pub fn temp_snapshot_id(&self) -> String {
        // Sleep to avoid timestamp collision when this function is called twice in a short time.
        std::thread::sleep(std::time::Duration::from_millis(2));

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        format!("{}-{}", Self::TEMP_PREFIX, ts)
    }

    /// Make directory for snapshot if it does not exist and return the snapshot directory.
    pub(crate) fn ensure_snapshot_dir(&self) -> Result<String, io::Error> {
        let dir = self.snapshot_dir();

        fs::create_dir_all(&dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while create_dir_all(); path: {}", e, dir),
            )
        })?;

        Ok(dir)
    }

    /// Move the snapshot to the final path.
    ///
    /// So that it is visible and can be loaded.
    ///
    /// It returns the final storage path and rel path.
    pub fn move_to_final_path(
        &self,
        temp_path: &str,
        snapshot_id: SnapshotId,
    ) -> Result<(String, String), io::Error> {
        let (storage_path, rel_path) = self.snapshot_dir_fn(&snapshot_id);
        let final_path = format!("{storage_path}/{rel_path}");

        fs::rename(temp_path, &final_path)?;

        info!(
            "snapshot {} moved to final path: {}",
            snapshot_id, final_path
        );

        Ok((storage_path, rel_path))
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

        let store = super::SnapshotConfig::new(DATA_VERSION, raft_config);

        let mut prev = None;
        for _i in 0..10 {
            let path = store.snapshot_temp_path();
            assert_ne!(prev, Some(path.clone()), "dup: {}", path);
            prev = Some(path);
        }

        Ok(())
    }
}
