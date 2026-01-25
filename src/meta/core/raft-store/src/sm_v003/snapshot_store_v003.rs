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

use std::fs;
use std::io;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::DerefMut;

use databend_common_meta_runtime_api::SpawnApi;
use databend_common_meta_types::snapshot_db::DB;

use crate::config::RaftConfig;
use crate::ondisk::DataVersion;
use crate::sm_v003::WriterV003;
use crate::sm_v003::receiver_v003::ReceiverV003;
use crate::sm_v003::snapshot_loader::SnapshotLoader;
use crate::snapshot_config::SnapshotConfig;

#[derive(Debug)]
pub struct SnapshotStoreV003<SP> {
    v004: SnapshotStoreV004<SP>,
}

impl<SP: SpawnApi> SnapshotStoreV003<SP> {
    pub fn new(config: RaftConfig) -> Self {
        SnapshotStoreV003 {
            v004: SnapshotStoreV004 {
                snapshot_config: SnapshotConfig::new(DataVersion::V003, config),
                _phantom: PhantomData,
            },
        }
    }
}

impl<SP> Deref for SnapshotStoreV003<SP> {
    type Target = SnapshotStoreV004<SP>;

    fn deref(&self) -> &Self::Target {
        &self.v004
    }
}

impl<SP> DerefMut for SnapshotStoreV003<SP> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.v004
    }
}

#[derive(Debug)]
pub struct SnapshotStoreV004<SP> {
    snapshot_config: SnapshotConfig,
    _phantom: PhantomData<SP>,
}

impl<SP: SpawnApi> SnapshotStoreV004<SP> {
    pub fn new(config: RaftConfig) -> Self {
        SnapshotStoreV004 {
            snapshot_config: SnapshotConfig::new(DataVersion::V004, config),
            _phantom: PhantomData,
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.snapshot_config.data_version()
    }

    pub fn config(&self) -> &RaftConfig {
        self.snapshot_config.raft_config()
    }

    pub fn snapshot_config(&self) -> &SnapshotConfig {
        &self.snapshot_config
    }

    /// Create a receiver to receive snapshot in binary form.
    pub fn new_receiver(&self, remote_addr: impl ToString) -> Result<ReceiverV003<SP>, io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;

        let (storage_path, temp_rel_path) = self.snapshot_config.snapshot_temp_dir_fn();
        let temp_path = format!("{}/{}", storage_path, temp_rel_path);

        let f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("{}: while new_receiver(); path: {}", e, temp_path),
                )
            })?;

        let r = ReceiverV003::new(remote_addr, storage_path, temp_rel_path, f);
        Ok(r)
    }

    /// Create a loader to load snapshot from disk
    pub fn new_loader(&self) -> SnapshotLoader<DB> {
        SnapshotLoader::new(self.snapshot_config.clone())
    }

    /// Create a writer to build a snapshot from key-value pairs
    pub fn new_writer(&self) -> Result<WriterV003<SP>, io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;
        WriterV003::new(&self.snapshot_config)
    }
}
