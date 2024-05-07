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

use databend_common_meta_types::snapshot_db::DB;
use databend_common_meta_types::sys_data::SysData;

use crate::config::RaftConfig;
use crate::ondisk::DataVersion;
use crate::sm_v003::receiver_v003::Receiver;
use crate::sm_v003::snapshot_loader::SnapshotLoader;
use crate::sm_v003::WriterV003;
use crate::snapshot_config::SnapshotConfig;

#[derive(Debug)]
pub struct SnapshotStoreV003 {
    snapshot_config: SnapshotConfig,
}

impl SnapshotStoreV003 {
    pub fn new(config: RaftConfig) -> Self {
        SnapshotStoreV003 {
            snapshot_config: SnapshotConfig::new(DataVersion::V003, config),
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

    pub fn new_receiver(&self, remote_addr: impl ToString) -> Result<Receiver, io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;

        let temp_path = self.snapshot_config.snapshot_temp_path();

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

        let r = Receiver::new(remote_addr, temp_path, f);
        Ok(r)
    }

    pub fn new_loader(&self) -> SnapshotLoader<DB> {
        SnapshotLoader::new(self.snapshot_config.clone())
    }

    pub fn new_writer(&self) -> Result<WriterV003, io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;
        WriterV003::new(&self.snapshot_config)
    }

    /// Create a new temp file to receive snapshot data in binary format
    pub fn new_temp_file(&self) -> Result<(String, fs::File), io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;

        let temp_path = self.snapshot_config.snapshot_temp_path();

        let f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "{}: while(SnapshotStoreV003::new_temp_file(); path: {})",
                        e, temp_path
                    ),
                )
            })?;

        Ok((temp_path, f))
    }

    /// This method is only used to pass openraft test.
    pub fn new_temp(&self) -> Result<DB, io::Error> {
        self.snapshot_config.ensure_snapshot_dir()?;

        let w = self.new_writer()?;
        let temp_data = w.flush(SysData::default())?;

        let temp_id = self.snapshot_config.temp_snapshot_id();
        let db = temp_data.move_to_final_path(temp_id.clone())?;
        Ok(db)
    }
}
