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

use std::io;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use openraft::SnapshotId;
use rotbl::v001::Rotbl;

use crate::sm_v003::open_snapshot::OpenSnapshot;
use crate::snapshot_config::SnapshotConfig;

/// A typed temporary snapshot data.
pub struct TempSnapshotDataV003 {
    storage_path: String,
    rel_path: String,
    snapshot_config: SnapshotConfig,
    inner: Arc<Rotbl>,
}

impl Deref for TempSnapshotDataV003 {
    type Target = Arc<Rotbl>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TempSnapshotDataV003 {
    pub fn new(
        storage_path: impl ToString,
        rel_path: impl ToString,
        snapshot_config: SnapshotConfig,
        r: Arc<Rotbl>,
    ) -> Self {
        Self {
            storage_path: storage_path.to_string(),
            rel_path: rel_path.to_string(),
            snapshot_config,
            inner: r,
        }
    }

    pub fn move_to_final_path(self, snapshot_id: SnapshotId) -> Result<DB, io::Error> {
        let final_path = format!("{}/{}", self.storage_path, self.rel_path);
        let (storage_path, rel_path) = self
            .snapshot_config
            .move_to_final_path(&final_path, snapshot_id.clone())?;

        let db = DB::open_snapshot(
            storage_path,
            rel_path,
            snapshot_id,
            self.snapshot_config.raft_config(),
        )?;
        Ok(db)
    }

    pub fn path(&self) -> String {
        format!("{}/{}", self.storage_path, self.rel_path)
    }
}
