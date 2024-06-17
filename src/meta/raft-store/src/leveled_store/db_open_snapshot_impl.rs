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
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use log::info;
use openraft::SnapshotId;
use rotbl::v001::Rotbl;

use crate::config::RaftConfig;
use crate::sm_v003::open_snapshot::OpenSnapshot;

impl OpenSnapshot for DB {
    fn open_snapshot(
        path: impl ToString,
        snapshot_id: SnapshotId,
        raft_config: &RaftConfig,
    ) -> Result<Self, io::Error> {
        let config = raft_config.to_rotbl_config();
        let r = Rotbl::open(config, path.to_string())?;

        info!("Opened snapshot at {}", path.to_string());

        let db = Self::new(path, snapshot_id, Arc::new(r))?;
        Ok(db)
    }
}
