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
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_meta_types::snapshot_db::DB;
use log::info;
use openraft::SnapshotId;
use rotbl::storage::impls::fs::FsStorage;
use rotbl::v001::Rotbl;

use crate::sm_v003::open_snapshot::OpenSnapshot;

impl OpenSnapshot for DB {
    fn open_snapshot(
        storage_path: impl ToString,
        rel_path: impl ToString,
        snapshot_id: SnapshotId,
        config: rotbl::v001::Config,
    ) -> Result<Self, io::Error> {
        let storage_path = storage_path.to_string();
        let rel_path = rel_path.to_string();

        let storage = FsStorage::new(PathBuf::from(&storage_path));

        let r = Rotbl::open(storage, config, &rel_path.to_string())?;

        info!("Opened snapshot at {storage_path}/{rel_path}");

        let db = Self::new(storage_path, rel_path, snapshot_id, Arc::new(r))?;
        Ok(db)
    }
}
