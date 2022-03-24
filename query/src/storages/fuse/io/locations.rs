//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_exception::Result;
use uuid::Uuid;

use crate::storages::fuse::constants::FUSE_TBL_BLOCK_PREFIX;
use crate::storages::fuse::constants::FUSE_TBL_SEGMENT_PREFIX;
use crate::storages::fuse::constants::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::storages::fuse::meta::SnapshotVersion;
use crate::storages::fuse::meta::CURRNET_BLOCK_VERSION;
use crate::storages::fuse::meta::CURRNET_SEGMETN_VERSION;
use crate::storages::fuse::meta::CURRNET_SNAPSHOT_VERSION;

#[derive(Clone)]
pub struct TableMetaLocationGenerator {
    prefix: String,
}

impl TableMetaLocationGenerator {
    pub fn with_prefix(prefix: String) -> Self {
        Self { prefix }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn gen_block_location(&self) -> String {
        let part_uuid = Uuid::new_v4().to_simple().to_string();
        format!(
            "{}/{}/{}_v{}.parquet",
            &self.prefix, FUSE_TBL_BLOCK_PREFIX, part_uuid, CURRNET_BLOCK_VERSION
        )
    }

    pub fn gen_segment_info_location(&self) -> String where {
        let segment_uuid = Uuid::new_v4().to_simple().to_string();
        format!(
            "{}/{}/{}_v{}.json",
            &self.prefix, FUSE_TBL_SEGMENT_PREFIX, segment_uuid, CURRNET_SEGMETN_VERSION
        )
    }

    pub fn snapshot_location_from_uuid(&self, id: &Uuid, version: u64) -> Result<String> {
        let snaphost_version = SnapshotVersion::try_from(version)?;
        Ok(snaphost_version.create(id, &self.prefix))
    }
}

trait SnapshotLocationCreator {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String;
}

impl SnapshotLocationCreator for SnapshotVersion {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String {
        match self {
            SnapshotVersion::V0(_) => {
                format!(
                    "{}/{}/{}",
                    prefix.as_ref(),
                    FUSE_TBL_SNAPSHOT_PREFIX,
                    id.to_simple(),
                )
            }
            SnapshotVersion::V1(_) => {
                format!(
                    "{}/{}/{}_v{}.json",
                    prefix.as_ref(),
                    FUSE_TBL_SNAPSHOT_PREFIX,
                    id.to_simple(),
                    CURRNET_SNAPSHOT_VERSION,
                )
            }
        }
    }
}
