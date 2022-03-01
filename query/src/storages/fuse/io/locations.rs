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

use uuid::Uuid;

use crate::storages::fuse::constants::FUSE_TBL_BLOCK_PREFIX;
use crate::storages::fuse::constants::FUSE_TBL_SEGMENT_PREFIX;
use crate::storages::fuse::constants::FUSE_TBL_SNAPSHOT_PREFIX;

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
        let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
        format!("{}/{}/{}", &self.prefix, FUSE_TBL_BLOCK_PREFIX, part_uuid)
    }

    pub fn gen_segment_info_location(&self) -> String {
        let segment_uuid = Uuid::new_v4().to_simple().to_string();
        format!(
            "{}/{}/{}",
            &self.prefix, FUSE_TBL_SEGMENT_PREFIX, segment_uuid
        )
    }

    pub fn snapshot_location_from_uuid(&self, id: &Uuid) -> String {
        format!(
            "{}/{}/{}",
            &self.prefix,
            FUSE_TBL_SNAPSHOT_PREFIX,
            id.to_simple()
        )
    }
}
