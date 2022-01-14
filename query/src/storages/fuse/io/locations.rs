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

pub fn gen_block_location() -> String {
    let part_uuid = Uuid::new_v4().simple().to_string() + ".parquet";
    format!("{}/{}", FUSE_TBL_BLOCK_PREFIX, part_uuid)
}

pub fn gen_segment_info_location() -> String {
    let segment_uuid = Uuid::new_v4().simple().to_string();
    format!("{}/{}", FUSE_TBL_SEGMENT_PREFIX, segment_uuid)
}

pub fn snapshot_location(id: &Uuid) -> String {
    format!("{}/{}", FUSE_TBL_SNAPSHOT_PREFIX, id.simple())
}
