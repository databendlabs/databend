// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use uuid::Uuid;

use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Versioned;

pub const RESULT_CACHE_PREFIX: &str = "_res";

pub struct ResultLocations {
    prefix: String,
}

impl ResultLocations {
    pub fn new(query_id: &str) -> Self {
        ResultLocations {
            prefix: format!("{}/{}", RESULT_CACHE_PREFIX, query_id),
        }
    }

    pub fn get_meta_location(&self) -> String where {
        format!("{}/_t/meta_v{}.json", &self.prefix, SegmentInfo::VERSION,)
    }

    pub fn gen_block_location(&self) -> String {
        let part_uuid = Uuid::new_v4().simple().to_string();
        format!(
            "{}/_t/part-{}_v{}.parquet",
            &self.prefix,
            part_uuid,
            DataBlock::VERSION,
        )
    }
}
