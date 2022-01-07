// Copyright 2021 Datafuse Labs.
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

use async_trait::async_trait;
use common_dal::DataAccessor;
use common_exception::Result;

use crate::configs::QueryConfig;
use crate::storages::fuse::io::BlockMetaCache;
use crate::storages::fuse::io::SegmentInfoCache;
use crate::storages::fuse::io::TableSnapshotCache;

#[async_trait]
pub trait StorageCache: Send + Sync {
    async fn get(&self, location: &str, da: &dyn DataAccessor) -> Result<Vec<u8>>;
}

pub struct CacheManager {
    table_snapshot_cache: Option<TableSnapshotCache>,
    segment_info_cache: Option<SegmentInfoCache>,
    block_meta_cache: Option<BlockMetaCache>,
}

impl CacheManager {
    pub fn init(config: &QueryConfig) -> Result<Self> {
        if !config.table_cache_enabled {
            Ok(Self {
                table_snapshot_cache: None,
                segment_info_cache: None,
                block_meta_cache: None,
            })
        } else {
            todo!()
        }
    }

    pub fn get_table_snapshot_cache(&self) -> Option<TableSnapshotCache> {
        self.table_snapshot_cache.clone()
    }

    pub fn get_table_segment_cache(&self) -> Option<SegmentInfoCache> {
        self.segment_info_cache.clone()
    }

    pub fn get_block_meta_cache(&self) -> Option<BlockMetaCache> {
        self.block_meta_cache.clone()
    }
}
