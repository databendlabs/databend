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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache_manager::CacheManager;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Operator;

use crate::io::Files;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct AbortOperation {
    pub segments: Vec<String>,
    pub blocks: Vec<String>,
    pub bloom_filter_indexes: Vec<String>,
}

impl AbortOperation {
    pub fn merge(&mut self, rhs: AbortOperation) {
        self.segments.extend(rhs.segments);
        self.blocks.extend(rhs.blocks);
        self.bloom_filter_indexes.extend(rhs.bloom_filter_indexes);
    }

    pub fn add_block(&mut self, block: &BlockMeta) {
        let block_location = block.location.clone();
        self.blocks.push(block_location.0);
        if let Some(index) = block.bloom_filter_index_location.clone() {
            self.bloom_filter_indexes.push(index.0);
        }
    }

    pub fn add_segment(&mut self, segment: String) {
        self.segments.push(segment);
    }

    #[async_backtrace::framed]
    pub async fn abort(self, ctx: Arc<dyn TableContext>, operator: Operator) -> Result<()> {
        // evict segment cache.
        if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
            for loc in self.segments.iter() {
                segment_cache.evict(loc);
            }
        }

        let fuse_file = Files::create(ctx, operator);
        let locations = self
            .blocks
            .into_iter()
            .chain(self.bloom_filter_indexes.into_iter())
            .chain(self.segments.into_iter());
        fuse_file.remove_file_in_batch(locations).await
    }
}
