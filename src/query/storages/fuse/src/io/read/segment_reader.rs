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

use std::sync::Arc;

use common_exception::Result;
use common_storages_cache::CachedObjectAccessor;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::SegmentInfo;
use opendal::Operator;

pub struct SegmentReader {
    dal: Operator,
    object_accessor: CachedObjectAccessor<SegmentInfo>,
}

impl SegmentReader {
    pub fn create(dal: Operator) -> SegmentReader {
        let cache = CacheManager::instance().get_table_segment_cache_v2();
        let object_accessor = match cache {
            // ByPassCache.
            None => CachedObjectAccessor::default(),
            // MemoryItemCache.
            Some(v) => CachedObjectAccessor::create(v),
        };

        SegmentReader {
            dal,
            object_accessor,
        }
    }

    pub async fn read(
        &self,
        path: &str,
        len_hint: Option<u64>,
        _version: u64,
    ) -> Result<Arc<SegmentInfo>> {
        let object = self.dal.object(path);
        let len = match len_hint {
            None => object.content_length().await?,
            Some(v) => v,
        };
        self.object_accessor.read(&object, 0, len).await
    }
}
