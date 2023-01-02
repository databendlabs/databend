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

use common_storages_cache::CachedObjectAccessor;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::SegmentInfo;

pub struct SegmentReader {
    pub object_accessor: CachedObjectAccessor<SegmentInfo>,
}

impl SegmentReader {
    pub fn create() -> SegmentReader {
        let cache = CacheManager::instance().get_table_segment_cache_v2();
        let object_accessor = match cache {
            // ByPassCache.
            None => CachedObjectAccessor::default(),
            // MemoryItemCache.
            Some(v) => CachedObjectAccessor::create(v),
        };

        SegmentReader { object_accessor }
    }
}
