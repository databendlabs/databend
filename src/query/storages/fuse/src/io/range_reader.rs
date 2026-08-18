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

use databend_common_exception::Result;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::DiskCacheRangeReader;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::RangeReader;
use opendal::Operator;

const DISK_CACHE_CHUNK_SIZE: u64 = 1024 * 1024;

pub(crate) fn create_file_range_reader(
    operator: Operator,
    path: String,
    file_len: u64,
    max_prefetch: usize,
    max_segment_size: u64,
    held_budget: usize,
) -> Result<Box<dyn RangeReader>> {
    let tail = OperatorRangeReader::new(operator, path.clone(), max_prefetch.saturating_add(1));
    let cache_manager = CacheManager::instance();
    let mut disk_cache = None;
    if let Some(column_cache) = cache_manager.get_column_data_cache() {
        if let Some(cache) = column_cache.on_disk_cache() {
            disk_cache = Some(cache.lru_disk_cache().clone());
        }
    }

    match disk_cache {
        Some(cache) => Ok(Box::new(DiskCacheRangeReader::new(
            cache,
            tail,
            path,
            file_len,
            DISK_CACHE_CHUNK_SIZE,
            max_segment_size,
            held_budget,
        )?)),
        None => Ok(Box::new(tail)),
    }
}
