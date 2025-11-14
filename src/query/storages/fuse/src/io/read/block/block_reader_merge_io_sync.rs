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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;

use crate::io::BlockReader;
use crate::BlockReadResult;

impl BlockReader {
    pub fn sync_read_columns_data_by_merge_io_2(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<BlockReadResult> {
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();

        let mut ranges = vec![];
        let mut cached_column_array = vec![];
        for (_index, (column_id, ..)) in self.project_indices.iter() {
            if let Some(ignore_column_ids) = ignore_column_ids {
                if ignore_column_ids.contains(column_id) {
                    continue;
                }
            }

            let block_path = location;

            if let Some(column_meta) = columns_meta.get(column_id) {
                // first, check column array object cache
                let (offset, len) = column_meta.offset_length();
                let column_cache_key = TableDataCacheKey::new(block_path, *column_id, offset, len);
                if let Some(cache_array) = column_array_cache.get(&column_cache_key) {
                    cached_column_array.push((*column_id, cache_array));
                    continue;
                }
                ranges.push((*column_id, offset..(offset + len)));
            }
        }

        let merge_io_result =
            MergeIOReader::sync_merge_io_read(settings, self.operator.clone(), location, &ranges)?;

        // for sync read, we disable table data cache
        let cached_column_data = vec![];
        let block_read_res =
            BlockReadResult::create(merge_io_result, cached_column_data, cached_column_array);

        self.report_cache_metrics(&block_read_res, ranges.iter().map(|(_, r)| r));

        Ok(block_read_res)
    }
}
