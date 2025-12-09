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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::*;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;

use crate::io::BlockReader;
use crate::BlockReadResult;

impl BlockReader {
    #[async_backtrace::framed]
    pub async fn read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<BlockReadResult> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let mut ranges = vec![];
        // for async read, try using table data cache (if enabled in settings)
        let column_data_cache = CacheManager::instance().get_column_data_cache();
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();
        let mut cached_column_data = vec![];
        let mut cached_column_array = vec![];

        let column_cache_key_builder = ColumnCacheKeyBuilder::new(location);

        for (_index, (column_id, ..)) in self.project_indices.iter() {
            if let Some(ignore_column_ids) = ignore_column_ids {
                if ignore_column_ids.contains(column_id) {
                    continue;
                }
            }

            if let Some(column_meta) = columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();

                let column_cache_key = column_cache_key_builder.cache_key(column_id, column_meta);

                // first, check in memory table data cache
                // column_array_cache
                if let Some(cache_array) = column_array_cache.get_sized(&column_cache_key, len) {
                    // Record bytes scanned from memory cache (table data only)
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytesFromMemory,
                        len as usize,
                    );
                    cached_column_array.push((*column_id, cache_array));
                    continue;
                }

                // and then, check on disk table data cache
                if let Some(cached_column_raw_data) =
                    column_data_cache.get_sized(&column_cache_key, len)
                {
                    cached_column_data.push((*column_id, cached_column_raw_data));
                    continue;
                }

                // if all caches missed, prepare the ranges to be read
                ranges.push((*column_id, offset..(offset + len)));

                // Perf
                {
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }

                // Record bytes scanned from remote storage
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytesFromRemote,
                    len as usize,
                );
            }
        }

        let merge_io_result =
            MergeIOReader::merge_io_read(settings, self.operator.clone(), location, &ranges)
                .await?;

        if self.put_cache {
            // add raw data (compressed raw bytes) to column cache
            for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
                // Should NOT use `range.start` as part of the cache key,
                // as they are not stable and can vary for the same column depending on the query's projection.
                // For instance:
                //  - `SELECT col1, col2 FROM t;`
                //  - `SELECT col2 FROM t;`
                // may result in different ranges for `col2`
                // This can lead to cache missing or INCONSISTENCIES

                // Safe to unwrap here, since this column has been fetched, its meta must be present.
                let column_meta = columns_meta.get(column_id).unwrap();
                let column_cache_key = column_cache_key_builder.cache_key(column_id, column_meta);

                let chunk_data = merge_io_result
                    .owner_memory
                    .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
                let data = chunk_data.slice(range.clone());

                column_data_cache.insert(
                    column_cache_key.as_ref().to_owned(),
                    ColumnData::from_merge_io_read_result(data.to_vec()),
                );
            }
        }

        let block_read_res =
            BlockReadResult::create(merge_io_result, cached_column_data, cached_column_array);

        self.report_cache_metrics(&block_read_res, ranges.iter().map(|(_, r)| r));

        Ok(block_read_res)
    }
}

struct ColumnCacheKeyBuilder<'a> {
    block_path: &'a str,
}

impl<'a> ColumnCacheKeyBuilder<'a> {
    fn new(block_path: &'a str) -> Self {
        Self { block_path }
    }
    fn cache_key(&self, column_id: &ColumnId, column_meta: &ColumnMeta) -> TableDataCacheKey {
        let (offset, len) = column_meta.offset_length();
        TableDataCacheKey::new(self.block_path, *column_id, offset, len)
    }
}
