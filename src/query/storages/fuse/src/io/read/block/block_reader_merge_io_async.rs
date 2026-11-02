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
use databend_common_metrics::cache::metrics_inc_cache_miss_bytes;
use databend_common_metrics::storage::*;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskFill;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;

use crate::BlockReadResult;
use crate::io::BlockReadContext;
use crate::io::BlockReader;
use crate::io::FuseCaches;
use crate::io::read::block::parquet::ColumnEncoding;

impl BlockReader {
    #[async_backtrace::framed]
    pub async fn read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<BlockReadResult> {
        self.read_context()
            .read_columns_data_by_merge_io(settings, location, columns_meta, ignore_column_ids)
            .await
    }
}

impl BlockReadContext {
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
        let caches = FuseCaches::instance();
        let column_array_cache = &caches.column_arrays;
        let mut cached_column_array = vec![];

        let column_cache_key_builder = ColumnCacheKeyBuilder::new(location);

        for (column_id, _, data_type) in self.project_indices().values() {
            if let Some(ignore_column_ids) = ignore_column_ids {
                if ignore_column_ids.contains(column_id) {
                    continue;
                }
            }

            if let Some(column_meta) = columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();

                let column_array_cache_key = column_cache_key_builder.array_cache_key(
                    column_id,
                    column_meta,
                    &ColumnEncoding::cache_type_key(self.column_encoding(*column_id), data_type),
                );

                // first, check in memory table data cache
                // column_array_cache
                // Bytes are reported as from memory, not from the data cache.
                if let Some(cache_array) =
                    column_array_cache.lookup(column_array_cache_key.as_ref())
                {
                    // Record bytes scanned from memory cache (table data only)
                    Profile::record_usize_profile(
                        ProfileStatisticsName::ScanBytesFromMemory,
                        len as usize,
                    );
                    cached_column_array.push((*column_id, cache_array));
                    continue;
                }
                if column_array_cache.is_enabled() {
                    metrics_inc_cache_miss_bytes(len, column_array_cache.name());
                }

                // Not in memory: the column chunk, from the block meta, is a unit of the disk
                // cache. Its range comes from the meta, not from the merged IO, so the key is
                // the same whatever the query's projection.
                ranges.push((*column_id, offset..(offset + len)));
            }
        }

        // Background tasks (`put_cache = false`) use the disk cache but do not fill it.
        let fill = if self.put_cache() {
            DiskFill::ReadWrite
        } else {
            DiskFill::ReadOnly
        };
        let units = DiskCachedStorage::new(self.operator().clone())
            .read_units(
                CacheKind::of::<column::Data>(),
                location,
                &ranges,
                fill,
                settings,
            )
            .await?;
        let remote: Vec<_> = ranges
            .iter()
            .filter(|(id, _)| !units.cached.contains_key(id))
            .map(|(_, range)| range.clone())
            .collect();
        for range in &remote {
            let len = range.end - range.start;
            metrics_inc_remote_io_seeks(1);
            metrics_inc_remote_io_read_bytes(len);
            Profile::record_usize_profile(ProfileStatisticsName::ScanBytesFromRemote, len as usize);
        }

        let block_read_res = BlockReadResult::from(units).with_column_arrays(cached_column_array);

        self.report_cache_metrics(&block_read_res, remote.iter());

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
    fn array_cache_key(
        &self,
        column_id: &ColumnId,
        column_meta: &ColumnMeta,
        data_type: &str,
    ) -> TableDataCacheKey {
        let (offset, len) = column_meta.offset_length();
        TableDataCacheKey::new(self.block_path, *column_id, offset, len, data_type)
    }
}
