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
use std::ops::Range;
use std::time::Instant;

use common_base::rangemap::RangeMerger;
use common_base::runtime::UnlimitedFuture;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use futures::future::try_join_all;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache::TableDataCacheKey;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::ColumnMeta;

use crate::io::read::block::block_reader_merge_io::OwnerMemory;
use crate::io::read::ReadSettings;
use crate::io::BlockReader;
use crate::metrics::*;
use crate::MergeIOReadResult;

impl BlockReader {
    /// This is an optimized for data read, works like the Linux kernel io-scheduler IO merging.
    /// If the distance between two IO request ranges to be read is less than storage_io_min_bytes_for_seek(Default is 48Bytes),
    /// will read the range that contains both ranges, thus avoiding extra seek.
    ///
    /// It will *NOT* merge two requests:
    /// if the last io request size is larger than storage_io_page_bytes_for_read(Default is 512KB).
    #[async_backtrace::framed]
    async fn merge_io_read(
        read_settings: &ReadSettings,
        op: Operator,
        location: &str,
        raw_ranges: Vec<(ColumnId, Range<u64>)>,
    ) -> Result<MergeIOReadResult> {
        if raw_ranges.is_empty() {
            // shortcut
            let read_res = MergeIOReadResult::create(
                OwnerMemory::create(vec![]),
                raw_ranges.len(),
                location.to_string(),
                CacheManager::instance().get_table_data_cache(),
            );
            return Ok(read_res);
        }

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.storage_io_min_bytes_for_seek,
            read_settings.storage_io_max_page_bytes_for_read,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut read_handlers = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            // Perf.
            {
                metrics_inc_remote_io_seeks_after_merged(1);
                metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
            }

            read_handlers.push(UnlimitedFuture::create(Self::read_range(
                op.clone(),
                location,
                idx,
                range.start,
                range.end,
            )));
        }

        let start = Instant::now();
        let owner_memory = OwnerMemory::create(try_join_all(read_handlers).await?);
        let table_data_cache = CacheManager::instance().get_table_data_cache();
        let mut read_res = MergeIOReadResult::create(
            owner_memory,
            raw_ranges.len(),
            location.to_string(),
            table_data_cache,
        );

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        for (raw_idx, raw_range) in &raw_ranges {
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = range_merger.get(column_range.clone()).ok_or(ErrorCode::Internal(format!(
                "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                column_range, location, merged_ranges
            )))?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            let column_id = *raw_idx as ColumnId;
            read_res.add_column_chunk(merged_range_idx, column_id, start..end);
        }

        Ok(read_res)
    }

    #[async_backtrace::framed]
    pub async fn read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
    ) -> Result<MergeIOReadResult> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let mut ranges = vec![];
        // for async read, try using table data cache (if enabled in settings)
        let column_data_cache = CacheManager::instance().get_table_data_cache();
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();
        let mut cached_column_data = vec![];
        let mut cached_column_array = vec![];
        for (_index, (column_id, ..)) in self.project_indices.iter() {
            let column_cache_key = TableDataCacheKey::new(location, *column_id);

            // first, check column array object cache
            if let Some(cache_array) = column_array_cache.get(&column_cache_key) {
                cached_column_array.push((*column_id, cache_array));
                continue;
            }

            // and then, check column data cache
            if let Some(cached_column_raw_data) = column_data_cache.get(&column_cache_key) {
                cached_column_data.push((*column_id, cached_column_raw_data));
                continue;
            }

            // if all cache missed, prepare the ranges to be read
            if let Some(column_meta) = columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();
                ranges.push((*column_id, offset..(offset + len)));

                // Perf
                {
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }
            }
        }

        let mut merge_io_read_res =
            Self::merge_io_read(settings, self.operator.clone(), location, ranges).await?;

        merge_io_read_res.cached_column_data = cached_column_data;
        merge_io_read_res.cached_column_array = cached_column_array;
        Ok(merge_io_read_res)
    }

    #[inline]
    #[async_backtrace::framed]
    pub async fn read_range(
        op: Operator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.range_read(path, start..end).await?;
        Ok((index, chunk))
    }
}
