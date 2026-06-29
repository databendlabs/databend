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

use crate::BlockReadResult;
use crate::io::BlockReadContext;
use crate::io::BlockReadPlan;
use crate::io::BlockReader;
use crate::io::ColumnReadPlan;

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
    /// Read only the byte ranges of the projected columns covered by a sparse-page-index
    /// [`BlockReadPlan`]. Each projected leaf column is fetched as its dictionary page (if any)
    /// plus the single contiguous data-page range for the selected granules, then reassembled
    /// into a `[dict bytes] ++ [data bytes]` buffer — a valid standalone parquet column chunk.
    ///
    /// Skips all caches: the byte ranges depend on the query predicate, so they are not stable
    /// cache keys. The result carries the narrowed row count via `num_rows_override`.
    #[async_backtrace::framed]
    pub async fn read_columns_data_by_page_index(
        &self,
        settings: &ReadSettings,
        location: &str,
        plan: &BlockReadPlan,
    ) -> Result<BlockReadResult> {
        metrics_inc_remote_io_read_parts(1);

        // Index plan columns by id so we can look up the projected ones.
        let plan_by_id: HashMap<ColumnId, &ColumnReadPlan> =
            plan.columns.iter().map(|c| (c.column_id, c)).collect();

        // Assign each (column -> dict range, data range) a synthetic id so a single merged read
        // can fetch them all; reassemble per real column afterwards. `dict_id = 2*i`,
        // `data_id = 2*i + 1`.
        let mut ranges: Vec<(ColumnId, std::ops::Range<u64>)> = Vec::new();
        let mut layout: Vec<(ColumnId, ColumnId, Option<ColumnId>)> = Vec::new();
        let mut total_len: u64 = 0;
        for (i, (_field_index, (column_id, ..))) in self.project_indices().iter().enumerate() {
            let Some(col_plan) = plan_by_id.get(column_id) else {
                continue;
            };
            let data_id = (i as ColumnId) * 2 + 1;
            let dict_id = (i as ColumnId) * 2;
            let dict_synth = col_plan.dict_range.as_ref().map(|r| {
                ranges.push((dict_id, r.clone()));
                total_len += r.end - r.start;
                dict_id
            });
            ranges.push((data_id, col_plan.data_range.clone()));
            total_len += col_plan.data_range.end - col_plan.data_range.start;
            layout.push((*column_id, data_id, dict_synth));
        }

        metrics_inc_remote_io_read_bytes(total_len);
        Profile::record_usize_profile(
            ProfileStatisticsName::ScanBytesFromRemote,
            total_len as usize,
        );

        let merge_io_result =
            MergeIOReader::merge_io_read(settings, self.operator().clone(), location, &ranges)
                .await?;

        // Reassemble `[dict] ++ [data]` per real column into a fresh owner memory, one chunk each.
        let mut chunks = Vec::with_capacity(layout.len());
        let mut columns_chunk_offsets = HashMap::with_capacity(layout.len());
        let fetch = |synth_id: ColumnId| -> Result<opendal::Buffer> {
            let (chunk_idx, range) = merge_io_result
                .columns_chunk_offsets
                .get(&synth_id)
                .ok_or_else(|| {
                    databend_common_exception::ErrorCode::Internal(format!(
                        "page index narrowed read missing synthetic range {synth_id}"
                    ))
                })?;
            let chunk = merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
            Ok(chunk.slice(range.clone()))
        };
        for (chunk_idx, (column_id, data_id, dict_synth)) in layout.into_iter().enumerate() {
            let data_buf = fetch(data_id)?;
            let buffer = if let Some(dict_id) = dict_synth {
                let dict_buf = fetch(dict_id)?;
                let mut bytes = Vec::with_capacity(dict_buf.len() + data_buf.len());
                bytes.extend_from_slice(&dict_buf.to_bytes());
                bytes.extend_from_slice(&data_buf.to_bytes());
                opendal::Buffer::from(bytes)
            } else {
                data_buf
            };
            let len = buffer.len();
            chunks.push((chunk_idx, buffer));
            columns_chunk_offsets.insert(column_id, (chunk_idx, 0..len));
        }

        let owner_memory = databend_storages_common_io::OwnerMemory::create(chunks);
        let narrowed = databend_storages_common_io::MergeIOReadResult::create(
            owner_memory,
            columns_chunk_offsets,
            location.to_string(),
        );
        Ok(BlockReadResult::create_with_num_rows(
            narrowed,
            plan.num_rows,
        ))
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
        let column_data_cache = CacheManager::instance().get_column_data_cache();
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();
        let mut cached_column_data = vec![];
        let mut cached_column_array = vec![];

        let column_cache_key_builder = ColumnCacheKeyBuilder::new(location);

        for (_index, (column_id, ..)) in self.project_indices().iter() {
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
            MergeIOReader::merge_io_read(settings, self.operator().clone(), location, &ranges)
                .await?;

        if self.put_cache() {
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
