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

use std::ops::Range;

use common_base::rangemap::RangeMerger;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache::TableDataCacheKey;
use storages_common_cache_manager::CacheManager;

use crate::fuse_part::FusePartInfo;
use crate::io::read::block::block_reader_merge_io::OwnerMemory;
use crate::io::read::ReadSettings;
use crate::io::BlockReader;
use crate::MergeIOReadResult;

impl BlockReader {
    pub fn sync_merge_io_read(
        read_settings: &ReadSettings,
        op: Operator,
        location: &str,
        raw_ranges: Vec<(ColumnId, Range<u64>)>,
    ) -> Result<MergeIOReadResult> {
        let path = location.to_string();

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
        let mut io_res = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            io_res.push(Self::sync_read_range(
                op.clone(),
                location,
                idx,
                range.start,
                range.end,
            )?);
        }

        let owner_memory = OwnerMemory::create(io_res);

        // for sync read, we disable table data cache
        let table_data_cache = None;
        let mut read_res = MergeIOReadResult::create(
            owner_memory,
            raw_ranges.len(),
            path.clone(),
            table_data_cache,
        );

        for (raw_idx, raw_range) in &raw_ranges {
            let column_id = *raw_idx as ColumnId;
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = range_merger.get(column_range.clone()).ok_or(ErrorCode::Internal(format!(
                "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                column_range, path, merged_ranges
            )))?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            read_res.add_column_chunk(merged_range_idx, column_id, start..end);
        }

        Ok(read_res)
    }

    pub fn sync_read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        part: PartInfoPtr,
    ) -> Result<MergeIOReadResult> {
        let part = FusePartInfo::from_part(&part)?;
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();

        let mut ranges = vec![];
        let mut cached_column_array = vec![];
        for (_index, (column_id, ..)) in self.project_indices.iter() {
            // first, check column array object cache
            let block_path = &part.location;
            let column_cache_key = TableDataCacheKey::new(block_path, *column_id);
            if let Some(cache_array) = column_array_cache.get(&column_cache_key) {
                cached_column_array.push((*column_id, cache_array));
                continue;
            }
            if let Some(column_meta) = part.columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();
                ranges.push((*column_id, offset..(offset + len)));
            }
        }

        let mut merge_io_result =
            Self::sync_merge_io_read(settings, self.operator.clone(), &part.location, ranges)?;
        merge_io_result.cached_column_array = cached_column_array;
        Ok(merge_io_result)
    }

    #[inline]
    pub fn sync_read_range(
        op: Operator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.blocking().range_read(path, start..end)?;
        Ok((index, chunk))
    }
}
