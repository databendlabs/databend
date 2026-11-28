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

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_cache::CacheRef;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_cache::UnitsRead;
use databend_storages_common_io::MergeIOReadResult;
use enum_as_inner::EnumAsInner;
use opendal::Buffer;

/// Column chunks read from the disk cache.
type DiskColumnData = Vec<(ColumnId, Bytes)>;
type CachedColumnArray = Vec<(ColumnId, CacheRef<SizedColumnArray>)>;

#[derive(EnumAsInner, Clone)]
pub enum DataItem<'a> {
    RawData(Buffer),
    GranuleData(Buffer, std::ops::Range<u64>),
    ColumnArray(&'a CacheRef<SizedColumnArray>),
}

/// The column chunks of one block: from storage, from the disk cache, and from the memory cache
/// of deserialized columns. A split block reads from several files, one storage read each.
pub struct BlockReadResult {
    merge_io_results: Vec<MergeIOReadResult>,
    pub(crate) disk_column_data: DiskColumnData,
    pub(crate) cached_column_array: CachedColumnArray,
    row_range: Option<std::ops::Range<usize>>,
    column_data_ranges: HashMap<ColumnId, std::ops::Range<u64>>,
}

/// The chunks `read_units` read, with no deserialized columns.
impl From<UnitsRead> for BlockReadResult {
    fn from(units: UnitsRead) -> Self {
        BlockReadResult {
            merge_io_results: vec![units.fetched],
            disk_column_data: units.cached.into_iter().collect(),
            cached_column_array: vec![],
            row_range: None,
            column_data_ranges: HashMap::new(),
        }
    }
}

impl BlockReadResult {
    pub fn with_column_arrays(mut self, arrays: CachedColumnArray) -> Self {
        self.cached_column_array = arrays;
        self
    }

    pub fn create_with_row_range(
        merge_io_result: MergeIOReadResult,
        cached_column_array: CachedColumnArray,
        column_data_ranges: HashMap<ColumnId, std::ops::Range<u64>>,
        row_range: std::ops::Range<usize>,
    ) -> BlockReadResult {
        BlockReadResult {
            merge_io_results: vec![merge_io_result],
            disk_column_data: vec![],
            cached_column_array,
            row_range: Some(row_range),
            column_data_ranges,
        }
    }

    pub fn row_range(&self) -> Option<&std::ops::Range<usize>> {
        self.row_range.as_ref()
    }

    /// Combines the reads of the files of one block. The files store disjoint columns.
    pub fn merge(results: Vec<BlockReadResult>) -> Self {
        let mut merged = BlockReadResult {
            merge_io_results: Vec::with_capacity(results.len()),
            disk_column_data: vec![],
            cached_column_array: vec![],
            row_range: None,
            column_data_ranges: HashMap::new(),
        };
        for result in results {
            debug_assert!(result.row_range.is_none());
            debug_assert!(result.column_data_ranges.is_empty());
            merged.merge_io_results.extend(result.merge_io_results);
            merged.disk_column_data.extend(result.disk_column_data);
            merged
                .cached_column_array
                .extend(result.cached_column_array);
        }
        merged
    }

    fn remote_chunks_len(&self) -> usize {
        self.merge_io_results
            .iter()
            .map(|r| r.columns_chunk_offsets.len())
            .sum()
    }

    pub fn columns_chunks(&self) -> Result<HashMap<ColumnId, DataItem<'_>>> {
        let mut res = HashMap::with_capacity(self.remote_chunks_len());

        // merge column data fetched from object storage
        for merge_io_result in &self.merge_io_results {
            for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
                let chunk = merge_io_result
                    .owner_memory
                    .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
                let data = chunk.slice(range.clone());
                let item = match self.column_data_ranges.get(column_id) {
                    Some(range) => DataItem::GranuleData(data, range.clone()),
                    None => DataItem::RawData(data),
                };
                res.insert(*column_id, item);
            }
        }

        // merge column data from the disk cache
        for (column_id, data) in &self.disk_column_data {
            res.insert(*column_id, DataItem::RawData(data.clone().into()));
        }

        // merge column array from cache
        for (column_id, data) in &self.cached_column_array {
            res.insert(*column_id, DataItem::ColumnArray(data));
        }

        Ok(res)
    }

    pub fn column_buffers(&self) -> Result<HashMap<ColumnId, Bytes>> {
        let mut res = HashMap::with_capacity(self.remote_chunks_len());

        // merge column data fetched from object storage
        for merge_io_result in &self.merge_io_results {
            for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
                let chunk = merge_io_result
                    .owner_memory
                    .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
                res.insert(*column_id, chunk.slice(range.clone()).to_bytes());
            }
        }

        // merge column data from the disk cache
        for (column_id, data) in &self.disk_column_data {
            res.insert(*column_id, data.clone());
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use databend_storages_common_io::OwnerMemory;

    use super::*;

    #[test]
    fn test_granule_data_keeps_file_range() {
        let merge = MergeIOReadResult::create(
            OwnerMemory::create(vec![(0, Buffer::from(vec![1, 2, 3]))]),
            HashMap::from([(7, (0, 0..3))]),
            "block".to_string(),
        );
        let result = BlockReadResult::create_with_row_range(
            merge,
            vec![],
            HashMap::from([(7, 100..103)]),
            0..3,
        );

        let chunks = result.columns_chunks().unwrap();
        assert!(matches!(
            chunks.get(&7),
            Some(DataItem::GranuleData(data, range))
                if data.to_bytes().as_ref() == [1, 2, 3] && *range == (100..103)
        ));
    }
}
