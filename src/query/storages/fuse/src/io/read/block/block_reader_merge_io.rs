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
    ColumnArray(&'a CacheRef<SizedColumnArray>),
}

/// The column chunks of one block: from storage, from the disk cache, and from the memory cache
/// of deserialized columns.
pub struct BlockReadResult {
    merge_io_result: MergeIOReadResult,
    pub(crate) disk_column_data: DiskColumnData,
    pub(crate) cached_column_array: CachedColumnArray,
}

/// The chunks `read_units` read, with no deserialized columns.
impl From<UnitsRead> for BlockReadResult {
    fn from(units: UnitsRead) -> Self {
        BlockReadResult {
            merge_io_result: units.fetched,
            disk_column_data: units.cached.into_iter().collect(),
            cached_column_array: vec![],
        }
    }
}

impl BlockReadResult {
    pub fn with_column_arrays(mut self, arrays: CachedColumnArray) -> Self {
        self.cached_column_array = arrays;
        self
    }

    pub fn columns_chunks(&self) -> Result<HashMap<ColumnId, DataItem<'_>>> {
        let mut res = HashMap::with_capacity(self.merge_io_result.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.merge_io_result.columns_chunk_offsets {
            let chunk = self
                .merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &self.merge_io_result.block_path)?;
            res.insert(*column_id, DataItem::RawData(chunk.slice(range.clone())));
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
        let mut res = HashMap::with_capacity(self.merge_io_result.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.merge_io_result.columns_chunk_offsets {
            let chunk = self
                .merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &self.merge_io_result.block_path)?;
            res.insert(*column_id, chunk.slice(range.clone()).to_bytes());
        }

        // merge column data from the disk cache
        for (column_id, data) in &self.disk_column_data {
            res.insert(*column_id, data.clone());
        }

        Ok(res)
    }
}
