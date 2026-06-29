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
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_io::MergeIOReadResult;
use enum_as_inner::EnumAsInner;
use opendal::Buffer;

type CachedColumnData = Vec<(ColumnId, Arc<ColumnData>)>;
type CachedColumnArray = Vec<(ColumnId, Arc<SizedColumnArray>)>;

#[derive(EnumAsInner, Clone)]
pub enum DataItem<'a> {
    RawData(Buffer),
    ColumnArray(&'a Arc<SizedColumnArray>),
}

pub struct BlockReadResult {
    merge_io_result: MergeIOReadResult,
    pub(crate) cached_column_data: CachedColumnData,
    pub(crate) cached_column_array: CachedColumnArray,
    /// When the read was narrowed to a contiguous run of sparse-page-index granules, the number of
    /// rows those granules contain (fewer than the block's total). `None` for a full-block read.
    /// The deserializer uses this to size the decoded batch and the prewhere filter bitmap.
    num_rows_override: Option<usize>,
}

impl BlockReadResult {
    pub fn create(
        merge_io_result: MergeIOReadResult,
        cached_column_data: CachedColumnData,
        cached_column_array: CachedColumnArray,
    ) -> BlockReadResult {
        BlockReadResult {
            merge_io_result,
            cached_column_data,
            cached_column_array,
            num_rows_override: None,
        }
    }

    pub fn create_with_num_rows(
        merge_io_result: MergeIOReadResult,
        num_rows: usize,
    ) -> BlockReadResult {
        BlockReadResult {
            merge_io_result,
            cached_column_data: vec![],
            cached_column_array: vec![],
            num_rows_override: Some(num_rows),
        }
    }

    /// Number of rows the narrowed read covers, when sparse-page-index skipping applied.
    pub fn num_rows_override(&self) -> Option<usize> {
        self.num_rows_override
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

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            res.insert(*column_id, DataItem::RawData(data.bytes().into()));
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

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            res.insert(*column_id, data.bytes());
        }

        Ok(res)
    }
}
