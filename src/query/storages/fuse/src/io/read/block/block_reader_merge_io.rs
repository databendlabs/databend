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
    merge_io_results: Vec<MergeIOReadResult>,
    pub(crate) cached_column_data: CachedColumnData,
    pub(crate) cached_column_array: CachedColumnArray,
}

impl BlockReadResult {
    pub fn create(
        merge_io_result: MergeIOReadResult,
        cached_column_data: CachedColumnData,
        cached_column_array: CachedColumnArray,
    ) -> BlockReadResult {
        BlockReadResult {
            merge_io_results: vec![merge_io_result],
            cached_column_data,
            cached_column_array,
        }
    }

    pub(crate) fn merge(results: Vec<BlockReadResult>) -> BlockReadResult {
        let mut merge_io_results = Vec::with_capacity(results.len());
        let mut cached_column_data = vec![];
        let mut cached_column_array = vec![];

        for result in results {
            merge_io_results.extend(result.merge_io_results);
            cached_column_data.extend(result.cached_column_data);
            cached_column_array.extend(result.cached_column_array);
        }

        BlockReadResult {
            merge_io_results,
            cached_column_data,
            cached_column_array,
        }
    }

    pub fn columns_chunks(&self) -> Result<HashMap<ColumnId, DataItem<'_>>> {
        let capacity = self
            .merge_io_results
            .iter()
            .map(|result| result.columns_chunk_offsets.len())
            .sum();
        let mut res = HashMap::with_capacity(capacity);

        // merge column data fetched from object storage
        for merge_io_result in &self.merge_io_results {
            for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
                let chunk = merge_io_result
                    .owner_memory
                    .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
                res.insert(*column_id, DataItem::RawData(chunk.slice(range.clone())));
            }
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
}
