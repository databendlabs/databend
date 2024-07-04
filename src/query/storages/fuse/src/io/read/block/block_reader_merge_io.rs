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
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::TableDataCache;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_cache_manager::SizedColumnArray;
use enum_as_inner::EnumAsInner;

type ChunkIndex = usize;
pub struct OwnerMemory {
    chunks: HashMap<ChunkIndex, Bytes>,
}

impl OwnerMemory {
    pub fn create(chunks: Vec<(ChunkIndex, Vec<u8>)>) -> OwnerMemory {
        let chunks = chunks
            .into_iter()
            .map(|(idx, chunk)| (idx, Bytes::from(chunk)))
            .collect();
        OwnerMemory { chunks }
    }

    pub fn get_chunk(&self, index: ChunkIndex, path: &str) -> Result<Bytes> {
        match self.chunks.get(&index) {
            Some(chunk) => Ok(chunk.clone()),
            None => Err(ErrorCode::Internal(format!(
                "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                index, path
            ))),
        }
    }
}

type CachedColumnData = Vec<(ColumnId, Arc<Bytes>)>;
type CachedColumnArray = Vec<(ColumnId, Arc<SizedColumnArray>)>;
type ScalarColumns = Vec<(ColumnId, Scalar)>;
pub struct MergeIOReadResult {
    block_path: String,
    columns_chunk_offsets: HashMap<ColumnId, (ChunkIndex, Range<usize>)>,
    owner_memory: OwnerMemory,
    pub cached_column_data: CachedColumnData,
    pub cached_column_array: CachedColumnArray,
    pub scalar_columns: ScalarColumns,
    table_data_cache: Option<TableDataCache>,
}

#[derive(EnumAsInner)]
pub enum DataItem<'a> {
    RawData(Bytes),
    ColumnArray(&'a Arc<SizedColumnArray>),
    Scalar(&'a Scalar),
}

impl MergeIOReadResult {
    pub fn create(
        owner_memory: OwnerMemory,
        capacity: usize,
        path: String,
        table_data_cache: Option<TableDataCache>,
    ) -> MergeIOReadResult {
        MergeIOReadResult {
            block_path: path,
            columns_chunk_offsets: HashMap::with_capacity(capacity),
            owner_memory,
            cached_column_data: vec![],
            cached_column_array: vec![],
            scalar_columns: vec![],
            table_data_cache,
        }
    }

    pub fn columns_chunks(&self) -> Result<HashMap<ColumnId, DataItem>> {
        let mut res = HashMap::with_capacity(self.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.columns_chunk_offsets {
            let chunk = self.owner_memory.get_chunk(*chunk_idx, &self.block_path)?;
            res.insert(*column_id, DataItem::RawData(chunk.slice(range.clone())));
        }

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            let data = data.as_ref();
            res.insert(*column_id, DataItem::RawData(data.clone()));
        }

        // merge column array from cache
        for (column_id, data) in &self.cached_column_array {
            res.insert(*column_id, DataItem::ColumnArray(data));
        }

        for (column_id, data) in &self.scalar_columns {
            res.insert(*column_id, DataItem::Scalar(data));
        }

        Ok(res)
    }

    pub fn column_buffers(&self) -> Result<HashMap<ColumnId, Bytes>> {
        let mut res = HashMap::with_capacity(self.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.columns_chunk_offsets {
            let chunk = self.owner_memory.get_chunk(*chunk_idx, &self.block_path)?;
            res.insert(*column_id, chunk.slice(range.clone()));
        }

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            let data = data.as_ref();
            res.insert(*column_id, data.clone());
        }

        Ok(res)
    }

    fn get_chunk(&self, index: usize, path: &str) -> Result<Bytes> {
        self.owner_memory.get_chunk(index, path)
    }

    pub fn add_column_chunk(
        &mut self,
        chunk_index: usize,
        column_id: ColumnId,
        column_range: Range<u64>,
        range: Range<usize>,
    ) {
        if let Some(table_data_cache) = &self.table_data_cache {
            // populate raw column data cache (compressed raw bytes)
            if let Ok(chunk_data) = self.get_chunk(chunk_index, &self.block_path) {
                let cache_key = TableDataCacheKey::new(
                    &self.block_path,
                    column_id,
                    column_range.start,
                    column_range.end - column_range.start,
                );
                let data = chunk_data.slice(range.clone());
                table_data_cache.put(cache_key.as_ref().to_owned(), Arc::new(data));
            }
        }
        self.columns_chunk_offsets
            .insert(column_id, (chunk_index, range));
    }
}
