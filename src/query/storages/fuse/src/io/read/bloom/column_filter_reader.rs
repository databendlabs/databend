// Copyright 2022 Datafuse Labs.
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
//

use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Column;
use common_expression::ColumnId;
use opendal::Operator;
use storages_common_cache::CacheKey;
use storages_common_cache::InMemoryItemCacheReader;
use storages_common_cache::LoadParams;
use storages_common_cache::Loader;
use storages_common_cache_manager::CachedObject;
use storages_common_index::filters::Filter;
use storages_common_index::filters::Xor8Filter;

use crate::metrics::metrics_inc_block_index_read_bytes;

type CachedReader = InMemoryItemCacheReader<Xor8Filter, Xor8FilterLoader>;

/// Load the filter of a given bloom index column. Also
/// - generates the proper cache key
/// - takes cares of getting the correct cache instance from [CacheManager]
pub struct BloomColumnFilterReader {
    cached_reader: CachedReader,
    param: LoadParams,
}

impl BloomColumnFilterReader {
    pub fn new(
        index_path: String,
        column_id: ColumnId,
        column_chunk_meta: &ColumnChunkMetaData,
        operator: Operator,
    ) -> Self {
        let meta = column_chunk_meta.metadata();
        let cache_key = format!("{index_path}-{column_id}");
        let loader = Xor8FilterLoader {
            offset: meta.data_page_offset as u64,
            len: meta.total_compressed_size as u64,
            cache_key,
            operator,
            column_descriptor: column_chunk_meta.descriptor().clone(),
        };

        let cached_reader = CachedReader::new(Xor8Filter::cache(), loader);

        let param = LoadParams {
            location: index_path,
            len_hint: None,
            ver: 0,
        };

        BloomColumnFilterReader {
            cached_reader,
            param,
        }
    }

    pub async fn read(&self) -> Result<Arc<Xor8Filter>> {
        self.cached_reader.read(&self.param).await
    }
}

/// Loader that fetch range of the target object with customized cache key
pub struct Xor8FilterLoader {
    pub offset: u64,
    pub len: u64,
    pub cache_key: String,
    pub operator: Operator,
    pub column_descriptor: ColumnDescriptor,
}

#[async_trait::async_trait]
impl Loader<Xor8Filter> for Xor8FilterLoader {
    async fn load(&self, params: &LoadParams) -> Result<Xor8Filter> {
        let reader = self.operator.object(&params.location);
        let bytes = reader
            .range_read(self.offset..self.offset + self.len)
            .await?;

        let page_meta_data = PageMetaData {
            column_start: 0,
            num_values: 1,
            compression: Compression::Uncompressed,
            descriptor: self.column_descriptor.descriptor.clone(),
        };

        let page_reader = PageReader::new_with_page_meta(
            std::io::Cursor::new(bytes), /* we can not use &[u8] as Reader here, lifetime not valid */
            page_meta_data,
            Arc::new(|_, _| true),
            vec![],
            usize::MAX,
        );

        let decompressor = BasicDecompressor::new(page_reader, vec![]);
        let column_type = self.column_descriptor.descriptor.primitive_type.clone();
        let filed_name = self.column_descriptor.path_in_schema[0].to_owned();
        let field = ArrowField::new(filed_name, DataType::Binary, false);
        let mut array_iter =
            column_iter_to_arrays(vec![decompressor], vec![&column_type], field, None, 1)?;
        if let Some(array) = array_iter.next() {
            let array = array?;
            let col =
                Column::from_arrow(array.as_ref(), &common_expression::types::DataType::String);

            let filter_bytes = col
                .as_string()
                .map(|str| unsafe { str.index_unchecked(0) })
                .ok_or_else(|| {
                    // BloomPruner will log and handle this exception
                    ErrorCode::Internal(
                        "unexpected exception: load bloom filter raw data as string failed",
                    )
                })?;
            metrics_inc_block_index_read_bytes(filter_bytes.len() as u64);
            let (filter, _size) = Xor8Filter::from_bytes(filter_bytes)?;
            Ok(filter)
        } else {
            Err(ErrorCode::StorageOther(
                "bloom index data not available as expected",
            ))
        }
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
