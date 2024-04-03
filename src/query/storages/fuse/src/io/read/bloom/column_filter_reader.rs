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

use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_cache::CacheKey;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_cache_manager::BloomIndexFilterMeter;
use databend_storages_common_cache_manager::CachedObject;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;
use parquet_rs::basic::Compression as ParquetCompression;
use parquet_rs::schema::types::SchemaDescPtr;

use crate::io::read::block::parquet::RowGroupImplBuilder;

type CachedReader = InMemoryCacheReader<Xor8Filter, Xor8FilterLoader, BloomIndexFilterMeter>;

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
        column_chunk_meta: &SingleColumnMeta,
        operator: Operator,
        schema_desc: SchemaDescPtr,
    ) -> Self {
        let cache_key = format!("{index_path}-{column_id}");

        let SingleColumnMeta {
            offset,
            len,
            num_values,
        } = column_chunk_meta;

        let loader = Xor8FilterLoader {
            cache_key,
            operator,
            offset: *offset,
            len: *len,
            num_values: *num_values,
            schema_desc,
            column_id,
        };

        let cached_reader = CachedReader::new(Xor8Filter::cache(), loader);

        let param = LoadParams {
            location: index_path,
            len_hint: None,
            ver: 0,
            put_cache: true,
        };

        BloomColumnFilterReader {
            cached_reader,
            param,
        }
    }

    #[async_backtrace::framed]
    pub async fn read(&self) -> Result<Arc<Xor8Filter>> {
        self.cached_reader.read(&self.param).await
    }
}

/// Loader that fetch range of the target object with customized cache key
pub struct Xor8FilterLoader {
    pub offset: u64,
    pub len: u64,
    pub num_values: u64,
    pub schema_desc: SchemaDescPtr,
    pub column_id: u32,
    pub cache_key: String,
    pub operator: Operator,
}

#[async_trait::async_trait]
impl Loader<Xor8Filter> for Xor8FilterLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<Xor8Filter> {
        let bytes = self
            .operator
            .read_with(&params.location)
            .range(self.offset..self.offset + self.len)
            .await?;
        let mut builder = RowGroupImplBuilder::new(
            self.num_values as usize,
            &self.schema_desc,
            ParquetCompression::UNCOMPRESSED,
        );
        builder.add_column_chunk(self.column_id as usize, Bytes::from(bytes));
        todo!()
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
