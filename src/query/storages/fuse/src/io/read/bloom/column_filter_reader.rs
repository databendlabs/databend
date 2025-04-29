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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::metrics_inc_block_index_read_bytes;
use databend_storages_common_cache::CacheKey;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::HybridCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression as ParquetCompression;
use parquet::schema::types::SchemaDescPtr;

use crate::io::read::block::parquet::RowGroupImplBuilder;

type CachedReader = HybridCacheReader<FilterImpl, BloomFilterLoader>;

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

        let loader = BloomFilterLoader {
            cache_key,
            operator,
            offset: *offset,
            len: *len,
            num_values: *num_values,
            schema_desc,
            column_id,
        };

        let cached_reader = CachedReader::new(FilterImpl::cache(), loader);

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
    pub async fn read(&self) -> Result<Arc<FilterImpl>> {
        self.cached_reader.read(&self.param).await
    }
}

/// Loader that fetch range of the target object with customized cache key
pub struct BloomFilterLoader {
    pub offset: u64,
    pub len: u64,
    pub num_values: u64,
    pub schema_desc: SchemaDescPtr,
    pub column_id: u32,
    pub cache_key: String,
    pub operator: Operator,
}

#[async_trait::async_trait]
impl Loader<FilterImpl> for BloomFilterLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<FilterImpl> {
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
        builder.add_column_chunk(self.column_id as usize, bytes.to_bytes());
        let row_group = Box::new(builder.build());
        let field_levels = parquet_to_arrow_field_levels(
            self.schema_desc.as_ref(),
            ProjectionMask::leaves(&self.schema_desc, vec![self.column_id as usize]),
            None,
        )?;
        let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
            &field_levels,
            row_group.as_ref(),
            self.num_values as usize,
            None,
        )?;
        let record = record_reader.next().unwrap()?;
        assert!(record_reader.next().is_none());
        let bloom_filter_binary = record.column(0).clone();
        let col = Column::from_arrow_rs(
            bloom_filter_binary,
            &databend_common_expression::types::DataType::Binary,
        )?;
        let filter_bytes = col
            .as_binary()
            .expect("load bloom filter raw data as binary failed")
            .index(0)
            .unwrap();
        metrics_inc_block_index_read_bytes(filter_bytes.len() as u64);
        let (filter, _size) = FilterImpl::from_bytes(filter_bytes)?;
        Ok(filter)
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
