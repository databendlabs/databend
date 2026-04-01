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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use bytes::Bytes;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use futures_util::future::try_join_all;
use opendal::Operator;
use parquet::arrow::ArrowSchemaConverter;
use parquet::format::FileMetaData;
use parquet::schema::types::SchemaDescPtr;
use parquet::thrift::TSerializable;
use thrift::protocol::TCompactInputProtocol;

use crate::index::filters::BlockBloomFilterIndexVersion;
use crate::index::filters::BlockFilter;
use crate::io::MetaReaders;
use crate::io::read::bloom::column_filter_reader::BloomColumnFilterReader;

#[async_trait::async_trait]
pub trait BloomBlockFilterReader {
    async fn read_block_filter(
        &self,
        dal: Operator,
        settings: &ReadSettings,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockFilter>;
}

#[async_trait::async_trait]
impl BloomBlockFilterReader for Location {
    // NOTE that the `columns` will be de-duplicated first,
    // and the filters contained by the returned `BlockFilter` may not order by `columns`
    #[async_backtrace::framed]
    async fn read_block_filter(
        &self,
        dal: Operator,
        settings: &ReadSettings,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockFilter> {
        let (path, ver) = &self;
        let index_version = BlockBloomFilterIndexVersion::try_from(*ver)?;
        match index_version {
            BlockBloomFilterIndexVersion::V0(_) => Err(ErrorCode::DeprecatedIndexFormat(
                "bloom filter index version(v0) is deprecated",
            )),
            BlockBloomFilterIndexVersion::V2(_)
            | BlockBloomFilterIndexVersion::V3(_)
            | BlockBloomFilterIndexVersion::V4(_) => {
                let res = load_bloom_filter_by_columns(dal, settings, columns, path, index_length)
                    .await?;
                Ok(res)
            }
        }
    }
}

/// load index column data
#[fastrace::trace]
pub async fn load_bloom_filter_by_columns<'a>(
    dal: Operator,
    settings: &ReadSettings,
    column_needed: &'a [String],
    index_path: &'a str,
    index_length: u64,
) -> Result<BlockFilter> {
    let whole_file =
        prepare_bloom_index_read_source(&dal, settings, index_path, index_length).await?;

    // 1. load index meta
    let bloom_index_meta =
        load_index_meta(dal.clone(), index_path, index_length, whole_file.as_ref()).await?;

    // 2. filter out columns that needed and exist in the index
    // 2.1 dedup the columns
    let column_needed: HashSet<&String> = HashSet::from_iter(column_needed);
    // 2.2 collects the column metas and their column ids
    let index_column_chunk_metas = &bloom_index_meta.columns;
    let mut col_metas = BTreeMap::new();
    for column_name in column_needed {
        for (idx, (name, column_meta)) in index_column_chunk_metas.iter().enumerate() {
            if name == column_name {
                col_metas.insert(idx as ColumnId, (name, column_meta));
                break;
            }
        }
    }

    // 3. load filters
    let bloom_index_fields: Vec<_> = bloom_index_meta
        .columns
        .iter()
        .map(|col| Field::new(col.0.clone(), arrow::datatypes::DataType::Binary, false))
        .collect();
    let bloom_index_schema = Schema::new(Fields::from(bloom_index_fields));
    let bloom_index_schema_desc =
        Arc::new(ArrowSchemaConverter::new().convert(&bloom_index_schema)?);

    let futs = col_metas
        .iter()
        .map(|(idx, (name, col_chunk_meta))| {
            load_column_bloom_filter(
                *idx,
                name,
                col_chunk_meta,
                index_path,
                &dal,
                bloom_index_schema_desc.clone(),
                whole_file.as_ref(),
            )
        })
        .collect::<Vec<_>>();

    let filters = try_join_all(futs).await?.into_iter().collect();

    // 4. build index schema
    let fields = col_metas
        .iter()
        .map(|(_, (name, _col_chunk_mea))| TableField::new(name, TableDataType::Binary))
        .collect();

    let filter_schema = TableSchema::new(fields);

    Ok(BlockFilter {
        filter_schema: Arc::new(filter_schema),
        filters,
    })
}

/// Loads bytes and index of the given column.
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
async fn load_column_bloom_filter<'a>(
    idx: ColumnId,
    filter_name: &str,
    col_chunk_meta: &'a SingleColumnMeta,
    index_path: &'a str,
    dal: &'a Operator,
    bloom_index_schema_desc: SchemaDescPtr,
    whole_file: Option<&Arc<Bytes>>,
) -> Result<Arc<FilterImpl>> {
    let storage_runtime = GlobalIORuntime::instance();
    let bytes = {
        let column_data_reader = BloomColumnFilterReader::new(
            index_path.to_owned(),
            idx,
            filter_name,
            col_chunk_meta,
            dal.clone(),
            bloom_index_schema_desc,
        );
        let whole_file = whole_file.cloned();
        async move { column_data_reader.read(whole_file.as_ref()).await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(bytes)
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub async fn load_index_meta(
    dal: Operator,
    path: &str,
    length: u64,
    whole_file: Option<&Arc<Bytes>>,
) -> Result<Arc<BloomIndexMeta>> {
    if let Some(whole_file) = whole_file {
        let meta = load_index_meta_from_bytes(whole_file)?;
        if let Some(cache) = BloomIndexMeta::cache() {
            return Ok(cache.insert(path.to_owned(), meta));
        }
        return Ok(Arc::new(meta));
    }

    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::bloom_index_meta_reader(dal);
        // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
        // so we just pass a zero to reader
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: Some(length),
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

async fn prepare_bloom_index_read_source(
    dal: &Operator,
    settings: &ReadSettings,
    path: &str,
    index_length: u64,
) -> Result<Option<Arc<Bytes>>> {
    if should_read_whole_bloom_index(path, settings, index_length) {
        let data = dal.read(path).await?;
        return Ok(Some(Arc::new(data.to_bytes())));
    }

    Ok(None)
}

fn should_read_whole_bloom_index(path: &str, settings: &ReadSettings, index_length: u64) -> bool {
    if index_length > settings.parquet_fast_read_bytes {
        return false;
    }

    !BloomIndexMeta::cache().is_some_and(|cache| cache.contains_key(path))
}

fn load_index_meta_from_bytes(data: &Bytes) -> Result<BloomIndexMeta> {
    const HEADER_SIZE: usize = 4;
    const FOOTER_SIZE: usize = 8;
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

    if data.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(ErrorCode::StorageOther(
            "read bloom index meta failed, A parquet file must contain a header and footer with at least 12 bytes"
                .to_string(),
        ));
    }

    if data[data.len() - PARQUET_MAGIC.len()..] != PARQUET_MAGIC {
        return Err(ErrorCode::StorageOther(
            "read bloom index meta failed, Invalid Parquet file. Corrupt footer".to_string(),
        ));
    }

    let metadata_len = i32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap());
    let metadata_len: u64 = metadata_len.try_into()?;
    let footer_len = FOOTER_SIZE as u64 + metadata_len;
    if footer_len > data.len() as u64 {
        return Err(ErrorCode::StorageOther(
            "read bloom index meta failed, The footer size must be smaller or equal to the file's size"
                .to_string(),
        ));
    }

    let remaining = data.len() - footer_len as usize;
    let mut prot = TCompactInputProtocol::new(&data[remaining..]);
    let thrift_meta = FileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|err| ErrorCode::StorageOther(format!("read bloom index meta failed, {err}")))?;
    BloomIndexMeta::try_from(thrift_meta)
}

#[async_trait::async_trait]
trait InRuntime
where Self: Future
{
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<Self::Output>;
}

#[async_trait::async_trait]
impl<T> InRuntime for T
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[async_backtrace::framed]
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
        runtime
            .spawn(self)
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::OnceLock;

    use databend_common_base::base::GlobalInstance;
    use databend_common_base::runtime::GlobalIORuntime;
    use databend_common_config::CacheConfig;
    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::Int32Type;
    use databend_storages_common_blocks::blocks_to_parquet;
    use databend_storages_common_cache::CacheAccessor;
    use databend_storages_common_cache::CacheManager;
    use databend_storages_common_cache::CachedObject;
    use databend_storages_common_index::BloomIndex;
    use databend_storages_common_index::BloomIndexBuilder;
    use databend_storages_common_index::BloomIndexType;
    use databend_storages_common_index::filters::BlockFilter;
    use databend_storages_common_table_meta::meta::Versioned;
    use databend_storages_common_table_meta::table::TableCompression;

    use super::*;

    fn init_test_globals() -> Result<()> {
        static INIT: OnceLock<Result<()>> = OnceLock::new();
        INIT.get_or_init(|| {
            GlobalInstance::init_production();
            GlobalIORuntime::init(1)?;
            CacheManager::init(&CacheConfig::default(), &(1024 * 1024), "test", false)
        })
        .clone()
    }

    fn write_bloom_index_bytes() -> Result<(Vec<u8>, String)> {
        let schema = TableSchema::new(vec![TableField::new(
            "y",
            TableDataType::Number(databend_common_expression::types::NumberDataType::Int32),
        )]);
        let field = schema.field_with_name("y")?;
        let bloom_columns_map = BTreeMap::from([(0usize, field.clone())]);
        let mut builder = BloomIndexBuilder::create(
            FunctionContext::default(),
            BloomIndexType::default(),
            bloom_columns_map,
            &[],
        )?;
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3, 4])]);
        builder.add_block(&block)?;
        let bloom_index = builder.finalize()?.unwrap();

        let index_block = bloom_index.serialize_to_data_block()?;
        let mut data = Vec::new();
        let _ = blocks_to_parquet(
            &bloom_index.filter_schema,
            vec![index_block],
            &mut data,
            TableCompression::None,
            false,
            None,
        )?;

        let filter_name = BloomIndex::build_filter_bloom_name(
            BlockFilter::VERSION,
            schema.field_with_name("y")?,
        )?;
        Ok((data, filter_name))
    }

    #[test]
    fn test_should_read_whole_bloom_index_by_threshold_and_cache() -> Result<()> {
        init_test_globals()?;
        let path = "bloom_index";
        let settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 0,
            parquet_fast_read_bytes: 1024,
        };

        if let Some(cache) = BloomIndexMeta::cache() {
            cache.evict(path);
        }
        assert!(should_read_whole_bloom_index(path, &settings, 512));
        assert!(!should_read_whole_bloom_index(path, &settings, 2048));

        if let Some(cache) = BloomIndexMeta::cache() {
            cache.insert(path.to_string(), BloomIndexMeta { columns: vec![] });
        }
        assert!(!should_read_whole_bloom_index(path, &settings, 512));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_load_bloom_filter_by_columns_from_small_whole_file() -> Result<()> {
        init_test_globals()?;

        let (data, filter_name) = write_bloom_index_bytes()?;
        let operator = opendal::Operator::via_iter(opendal::Scheme::Memory, [])?;
        let path = "bloom_index";
        operator.write(path, data.clone()).await?;

        if let Some(cache) = BloomIndexMeta::cache() {
            cache.evict(path);
        }
        if let Some(cache) = FilterImpl::cache() {
            cache.evict(&format!("{path}-{filter_name}"));
        }

        let settings = ReadSettings {
            max_gap_size: 0,
            max_range_size: 0,
            parquet_fast_read_bytes: data.len() as u64,
        };

        let block_filter = load_bloom_filter_by_columns(
            operator,
            &settings,
            &[filter_name.clone()],
            path,
            data.len() as u64,
        )
        .await?;

        assert_eq!(block_filter.filter_schema.num_fields(), 1);
        assert_eq!(block_filter.filter_schema.fields()[0].name(), &filter_name);
        assert_eq!(block_filter.filters.len(), 1);
        Ok(())
    }
}
