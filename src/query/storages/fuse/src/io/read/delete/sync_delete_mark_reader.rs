// Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::parquet::compression::Compression;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageMetaData;
use common_arrow::parquet::read::PageReader;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::ValueType;
use common_expression::Column;
use opendal::Operator;
use storages_common_cache::CacheKey;
use storages_common_cache::LoadParams;
use storages_common_cache_manager::DeleteMarkMeta;
use storages_common_table_meta::meta::DeleteMaskVersion;
use storages_common_table_meta::meta::Location;

use crate::io::read::delete::delete_mark_reader::DeleteMetaLoader;

// TODO 1. code duplication 2. cache
pub trait DeleteMarkSyncReader {
    fn sync_read_delete_mark(
        &self,
        dal: Operator,
        size: u64,
        num_rows: usize,
    ) -> Result<Arc<Bitmap>>;
}

impl DeleteMarkSyncReader for Location {
    fn sync_read_delete_mark(
        &self,
        dal: Operator,
        size: u64,
        num_rows: usize,
    ) -> Result<Arc<Bitmap>> {
        let (path, ver) = &self;
        let mask_version = DeleteMaskVersion::try_from(*ver)?;
        if matches!(mask_version, DeleteMaskVersion::V0(_)) {
            let res = sync_load_delete_mark(dal, path, size, num_rows)?;
            Ok(res)
        } else {
            unreachable!()
        }
    }
}

/// load delete mark data
#[tracing::instrument(level = "debug", skip_all)]
fn sync_load_delete_mark(
    dal: Operator,
    path: &str,
    size: u64,
    num_rows: usize,
) -> Result<Arc<Bitmap>> {
    // 1. load delete mark meta
    let delete_mark_meta = sync_load_mark_meta(dal.clone(), path, size)?;
    let file_meta = &delete_mark_meta.0;
    if file_meta.row_groups.len() != 1 {
        return Err(ErrorCode::StorageOther(format!(
            "invalid delete mark meta, number of row group should be 1, but found {} row groups",
            file_meta.row_groups.len()
        )));
    }

    // 2. load delete mark data
    let index_column_chunk_metas = file_meta.row_groups[0].columns();
    assert_eq!(index_column_chunk_metas.len(), 1);
    let column_meta = &index_column_chunk_metas[0];
    let marks = sync_load_delete_mark_data(column_meta, path, &dal, num_rows)?;

    Ok(marks)
}

/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
fn sync_load_delete_mark_data<'a>(
    col_chunk_meta: &'a ColumnChunkMetaData,
    path: &'a str,
    dal: &'a Operator,
    num_rows: usize,
) -> Result<Arc<Bitmap>> {
    // TODO cache
    let meta = col_chunk_meta.metadata();
    let location = path.to_string();
    let loader = DeleteMetaLoader {
        num_rows,
        offset: meta.data_page_offset as u64,
        len: meta.total_compressed_size as u64,
        cache_key: location.clone(),
        operator: dal.clone(),
        column_descriptor: col_chunk_meta.descriptor().clone(),
    };

    // let cached_reader = CachedReader::new(Bitmap::cache(), loader);

    let param = LoadParams {
        location,
        len_hint: None,
        ver: 0,
    };
    // let bytes = cached_reader.read(&param)?;
    let bytes = loader.sync_load(&param)?;
    Ok(Arc::new(bytes))
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[tracing::instrument(level = "debug", skip_all)]
fn sync_load_mark_meta(dal: Operator, path: &str, size: u64) -> Result<Arc<DeleteMarkMeta>> {
    // TODO cache
    let object = dal.object(path);
    let mut reader = object.blocking_range_reader(0..size)?;

    use common_arrow::parquet::read::read_metadata;
    let meta = read_metadata(&mut reader).map_err(|err| {
        ErrorCode::Internal(format!("read file meta failed, {}, {:?}", path, err))
    })?;

    Ok(Arc::new(DeleteMarkMeta(meta)))
}

/// Loads an object from storage
#[async_trait::async_trait]
pub trait SyncLoader<T> {
    /// Loads object of type T, located by [params][LoadParams].
    fn sync_load(&self, params: &LoadParams) -> Result<T>;

    /// the [CacheKey] returns will be used as the key of cached item.
    fn cache_key(&self, params: &LoadParams) -> CacheKey {
        params.location.clone()
    }
}

impl SyncLoader<Bitmap> for DeleteMetaLoader {
    fn sync_load(&self, params: &LoadParams) -> Result<Bitmap> {
        let reader = self.operator.object(&params.location);
        let bytes = reader.blocking_range_read(self.offset..self.offset + self.len)?;

        let page_meta_data = PageMetaData {
            column_start: 0,
            num_values: self.num_rows as i64,
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
        let field = ArrowField::new(filed_name, DataType::Boolean, false);
        let mut array_iter = column_iter_to_arrays(
            vec![decompressor],
            vec![&column_type],
            field,
            None,
            self.num_rows,
        )?;
        if let Some(array) = array_iter.next() {
            let array = array?;
            let col =
                Column::from_arrow(array.as_ref(), &common_expression::types::DataType::Boolean);

            let bitmap = BooleanType::try_downcast_column(&col).ok_or_else(|| {
                ErrorCode::Internal(
                    "unexpected exception: load delete mark raw data as boolean failed",
                )
            })?;

            Ok(bitmap)
        } else {
            Err(ErrorCode::StorageOther(
                "delete mark data not available as expected",
            ))
        }
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
