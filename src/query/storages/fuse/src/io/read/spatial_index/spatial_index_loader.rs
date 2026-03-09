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
use databend_common_expression::Column;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_read_milliseconds;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::SpatialIndexFileCache;
use databend_storages_common_index::SpatialIndexMeta;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

use crate::index::SpatialIndexFile;
use crate::io::MetaReaders;
use crate::io::read::index_loader::IndexLoadSpec;
use crate::io::read::index_loader::MetaRead;
use crate::io::read::index_loader::load_index_files;
use crate::io::read::index_loader::load_index_meta;

struct SpatialIndexLoadSpec;

impl IndexLoadSpec for SpatialIndexLoadSpec {
    type Meta = SpatialIndexMeta;
    type File = SpatialIndexFile;
    type FileCache = Option<SpatialIndexFileCache>;

    fn meta_reader(dal: Operator) -> Box<dyn MetaRead<Self::Meta> + Send + Sync> {
        Box::new(MetaReaders::spatial_index_meta_reader(dal))
    }

    fn file_cache() -> Self::FileCache {
        CacheManager::instance().get_spatial_index_file_cache()
    }

    fn meta_columns(meta: &Self::Meta) -> &Vec<(String, SingleColumnMeta)> {
        &meta.columns
    }

    fn make_file(name: String, data: Bytes) -> Self::File {
        SpatialIndexFile::create(name, data)
    }

    fn file_data(file: &Self::File) -> &Bytes {
        &file.data
    }

    fn arrow_data_type(column_name: &str) -> arrow::datatypes::DataType {
        if column_name.ends_with("_srid") {
            arrow::datatypes::DataType::Int32
        } else {
            arrow::datatypes::DataType::Binary
        }
    }

    fn column_data_type(column_name: &str) -> DataType {
        if column_name.ends_with("_srid") {
            DataType::Number(NumberDataType::Int32)
        } else {
            DataType::Binary
        }
    }

    fn record_metrics(bytes: u64, ms: u64) {
        metrics_inc_block_spatial_index_read_bytes(bytes);
        metrics_inc_block_spatial_index_read_milliseconds(ms);
    }
}

/// Loads spatial index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub async fn load_spatial_index_meta(dal: Operator, path: &str) -> Result<Arc<SpatialIndexMeta>> {
    load_index_meta::<SpatialIndexLoadSpec>(dal, path).await
}

/// load index column data
#[fastrace::trace]
pub async fn load_spatial_index_files(
    operator: Operator,
    settings: &ReadSettings,
    column_names: &[String],
    location: &str,
) -> Result<Vec<Column>> {
    load_index_files::<SpatialIndexLoadSpec>(operator, settings, column_names, location).await
}
