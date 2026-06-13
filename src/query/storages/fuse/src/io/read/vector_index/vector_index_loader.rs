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
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_read_milliseconds;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::VectorIndexFileCache;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

use crate::index::VectorIndexFile;
use crate::io::MetaReaders;
use crate::io::read::index_loader::IndexLoadSpec;
use crate::io::read::index_loader::MetaRead;
use crate::io::read::index_loader::load_index_files;
use crate::io::read::index_loader::load_index_meta;

struct VectorIndexLoadSpec;

impl IndexLoadSpec for VectorIndexLoadSpec {
    type Meta = VectorIndexMeta;
    type File = VectorIndexFile;
    type FileCache = Option<VectorIndexFileCache>;

    fn meta_reader(dal: Operator) -> Box<dyn MetaRead<Self::Meta> + Send + Sync> {
        Box::new(MetaReaders::vector_index_meta_reader(dal))
    }

    fn file_cache() -> Self::FileCache {
        CacheManager::instance().get_vector_index_file_cache()
    }

    fn meta_columns(meta: &Self::Meta) -> &Vec<(String, SingleColumnMeta)> {
        &meta.columns
    }

    fn make_file(name: String, data: Bytes) -> Self::File {
        VectorIndexFile::create(name, data)
    }

    fn file_data(file: &Self::File) -> &Bytes {
        &file.data
    }

    fn arrow_data_type(_column_name: &str) -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Binary
    }

    fn column_data_type(_column_name: &str) -> DataType {
        DataType::Binary
    }

    fn record_metrics(bytes: u64, ms: u64) {
        metrics_inc_block_vector_index_read_bytes(bytes);
        metrics_inc_block_vector_index_read_milliseconds(ms);
    }
}

/// Loads vector index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub async fn load_vector_index_meta(dal: Operator, path: &str) -> Result<Arc<VectorIndexMeta>> {
    load_index_meta::<VectorIndexLoadSpec>(dal, path).await
}

/// load index column data
#[fastrace::trace]
pub async fn load_vector_index_files(
    operator: Operator,
    settings: &ReadSettings,
    column_names: &[String],
    location: &str,
) -> Result<Vec<Column>> {
    load_index_files::<VectorIndexLoadSpec>(operator, settings, column_names, location).await
}
