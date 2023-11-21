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

use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::parquet_rs::read_metadata_async;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use storages_common_cache::InMemoryItemCacheReader;
use storages_common_cache::LoadParams;
use storages_common_cache::Loader;
use storages_common_cache_manager::CacheManager;

pub struct LoaderWrapper<T>(T);
pub type FileMetaDataReader = InMemoryItemCacheReader<ParquetMetaData, LoaderWrapper<Operator>>;
pub struct MetaDataReader;

impl MetaDataReader {
    pub fn meta_data_reader(dal: Operator) -> FileMetaDataReader {
        FileMetaDataReader::new(
            CacheManager::instance().get_file_meta_data_cache(),
            LoaderWrapper(dal),
        )
    }
}

#[async_trait::async_trait]
impl Loader<ParquetMetaData> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<ParquetMetaData> {
        read_metadata_async(&params.location, &self.0, params.len_hint)
            .await
            .map_err(|err| {
                ErrorCode::Internal(format!(
                    "read file meta failed, {}, {:?}",
                    params.location, err
                ))
            })
    }
}
