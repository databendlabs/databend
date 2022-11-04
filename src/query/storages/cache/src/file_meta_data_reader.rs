//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::parquet::metadata::FileMetaData;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use opendal::Operator;

use super::cached_reader::CachedReader;
use super::cached_reader::Loader;

pub type FileMetaDataReader = CachedReader<FileMetaData, Operator>;

impl FileMetaDataReader {
    pub fn new_reader(dal: Operator) -> FileMetaDataReader {
        FileMetaDataReader::new(
            CacheManager::instance().get_file_meta_data_cache(),
            "FILE_META_DATA_CACHE".to_owned(),
            dal,
        )
    }
}

#[async_trait::async_trait]
impl Loader<FileMetaData> for Operator {
    async fn load(
        &self,
        key: &str,
        length_hint: Option<u64>,
        _version: u64,
    ) -> Result<FileMetaData> {
        let object = self.object(key);
        let mut reader = if let Some(len) = length_hint {
            object.seekable_reader(..len)
        } else {
            object.seekable_reader(..)
        };
        read_metadata_async(&mut reader).await.map_err(|err| {
            ErrorCode::Internal(format!("read file meta failed, {}, {:?}", key, err))
        })
    }
}
