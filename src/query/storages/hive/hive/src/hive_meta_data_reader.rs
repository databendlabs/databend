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

use common_arrow::parquet::metadata::FileMetaData;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::caches::MemoryCacheReader;
use opendal::Operator;

pub type FileMetaDataReader = MemoryCacheReader<FileMetaData, Operator>;

pub struct MetaDataReader;

impl MetaDataReader {
    pub fn meta_data_reader(dal: Operator) -> FileMetaDataReader {
        FileMetaDataReader::new(
            CacheManager::instance().get_file_meta_data_cache(),
            "FILE_META_DATA_CACHE".to_owned(),
            dal,
        )
    }
}
