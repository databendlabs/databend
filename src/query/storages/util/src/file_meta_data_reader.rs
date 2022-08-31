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

use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::parquet::metadata::FileMetaData;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use futures_util::io::BufReader;

use super::cached_reader::CachedReader;
use super::cached_reader::Loader;

pub type FileMetaDataReader = CachedReader<FileMetaData, Arc<dyn TableContext>>;

impl FileMetaDataReader {
    pub fn new_reader(ctx: Arc<dyn TableContext>) -> FileMetaDataReader {
        FileMetaDataReader::new(
            CacheManager::instance().get_file_meta_data_cache(),
            ctx,
            "FILE_META_DATA_CACHE".to_owned(),
        )
    }
}

#[async_trait::async_trait]
impl Loader<FileMetaData> for Arc<dyn TableContext> {
    async fn load(
        &self,
        key: &str,
        _length_hint: Option<u64>,
        _version: u64,
    ) -> Result<FileMetaData> {
        // TODO use length hint
        let dal = self.get_storage_operator()?;
        let object = dal.object(key);
        let reader = object.seekable_reader(0..);
        let buffer_size = self.get_settings().get_storage_read_buffer_size()?;
        let mut buf_reader = BufReader::with_capacity(buffer_size as usize, reader);
        read_metadata_async(&mut buf_reader)
            .await
            .map_err(|err| ErrorCode::ParquetError(format!("read meta failed, {}, {:?}", key, err)))
    }
}
