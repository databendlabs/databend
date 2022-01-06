// Copyright 2021 Datafuse Labs.
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

use std::io::ErrorKind;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_dal::DataAccessor;
use common_dal::InputStream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use serde::de::DeserializeOwned;

use crate::sessions::QueryContext;
use crate::storages::cache::StorageCache;
use crate::storages::fuse::cache::local_cache_new::Loader;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

type MemCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;
pub struct CachedMetaReader<T, L> {
    cache: Arc<RwLock<MemCache<String, Arc<T>>>>,
    loader: L,
}

trait ReadProvider {
    fn reader(&self, path: &str, len: Option<u64>) -> Result<InputStream>;
}

impl ReadProvider for Arc<QueryContext> {
    fn reader(&self, path: &str, len: Option<u64>) -> Result<InputStream> {
        let accessor = self.get_storage_accessor()?;
        accessor.get_input_stream(path, len)
    }
}

pub struct FileMetaDataW(FileMetaData);

#[async_trait::async_trait]
impl<T> Loader<FileMetaDataW> for T
where T: ReadProvider + Sync
{
    async fn load(&self, key: &str) -> Result<FileMetaDataW> {
        let mut reader = self.reader(key, None)?; // TODO we know the length of stream
        let meta = read_metadata_async(&mut reader)
            .instrument(debug_span!("parquet_source_read_meta"))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        Ok(FileMetaDataW(meta))
    }
}

#[async_trait::async_trait]
impl<T, V> Loader<V> for T
where
    T: ReadProvider + Sync,
    V: DeserializeOwned,
{
    async fn load(&self, key: &str) -> Result<V> {
        let mut reader = self.reader(key, None)?; // TODO we know the length of stream
        let mut buffer = vec![];

        use futures::AsyncReadExt;
        reader.read_to_end(&mut buffer).await.map_err(|e| {
            let msg = e.to_string();
            if e.kind() == ErrorKind::NotFound {
                ErrorCode::DalPathNotFound(msg)
            } else {
                ErrorCode::DalTransportError(msg)
            }
        })?;
        let r = serde_json::from_slice::<V>(&buffer)?;
        Ok(r)
    }
}

impl<T, L> CachedMetaReader<T, L>
where L: Loader<T>
{
    pub fn new(cache: Arc<RwLock<MemCache<String, Arc<T>>>>, loader: L) -> Self {
        Self { cache, loader }
    }
    pub async fn read(&self, loc: impl AsRef<str>) -> Result<Arc<T>> {
        let cache = &mut *self.cache.write().await;
        match cache.get(loc.as_ref()) {
            Some(item) => Ok(item.clone()),
            None => {
                let val = self.loader.load(loc.as_ref()).await?;
                let item = Arc::new(val);
                cache.put(loc.as_ref().to_owned(), item.clone());
                Ok(item)
            }
        }
    }
}

pub type SegmentInfoReader = CachedMetaReader<SegmentInfo, Arc<dyn DataAccessor>>;
pub type TableSnapshotReader = CachedMetaReader<TableSnapshot, Arc<dyn DataAccessor>>;
pub type ParquetMetaReader = CachedMetaReader<FileMetaDataW, Arc<QueryContext>>;

impl SegmentInfoReader {
    fn build(
        cache: Arc<RwLock<MemCache<String, Arc<SegmentInfo>>>>,
        loader: Arc<dyn DataAccessor>,
    ) -> Self {
        Self { cache, loader }
    }

    pub async fn api_test(reader: ParquetMetaReader) -> Result<Arc<FileMetaDataW>> {
        reader.read("test_loc").await
    }
}
