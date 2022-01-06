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

use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use serde::de::DeserializeOwned;

use crate::storages::cache::StorageCache;
use crate::storages::fuse::cache::local_cache_new::Loader;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

type MemCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;
pub struct MetaReader<T, L> {
    cache: Arc<RwLock<MemCache<String, Arc<T>>>>,
    loader: L,
}

#[async_trait::async_trait]
impl<T> Loader<T> for dyn DataAccessor
where T: DeserializeOwned
{
    async fn load(&self, key: &str) -> Result<T> {
        let bytes = self.read(key).await?;
        let r = serde_json::from_slice::<T>(&bytes)?;
        Ok(r)
    }
}

#[async_trait::async_trait]
impl Loader<FileMetaData> for Arc<dyn DataAccessor> {
    async fn load(&self, key: &str) -> Result<FileMetaData> {
        let mut reader = self.get_input_stream(key, None)?; // TODO we know the length of stream
        read_metadata_async(&mut reader)
            .instrument(debug_span!("parquet_source_read_meta"))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))
    }
}

impl<T, L> MetaReader<T, L>
where L: Loader<T>
{
    pub fn new(cache: Arc<RwLock<MemCache<String, Arc<T>>>>, loader: L) -> Self {
        Self { cache, loader }
    }
    pub async fn read(&self, loc: impl AsRef<str>) -> Result<Arc<T>>
    where T: DeserializeOwned {
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

pub type SegmentInfoReader = MetaReader<SegmentInfo, Arc<dyn DataAccessor>>;
pub type TableSnapshotReader = MetaReader<TableSnapshot, Arc<dyn DataAccessor>>;
pub type ParquetMetaReader = MetaReader<FileMetaData, Arc<dyn DataAccessor>>;

impl SegmentInfoReader {
    fn build(
        cache: Arc<RwLock<MemCache<String, Arc<SegmentInfo>>>>,
        loader: Arc<dyn DataAccessor>,
    ) -> Self {
        Self { cache, loader }
    }

    async fn api_test(reader: ParquetMetaReader) -> Result<Arc<FileMetaData>> {
        reader.read("test_loc").await
    }
}
