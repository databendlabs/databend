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
//

use std::hash::Hash;
use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::DefaultHashBuilder;
use common_cache::HeapSize;
use common_cache::HeapSizeOf;
use common_cache::LruCache;
use common_dal::DataAccessor;
use common_exception::Result;
use serde::de::DeserializeOwned;

use crate::storages::fuse::meta::SegmentInfo;

#[async_trait::async_trait]
pub trait Reader<V> {
    async fn load(&self, k: &str) -> Result<V>;
}

#[async_trait::async_trait]
pub trait LocalCache<V> {
    async fn read(&self, k: &str) -> Result<Arc<V>>;
}

pub struct CacheBuilder {}

type MemCache<K, V> = LruCache<K, Arc<V>, DefaultHashBuilder, HeapSize>;

impl CacheBuilder {
    fn build<K, V, R>(loader: R) -> LoadingCache<K, V, R>
    where
        R: Reader<V>,
        K: Eq + Hash,
        V: HeapSizeOf,
    {
        let cache: MemCache<K, V> = LruCache::with_meter(100, HeapSize);
        LoadingCache::new(loader, cache)
    }
}

struct LoadingCache<K, V, R>
where
    R: Reader<V>,
    K: Eq + Hash,
    V: HeapSizeOf,
{
    loader: R,
    cache: RwLock<MemCache<K, V>>,
}

impl<K, V, R> LoadingCache<K, V, R>
where
    V: HeapSizeOf,
    R: Reader<V>,
    K: Eq + Hash,
{
    fn new(loader: R, cache: MemCache<K, V>) -> Self {
        Self {
            loader,
            cache: RwLock::new(cache),
        }
    }
}

#[async_trait::async_trait]
impl<V, R> LocalCache<V> for LoadingCache<String, V, R>
where
    R: Reader<V> + Send + Sync,
    V: HeapSizeOf + Send + Sync,
{
    async fn read(&self, loc: &str) -> Result<Arc<V>> {
        let cache = &mut *self.cache.write().await;
        match cache.get(loc) {
            Some(item) => Ok(item.clone()),
            None => {
                let item = Arc::new(self.loader.load(loc).await?);
                cache.put(loc.to_owned(), item.clone());
                Ok(item)
            }
        }
    }
}

struct ObjectReader {
    da: Arc<dyn DataAccessor>,
}

#[async_trait::async_trait]
impl<T> Reader<T> for ObjectReader
where T: DeserializeOwned
{
    async fn load(&self, k: &str) -> Result<T> {
        let bytes = self.da.read(k.as_ref()).await?;
        let r = serde_json::from_slice::<T>(&bytes)?;
        Ok(r)
    }
}

fn get_da() -> Arc<dyn DataAccessor> {
    todo!()
}
async fn test() {
    let da = get_da();
    let loader = ObjectReader { da };
    let cache = CacheBuilder::build::<String, SegmentInfo, _>(loader);
}
