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
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;

pub struct RawObjectSizeMeter;

type MemCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;

pub struct CacheBuilder {
    capacity: u64,
}

impl CacheBuilder {
    fn build<K, V>(self) -> LoadingCache<K, V>
    where K: Hash + Eq {
        let cache: MemCache<K, _> = LruCache::with_meter(self.capacity, Count);
        LoadingCache::new(cache)
    }
}

/// Guava Cache ?
struct LoadingCache<K, V>
where K: Eq + Hash
{
    cache: RwLock<MemCache<K, Arc<V>>>,
}

impl<K, V> LoadingCache<K, V>
where K: Eq + Hash
{
    fn new(cache: MemCache<K, Arc<V>>) -> Self {
        Self {
            cache: RwLock::new(cache),
        }
    }
}

#[async_trait::async_trait]
pub trait LocalCache<V, R> {
    //async fn read<R: Loader<V> + Send + Sync>(&self, loader: R, loc: &str) -> Result<Arc<V>>;
    async fn read(&self, loader: R, loc: &str) -> Result<Arc<V>>;
}

#[async_trait::async_trait]
pub trait Loader<T> {
    async fn load(&self, key: &str) -> Result<T>;
}

//#[async_trait::async_trait]
//impl<V, R> LocalCache<V, R> for LoadingCache<String, V>
//where
//    V: Send + Sync,
//    R: Loader<V> + Send + Sync,
//{
//    //async fn read<R: Loader<V> + Send + Sync>(&self, loader: R, loc: &str) -> Result<Arc<V>> {
//    async fn read(&self, loader: R, loc: &str) -> Result<Arc<V>> {
//        let cache = &mut *self.cache.write().await;
//        match cache.get(loc) {
//            Some(item) => Ok(item.clone()),
//            None => {
//                let val = loader.load(loc).await?;
//                let item = Arc::new(val);
//                cache.put(loc.to_owned(), item.clone());
//                Ok(item)
//            }
//        }
//    }
//}
