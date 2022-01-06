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

use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_cache::Meter;
use common_dal::DataAccessor;
use common_exception::Result;
use serde::de::DeserializeOwned;

use crate::storages::fuse::meta::SegmentInfo;

#[async_trait::async_trait]
pub trait Reader {
    async fn load(&self, k: &str) -> Result<Vec<u8>>;
}

#[async_trait::async_trait]
pub trait LocalCache<V> {
    async fn read(&self, k: &str) -> Result<Arc<V>>;
}

pub struct RawObjectSizeMeter;

impl<K, V: HasLen> Meter<K, V> for RawObjectSizeMeter {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, item: &V) -> usize
    where K: Borrow<Q> {
        item.len()
    }
}

type MemCache<K, V> = LruCache<K, V, DefaultHashBuilder, RawObjectSizeMeter>;

pub struct CacheBuilder {}
impl CacheBuilder {
    fn build<K, V, R>(reader: R) -> LoadingCache<K, V, R>
    where
        K: Hash + Eq,
        R: Reader,
    {
        let cache: MemCache<K, _> = LruCache::with_meter(100, RawObjectSizeMeter);
        LoadingCache::new(reader, cache)
    }
}

/// Guava Cache ?
struct LoadingCache<K, V, R>
where
    R: Reader,
    K: Eq + Hash,
{
    loader: R,
    cache: RwLock<MemCache<K, (Arc<V>, usize)>>,
}

impl<K, V, R> LoadingCache<K, V, R>
where
    R: Reader,
    K: Eq + Hash,
{
    fn new(loader: R, cache: MemCache<K, (Arc<V>, usize)>) -> Self {
        Self {
            loader,
            cache: RwLock::new(cache),
        }
    }
}

trait HasLen {
    fn len(&self) -> usize;
}

impl<T> HasLen for (T, usize) {
    fn len(&self) -> usize {
        self.1
    }
}

#[async_trait::async_trait]
impl<V, R> LocalCache<V> for LoadingCache<String, V, R>
where
    R: Reader + Send + Sync,
    V: DeserializeOwned,
    V: Send + Sync,
{
    async fn read(&self, loc: &str) -> Result<Arc<V>> {
        let cache = &mut *self.cache.write().await;
        match cache.get(loc) {
            Some(item) => Ok(item.0.clone()),
            None => {
                let raw_val = self.loader.load(loc).await?;
                let len = raw_val.len();
                let obj = serde_json::from_slice::<V>(&raw_val)?;
                let item = Arc::new(obj);
                cache.put(loc.to_owned(), (item.clone(), len));
                Ok(item)
            }
        }
    }
}

#[async_trait::async_trait]
impl Reader for Arc<dyn DataAccessor> {
    async fn load(&self, k: &str) -> Result<Vec<u8>> {
        self.read(k.as_ref()).await
    }
}

pub async fn api_test(da: Arc<dyn DataAccessor>) -> Result<Arc<SegmentInfo>> {
    let cache = CacheBuilder::build(da);
    cache.read("test").await
}
