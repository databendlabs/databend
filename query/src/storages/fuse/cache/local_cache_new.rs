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

use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Cache;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;
use common_exception::Result;

use crate::storages::fuse::cache::metrics::CacheDeferMetrics;

#[async_trait::async_trait]
pub trait Loader<T> {
    async fn load(&self, key: &str) -> Result<T>;
}

pub trait TenantAware {
    fn get_tenant_info(&self) -> (String, String);
}

type LaCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;
pub type TableInMemCache<V> = Arc<RwLock<LaCache<String, Arc<V>>>>;

pub fn empty_with_capacity<V>(capacity: u64) -> TableInMemCache<V> {
    Arc::new(RwLock::new(LruCache::new(capacity)))
}

pub struct CachedLoader<T, L> {
    cache: Option<TableInMemCache<T>>,
    loader: L,
}

impl<V, L> CachedLoader<V, L>
where L: Loader<V> + TenantAware
{
    pub fn new(cache: Option<TableInMemCache<V>>, loader: L) -> Self {
        Self { cache, loader }
    }
    pub async fn read(&self, loc: impl AsRef<str>) -> Result<Arc<V>> {
        match &self.cache {
            None => self.load(loc.as_ref()).await,
            Some(cache) => {
                let (tenant_id, cluster_id) = self.loader.get_tenant_info();
                let mut metrics = CacheDeferMetrics {
                    tenant_id: tenant_id.as_str(),
                    cluster_id: cluster_id.as_str(),
                    cache_hit: false,
                    read_bytes: 0,
                };
                let cache = &mut cache.write().await;
                match cache.get(loc.as_ref()) {
                    Some(item) => {
                        metrics.cache_hit = true;
                        metrics.read_bytes = 0u64;
                        Ok(item.clone())
                    }
                    None => {
                        let item = self.load(loc.as_ref()).await?;
                        cache.put(loc.as_ref().to_owned(), item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    async fn load(&self, loc: &str) -> Result<Arc<V>> {
        let val = self.loader.load(loc).await?;
        let item = Arc::new(val);
        Ok(item)
    }
}
