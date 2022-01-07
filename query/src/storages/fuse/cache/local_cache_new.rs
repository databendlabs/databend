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

#[async_trait::async_trait]
pub trait Loader<T> {
    async fn load(&self, key: &str) -> Result<T>;
}

pub type MemCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;

pub struct CachedLoader<T, L> {
    cache: Arc<RwLock<MemCache<String, Arc<T>>>>,
    loader: L,
}

impl<T, L> CachedLoader<T, L>
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
