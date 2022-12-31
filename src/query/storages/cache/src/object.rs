// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use opendal::Object;

use crate::ByPassCache;
use crate::CacheSettings;
use crate::ObjectCacheProvider;

/// Wrap with cache for the object write/read/remove trait.
/// T is the cache item type.
/// If you want use cache, we should:
/// 1. create a cache provider(ByPassCache, MemoryBytesCache/MemoryItemsCache/FileCache)
/// 2. create the CachedObjectAccessor with the cache provider
/// 3. operation with object
pub struct CachedObjectAccessor<T> {
    cache: Arc<dyn ObjectCacheProvider<T>>,
}

impl<T> CachedObjectAccessor<T> {
    pub fn create(cache: Arc<dyn ObjectCacheProvider<T>>) -> CachedObjectAccessor<T> {
        Self { cache }
    }

    pub async fn read(&self, object: &Object, start: u64, end: u64) -> Result<Arc<T>> {
        self.cache.read_object(object, start, end).await
    }

    pub async fn write(&self, object: &Object, t: Arc<T>) -> Result<()> {
        self.cache.write_object(object, t).await
    }

    pub async fn remove(&self, object: &Object) -> Result<()> {
        self.cache.remove_object(object).await
    }
}

/// Default backend cache is ByPassCache.
/// There is no cache for all the operations.
impl<T> Default for CachedObjectAccessor<T>
where ByPassCache: ObjectCacheProvider<T>
{
    fn default() -> Self {
        let cache = ByPassCache::create(&CacheSettings::default());
        CachedObjectAccessor::create(Arc::new(cache))
    }
}
