// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::hash::Hash;
use std::sync::Arc;

use iceberg::cache::{ObjectCache, ObjectCacheProvide};
use iceberg::spec::{Manifest, ManifestList};

const DEFAULT_CACHE_SIZE_BYTES: u64 = 32 * 1024 * 1024; // 32MiB

struct MokaObjectCache<K, V>(moka::sync::Cache<K, V>);

impl<K, V> ObjectCache<K, V> for MokaObjectCache<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get(&self, key: &K) -> Option<V> {
        self.0.get(key)
    }

    fn set(&self, key: K, value: V) {
        self.0.insert(key, value);
    }
}

/// A cache provider that uses Moka for caching objects.
pub struct MokaObjectCacheProvider {
    manifest_cache: MokaObjectCache<String, Arc<Manifest>>,
    manifest_list_cache: MokaObjectCache<String, Arc<ManifestList>>,
}

impl Default for MokaObjectCacheProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MokaObjectCacheProvider {
    /// Creates a new `MokaObjectCacheProvider` with default cache sizes.
    pub fn new() -> Self {
        let manifest_cache = MokaObjectCache(moka::sync::Cache::new(DEFAULT_CACHE_SIZE_BYTES));
        let manifest_list_cache = MokaObjectCache(moka::sync::Cache::new(DEFAULT_CACHE_SIZE_BYTES));

        Self {
            manifest_cache,
            manifest_list_cache,
        }
    }

    /// Set the cache for manifests.
    pub fn with_manifest_cache(mut self, cache: moka::sync::Cache<String, Arc<Manifest>>) -> Self {
        self.manifest_cache = MokaObjectCache(cache);
        self
    }

    /// Set the cache for manifest lists.
    pub fn with_manifest_list_cache(
        mut self,
        cache: moka::sync::Cache<String, Arc<ManifestList>>,
    ) -> Self {
        self.manifest_list_cache = MokaObjectCache(cache);
        self
    }
}

impl ObjectCacheProvide for MokaObjectCacheProvider {
    fn manifest_cache(&self) -> &dyn ObjectCache<String, Arc<Manifest>> {
        &self.manifest_cache
    }

    fn manifest_list_cache(&self) -> &dyn ObjectCache<String, Arc<ManifestList>> {
        &self.manifest_list_cache
    }
}
