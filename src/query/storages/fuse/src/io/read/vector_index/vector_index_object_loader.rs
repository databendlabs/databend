// Copyright 2021 Datafuse Labs
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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Weak;

use databend_common_exception::Result;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CachedVectorIndex;
use databend_storages_common_cache::VectorIndexCache;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_io::ReadSettings;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use opendal::Operator;
use opendal::raw::AccessorInfo;
use parking_lot::Mutex;
use uuid::Uuid;

use super::vector_index_loader::load_vector_index_files;

// OpenDAL's scheme/name/root do not identify the endpoint (e.g. S3 name is just the bucket).
// Assign a non-reused identity to each live backend info instead. Unlike the outer accessor,
// this identity survives per-table StorageMetricsLayer wrappers. Replacing a backend cannot
// accidentally reuse an old cache namespace.
type StorageNamespace = (Weak<AccessorInfo>, Uuid);
static STORAGE_NAMESPACES: LazyLock<Mutex<Vec<StorageNamespace>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

fn storage_namespace(operator: &Operator) -> Uuid {
    let info = operator.inner().info_dyn();
    let accessor = Arc::downgrade(&info);
    let mut namespaces = STORAGE_NAMESPACES.lock();
    namespaces.retain(|(accessor, _)| accessor.strong_count() > 0);
    if let Some((_, namespace)) = namespaces
        .iter()
        .find(|(existing, _)| existing.ptr_eq(&accessor))
    {
        return *namespace;
    }
    let namespace = Uuid::new_v4();
    namespaces.push((accessor, namespace));
    namespace
}

#[derive(Clone)]
pub(super) struct VectorIndexLoader {
    operator: Operator,
    settings: ReadSettings,
    namespace: Uuid,
    distance_type: DistanceType,
    dim: usize,
    columns: Vec<String>,
}

impl VectorIndexLoader {
    pub fn new(
        operator: Operator,
        settings: ReadSettings,
        distance_type: DistanceType,
        dim: usize,
        columns: Vec<String>,
    ) -> Self {
        let namespace = storage_namespace(&operator);
        Self {
            operator,
            settings,
            namespace,
            distance_type,
            dim,
            columns,
        }
    }

    fn cache_key(&self, row_count: usize, location: &str) -> Result<String> {
        // The immutable file name includes its format version. Column names identify the stable
        // column id and distance representation; query values and search limits are not identity.
        Ok(serde_json::to_string(&(
            "vector-index-object-v1",
            self.namespace,
            location,
            &self.columns,
            self.distance_type,
            self.dim,
            row_count,
        ))?)
    }

    #[fastrace::trace]
    pub async fn load(&self, row_count: usize, location: &str) -> Result<Arc<CachedVectorIndex>> {
        let cache = CacheManager::instance().get_vector_index_cache();
        let key = self.cache_key(row_count, location)?;
        load_cached_index(cache, key, || {
            let loader = self.clone();
            let location = location.to_owned();
            async move {
                let columns = load_vector_index_files(
                    loader.operator,
                    &loader.settings,
                    &loader.columns,
                    &location,
                )
                .await?;
                HNSWIndex::open(loader.distance_type, loader.dim, row_count, columns)
            }
        })
        .await
    }
}

type IndexLoadResult = Result<Arc<CachedVectorIndex>>;
type IndexLoadFuture = Shared<BoxFuture<'static, IndexLoadResult>>;
static INDEX_LOADS: LazyLock<Mutex<HashMap<String, Weak<IndexLoad>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct IndexLoad {
    key: String,
    future: IndexLoadFuture,
}

impl Drop for IndexLoad {
    fn drop(&mut self) {
        let mut loads = INDEX_LOADS.lock();
        // A new generation may have replaced this load after its last strong reference vanished.
        if loads
            .get(&self.key)
            .is_some_and(|load| std::ptr::eq(load.as_ptr(), self))
        {
            loads.remove(&self.key);
        }
    }
}

/// Share the result of this load, not merely a lock followed by another cache lookup. This also
/// deduplicates oversized or temporarily uncached objects. Weak registry entries do not retain
/// completed objects. Dropping one waiter leaves the others running; dropping all cancels the load.
async fn load_cached_index<F, Fut>(
    cache: Option<VectorIndexCache>,
    key: String,
    load: F,
) -> IndexLoadResult
where
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<HNSWIndex>> + Send + 'static,
{
    if let Some(index) = cache.get(&key) {
        return Ok(index);
    }

    let flight = {
        let mut loads = INDEX_LOADS.lock();
        if let Some(flight) = loads.get(&key).and_then(Weak::upgrade) {
            flight
        } else {
            // An earlier flight could have populated the cache before we acquired the map.
            if let Some(index) = cache.get(&key) {
                return Ok(index);
            }
            let load_key = key.clone();
            let source = load();
            let future = async move {
                let index = CachedVectorIndex::new(source.await?);
                Ok(cache.insert(load_key, index))
            }
            .boxed()
            .shared();
            let flight = Arc::new(IndexLoad { key, future });
            loads.insert(flight.key.clone(), Arc::downgrade(&flight));
            flight
        }
    };

    flight.future.clone().await
}

#[cfg(test)]
mod tests;
