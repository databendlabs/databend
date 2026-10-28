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
use databend_storages_common_index::hnsw_component_names;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::IndexFileLocation;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use opendal::Operator;
use opendal::raw::AccessorInfo;
use parking_lot::Mutex;
use uuid::Uuid;

use super::VectorIndexIdentity;
use super::vector_index_loader::check_index_current;
use super::vector_index_loader::load_hnsw;

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
    identity: VectorIndexIdentity,
}

impl VectorIndexLoader {
    pub fn new(
        operator: Operator,
        settings: ReadSettings,
        distance_type: DistanceType,
        dim: usize,
        identity: VectorIndexIdentity,
    ) -> Self {
        let namespace = storage_namespace(&operator);
        Self {
            operator,
            settings,
            namespace,
            distance_type,
            dim,
            identity,
        }
    }

    pub fn identity(&self) -> &VectorIndexIdentity {
        &self.identity
    }

    /// Index files are immutable and uniquely named, so the storage backend, path,
    /// column and distance identify a decoded HNSW object. The namespace is per
    /// process, which suits this memory-only cache.
    fn cache_key(&self, location: &str) -> String {
        format!(
            "{}:{location}:{}:{}",
            self.namespace, self.identity.column_id, self.distance_type
        )
    }

    #[fastrace::trace]
    pub async fn load(
        &self,
        row_count: usize,
        location: &IndexFileLocation,
    ) -> Result<Arc<CachedVectorIndex>> {
        // Checked on every load, not only on a cache miss: the key does not include
        // the index version, and a legacy file may hold an outdated version.
        check_index_current(&self.operator, location, &self.identity).await?;
        let cache = CacheManager::instance().get_vector_index_cache();
        let key = self.cache_key(location.path());
        load_cached_index(cache, key, || {
            let loader = self.clone();
            let location = location.clone();
            async move {
                let names = hnsw_component_names(loader.identity.column_id, loader.distance_type);
                let (components, validity) = load_hnsw(
                    &loader.operator,
                    &loader.settings,
                    &location,
                    &loader.identity,
                    &names,
                    row_count,
                )
                .await?;
                HNSWIndex::open(
                    loader.distance_type,
                    loader.dim,
                    row_count,
                    &components,
                    validity.as_deref(),
                )
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
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::VectorColumn;
    use futures::future::join_all;

    use super::*;

    fn index_components() -> Result<Vec<Vec<u8>>> {
        let values = (0..64)
            .map(|i| F32::from((i as f32 * 0.17).sin()))
            .collect::<Vec<_>>();
        let column = Column::Vector(VectorColumn::Float32((values.into(), 8)));
        let (_, entries) = HNSWIndex::build(4, 8, 0, column, DistanceType::L2)?;
        Ok(entries
            .into_iter()
            .map(|entry| match entry {
                BlockEntry::Const(Scalar::Binary(bytes), _, _) => bytes,
                _ => unreachable!("HNSW build must produce binary components"),
            })
            .collect())
    }

    fn open(components: Vec<Vec<u8>>) -> Result<HNSWIndex> {
        HNSWIndex::open(DistanceType::L2, 8, 8, &components, None)
    }

    #[tokio::test]
    async fn test_concurrent_loads_share_one_load() -> Result<()> {
        let components = index_components()?;
        // Admitted, too large to admit, and no cache.
        for (i, cache) in [
            Some(VectorIndexCache::new(1024 * 1024)),
            Some(VectorIndexCache::new(1)),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            let key = format!("test_concurrent_loads_share_one_load_{i}");
            let opens = AtomicUsize::new(0);
            let results = join_all((0..8).map(|_| {
                load_cached_index(cache.clone(), key.clone(), || {
                    opens.fetch_add(1, Ordering::Relaxed);
                    let components = components.clone();
                    async move {
                        tokio::task::yield_now().await;
                        open(components)
                    }
                })
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
            assert_eq!(opens.load(Ordering::Relaxed), 1);
            assert!(results.iter().all(|index| Arc::ptr_eq(index, &results[0])));
            if i == 0 {
                let hit = load_cached_index(cache, key, || async {
                    panic!("warm cache must not reopen the index")
                })
                .await?;
                assert!(Arc::ptr_eq(&hit, &results[0]));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shared_failure_can_be_retried() -> Result<()> {
        let key = "test_shared_failure_can_be_retried".to_owned();
        let failures = join_all((0..4).map(|_| {
            load_cached_index(None, key.clone(), || async {
                tokio::task::yield_now().await;
                Err(ErrorCode::StorageOther("test load failure"))
            })
        }))
        .await;
        assert!(failures.iter().all(|failure| failure.is_err()));
        assert!(!INDEX_LOADS.lock().contains_key(&key));
        let components = index_components()?;
        load_cached_index(None, key, || async move { open(components) }).await?;
        Ok(())
    }
}
