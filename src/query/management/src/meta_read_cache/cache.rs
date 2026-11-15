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

use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_meta_client::RpcClientConf;
use log::info;

use super::cached_prefix::CachedPrefix;
use super::prefix_cache::PrefixCache;
use super::prefix_cache::PrefixStats;
use super::verifier;

/// The cached prefixes of this process; see the module documentation.
pub struct MetaReadCache {
    pub(super) prefixes: Vec<Arc<PrefixCache>>,
}

impl MetaReadCache {
    /// Create the process-wide instance if any prefix is configured; otherwise do nothing, and
    /// [`Self::try_instance`] returns `None` so that every read goes to meta-service.
    pub async fn init(
        conf: RpcClientConf,
        tenant: &Tenant,
        prefixes: &[String],
        verify_interval: Duration,
    ) -> Result<()> {
        if prefixes.is_empty() {
            return Ok(());
        }
        let kinds = prefixes
            .iter()
            .map(|p| CachedPrefix::parse(p))
            .collect::<Result<Vec<_>>>()?;
        let meta = MetaStoreProvider::new(conf)
            .create_default_meta_store()
            .await
            .map_err(|e| {
                ErrorCode::MetaServiceError(e.to_string())
                    .add_message_back("(while create meta read cache)")
            })?;
        match meta.get_local_addr().await {
            Ok(addr) => info!("meta read cache: meta-service sees this client at {addr}"),
            Err(e) => info!("meta read cache: local address unknown: {e}"),
        }
        let cache = Self::create(meta, tenant, &kinds, verify_interval).await?;
        GlobalInstance::set(cache);
        Ok(())
    }

    pub fn try_instance() -> Option<Arc<MetaReadCache>> {
        GlobalInstance::try_get()
    }

    /// Start one watch stream per prefix and the verifier.
    pub async fn create(
        meta: MetaStore,
        tenant: &Tenant,
        kinds: &[CachedPrefix],
        verify_interval: Duration,
    ) -> Result<Arc<Self>> {
        Self::create_with_leader(meta.clone(), meta, tenant, kinds, verify_interval).await
    }

    /// Watch `meta` and verify against `leader`. Tests use a separate `leader` to stand for a
    /// meta-service whose state the watch stream no longer delivers.
    pub(crate) async fn create_with_leader(
        meta: MetaStore,
        leader: MetaStore,
        tenant: &Tenant,
        kinds: &[CachedPrefix],
        verify_interval: Duration,
    ) -> Result<Arc<Self>> {
        let mut prefixes = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let dir = kind.dir(tenant)?;
            info!("meta read cache: watch prefix {} at {}", kind.name(), dir);
            prefixes.push(Arc::new(
                PrefixCache::new(meta.inner().clone(), kind.name(), dir, kind.serve_absent()).await,
            ));
        }
        let cache = Arc::new(MetaReadCache { prefixes });
        verifier::spawn(Arc::downgrade(&cache), Arc::new(leader), verify_interval);
        Ok(cache)
    }

    /// The cached prefix containing `key`.
    pub(crate) fn find(&self, key: &str) -> Option<&Arc<PrefixCache>> {
        self.prefixes.iter().find(|p| key.starts_with(&p.dir))
    }

    /// Per-prefix counters, for logs and tests.
    pub fn stats(&self) -> Vec<(&'static str, PrefixStats)> {
        self.prefixes.iter().map(|p| (p.name, p.stats())).collect()
    }

    /// Whether `name`'s prefix currently answers reads from memory; for tests.
    pub fn is_serving(&self, name: &str) -> bool {
        self.prefixes
            .iter()
            .any(|p| p.name == name && p.is_serving())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use databend_common_meta_app::tenant::Tenant;
    use databend_common_meta_store::MetaStore;
    use databend_meta_client::kvapi::KVApi;
    use databend_meta_client::kvapi::KvApiExt;
    use databend_meta_client::types::MetaError;
    use databend_meta_client::types::UpsertKV;
    use databend_meta_runtime::DatabendRuntime;

    use super::MetaReadCache;
    use crate::meta_read_cache::CachedKvApi;
    use crate::meta_read_cache::CachedPrefix;

    struct Stalled {
        cache: Arc<MetaReadCache>,
        /// Keeps the watched meta-service running; the cache holds only a client of it.
        _watched: MetaStore,
        leader: MetaStore,
        reading: CachedKvApi,
        fencing: CachedKvApi,
        key: String,
    }

    /// The cache watches a store that never receives the writes made to `leader`, as a stalled
    /// watch stream would; the verifier and the fallback reads go to `leader`.
    async fn stalled() -> anyhow::Result<Stalled> {
        let watched = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let leader = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let cache = MetaReadCache::create_with_leader(
            watched.clone(),
            leader.clone(),
            &Tenant::new_literal("t1"),
            &[CachedPrefix::Setting],
            Duration::from_millis(200),
        )
        .await?;
        for _ in 0..200 {
            if cache.is_serving("setting") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(cache.is_serving("setting"), "{:?}", cache.stats());
        let inner: Arc<dyn KVApi<Error = MetaError>> = Arc::new(leader.clone());
        Ok(Stalled {
            reading: CachedKvApi::reading(inner.clone(), cache.clone()),
            fencing: CachedKvApi::fencing(inner, cache.clone()),
            key: format!("{}max_threads", cache.prefixes[0].dir),
            cache,
            _watched: watched,
            leader,
        })
    }

    /// After the verifier gives up on the cache, reads are answered by the leader.
    async fn assert_resynced(s: &Stalled) -> anyhow::Result<()> {
        let st = s.cache.stats()[0].1;
        assert!(st.resync >= 1, "{st:?}");
        assert!(!s.cache.is_serving("setting"));
        assert_eq!(s.reading.get_kv(&s.key).await?.unwrap().data, b"v".to_vec());
        assert!(s.cache.stats()[0].1.fallback_suspect >= 1);
        Ok(())
    }

    /// A local write the stream does not deliver within one interval, while the leader has it,
    /// stops serving from memory at the next verification.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stalled_stream_with_local_write_resyncs() -> anyhow::Result<()> {
        let s = stalled().await?;
        s.fencing.upsert_kv(UpsertKV::update(&s.key, b"v")).await?;

        // Fenced: falls back and returns what the leader has.
        assert_eq!(s.reading.get_kv(&s.key).await?.unwrap().data, b"v".to_vec());
        assert!(s.cache.stats()[0].1.fallback_fence >= 1);

        // One verification at 200 ms finds the overdue fence and the difference.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_resynced(&s).await
    }

    /// A write by another node that the stream does not deliver makes consecutive verifications
    /// differ from the leader, which stops serving from memory.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stalled_stream_with_remote_write_resyncs() -> anyhow::Result<()> {
        let s = stalled().await?;
        s.leader.upsert_kv(UpsertKV::update(&s.key, b"v")).await?;

        // Not fenced and not delivered: the stale cache answers.
        assert!(s.reading.get_kv(&s.key).await?.is_none());

        // Mismatches at 200 ms and at the re-check 200 ms later.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_resynced(&s).await
    }
}
