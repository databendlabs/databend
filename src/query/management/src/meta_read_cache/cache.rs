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
        let mut prefixes = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let dir = kind.dir(tenant)?;
            info!("meta read cache: watch prefix {} at {}", kind.name(), dir);
            prefixes.push(Arc::new(
                PrefixCache::new(meta.inner().clone(), kind.name(), dir, kind.serve_absent()).await,
            ));
        }
        let cache = Arc::new(MetaReadCache { prefixes });
        verifier::spawn(Arc::downgrade(&cache), Arc::new(meta), verify_interval);
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
