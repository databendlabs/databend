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

//! A process-wide read cache for small, hot meta-service prefixes.
//!
//! Every query reads the user, the global settings and the cluster nodes from meta-service. These
//! prefixes are small and rarely change, so they are mirrored locally by one watch stream each
//! (see `databend-meta-plugin-cache`), and reads are answered from memory.
//!
//! Consistency rules, all enforced here so that callers do not need to know about the cache:
//!
//! - A read is answered from memory only if the prefix is ready, not suspected stale, every write
//!   this process made under the read range has been observed, and the result is non-empty.
//!   Otherwise it falls back to an ordinary meta-service read.
//! - Absent keys and empty lists are answered from memory only for global settings, where
//!   "absent" means "default"; for users and cluster nodes they fall back.
//! - Writes through [`CachedKvApi::fencing`] record fences: a later read of the same key or range
//!   bypasses the cache until the watch stream delivers that write (read-your-writes per process).
//! - A background verifier compares each prefix with a leader read every `verify_interval`,
//!   all prefixes concurrently. A mismatch is re-checked one second later; a second mismatch in a
//!   row, a fence not observed within one interval, or a verification read that fails or takes
//!   over 3 s marks the prefix suspect (all reads fall back) and forces the watch stream to
//!   re-fetch. This bounds staleness to about `verify_interval` + 3 s when a stream silently
//!   stops, e.g. a half-open connection or a lagging follower.
//!
//! Reads that are followed by a sequence-conditioned write must not use [`CachedKvApi::reading`].

mod cached_kv_api;
mod prefix_cache;
mod verifier;

use std::sync::Arc;
use std::time::Duration;

pub use cached_kv_api::CachedKvApi;
use databend_common_base::base::GlobalInstance;
use databend_common_base::base::escape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::SettingIdent;
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_meta_client::RpcClientConf;
use databend_meta_client::kvapi::StructKey;
use log::info;
use prefix_cache::PrefixCache;
pub use prefix_cache::PrefixStats;

use crate::warehouse::WAREHOUSE_API_KEY_PREFIX;

/// A meta-service prefix that can be served by [`MetaReadCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachedPrefix {
    /// `__fd_users/<tenant>/`
    User,
    /// `__fd_settings/<tenant>/`
    Setting,
    /// `__fd_clusters_v6/<tenant>/`: online nodes and cluster membership.
    Cluster,
}

impl CachedPrefix {
    pub fn parse(name: &str) -> Result<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(Self::User),
            "setting" => Ok(Self::Setting),
            "cluster" => Ok(Self::Cluster),
            other => Err(ErrorCode::InvalidConfig(format!(
                "unknown meta_read_cache prefix '{other}', expect one of: user, setting, cluster"
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Setting => "setting",
            Self::Cluster => "cluster",
        }
    }

    /// Whether "absent" may be answered from memory; see [`PrefixCache`].
    fn serve_absent(&self) -> bool {
        // No global setting means every setting has its default: as safe to serve slightly stale as
        // a changed setting. An unknown user or an empty node list falls back instead.
        matches!(self, Self::Setting)
    }

    /// The cached key range of `tenant`, with a trailing `/`.
    fn dir(&self, tenant: &Tenant) -> Result<String> {
        let dir = match self {
            Self::User => {
                TenantUserIdent::new_user_host(tenant.clone(), "dummy", "dummy").tenant_prefix()
            }
            Self::Setting => SettingIdent::new(tenant.clone(), "").to_string_key(),
            Self::Cluster => format!(
                "{}/{}/",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant.tenant_name())?
            ),
        };
        Ok(if dir.ends_with('/') { dir } else { dir + "/" })
    }
}

pub struct MetaReadCache {
    prefixes: Vec<Arc<PrefixCache>>,
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
