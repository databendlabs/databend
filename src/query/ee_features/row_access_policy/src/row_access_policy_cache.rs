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

use dashmap::DashMap;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_client::types::SeqV;
use log::debug;
use log::info;
use tokio::sync::OnceCell;

use crate::row_access_policy_handler::get_row_access_policy_handler;

/// Background cleanup interval for orphaned cache entries.
/// Entries for dropped policies on remote nodes are never accessed again,
/// so periodic full clear is the simplest way to bound memory growth.
const RAP_CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

/// Cache key: (tenant, policy_id).
type CacheKey = (Tenant, u64);

/// Each entry is an `Arc<OnceCell<...>>` so that concurrent misses on the same
/// key only trigger one meta-store RPC (singleflight).
///
/// `OnceCell::get_or_try_init` will NOT persist an `Err` — if the first caller
/// fails, the cell stays empty and the next caller retries.
type CacheEntry = Arc<OnceCell<SeqV<RowAccessPolicyMeta>>>;

pub struct RowAccessPolicyCacheManager {
    cache: DashMap<CacheKey, CacheEntry>,
}

impl RowAccessPolicyCacheManager {
    fn new() -> Self {
        Self {
            cache: DashMap::new(),
        }
    }

    pub fn init() -> Result<()> {
        let manager = Arc::new(Self::new());
        GlobalInstance::set(manager.clone());

        // Background task to reclaim orphaned entries.
        // On remote nodes, entries for dropped policies are never looked up
        // again (policy_id is monotonic), so periodic clear is the only way
        // to reclaim them.
        databend_common_base::runtime::spawn(async move {
            loop {
                tokio::time::sleep(RAP_CACHE_CLEANUP_INTERVAL).await;
                let count = manager.cache.len();
                if count > 0 {
                    manager.cache.clear();
                    debug!(
                        "row_access_policy_cache: cleared {} orphaned/stale entries",
                        count
                    );
                }
            }
        });

        Ok(())
    }

    pub fn instance() -> Arc<RowAccessPolicyCacheManager> {
        GlobalInstance::get()
    }

    /// Synchronous fast path — returns `Some` only when the definition is
    /// already cached.  The binder calls this first to avoid `block_on`
    /// overhead on cache hits.
    pub fn try_get(&self, tenant: &Tenant, policy_id: u64) -> Option<SeqV<RowAccessPolicyMeta>> {
        let key = (tenant.clone(), policy_id);
        self.cache.get(&key).and_then(|cell| cell.get().cloned())
    }

    /// Invalidate one cached definition when the underlying RAP is dropped.
    pub fn invalidate(&self, tenant: &Tenant, policy_id: u64) {
        let key = (tenant.clone(), policy_id);
        self.cache.remove(&key);
    }

    /// Async path — used on cache miss.  Guarantees singleflight: concurrent
    /// callers for the same `(tenant, policy_id)` share one in-flight RPC.
    /// Errors are never cached.
    pub async fn get_or_load(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<RowAccessPolicyMeta>> {
        let key = (tenant.clone(), policy_id);

        // Get-or-insert the OnceCell for this key.
        let cell = self
            .cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .value()
            .clone();

        let result = cell
            .get_or_try_init(|| {
                let meta_api = meta_api.clone();
                let tenant = tenant.clone();
                async move {
                    let handler = get_row_access_policy_handler();
                    let res = handler
                        .get_row_access_policy_by_id(meta_api, &tenant, policy_id)
                        .await;
                    if res.is_ok() {
                        info!(
                            "row_access_policy_cache: loaded policy_id={} from meta store",
                            policy_id
                        );
                    }
                    res
                }
            })
            .await?;

        Ok(result.clone())
    }
}
