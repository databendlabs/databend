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
use databend_common_ast::ast::Expr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use log::debug;
use log::info;
use tokio::sync::OnceCell;

use crate::data_mask_handler::get_datamask_handler;

/// Background cleanup interval for orphaned cache entries.
const DATA_MASK_CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

/// Cache key: (tenant, policy_id).
type CacheKey = (Tenant, u64);

/// Cached parsed representation of a data mask policy.
/// Stores the parsed AST expression and parameter metadata so that
/// cache hits skip both the metastore RPC and tokenize/parse.
#[derive(Clone, Debug)]
pub struct CachedDataMaskPolicy {
    /// Parsed policy body expression.
    pub expr: Expr,
    /// Parameter definitions: Vec<(param_name, param_type)>.
    /// Parameter names are already normalized to lowercase at creation time.
    pub args: Vec<(String, String)>,
}

/// Each entry is an `Arc<OnceCell<...>>` so that concurrent misses on the same
/// key only trigger one meta-store RPC + parse (singleflight).
///
/// `OnceCell::get_or_try_init` will NOT persist an `Err` — if the first caller
/// fails, the cell stays empty and the next caller retries.
type CacheEntry = Arc<OnceCell<CachedDataMaskPolicy>>;

pub struct DataMaskCacheManager {
    cache: DashMap<CacheKey, CacheEntry>,
}

impl DataMaskCacheManager {
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
                tokio::time::sleep(DATA_MASK_CACHE_CLEANUP_INTERVAL).await;
                let count = manager.cache.len();
                if count > 0 {
                    manager.cache.clear();
                    debug!(
                        "data_mask_cache: cleared {} orphaned/stale entries",
                        count
                    );
                }
            }
        });

        Ok(())
    }

    pub fn instance() -> Arc<DataMaskCacheManager> {
        GlobalInstance::get()
    }

    /// Synchronous fast path — returns `Some` only when the parsed policy is
    /// already cached. The binder calls this first to avoid `block_on`
    /// overhead on cache hits.
    pub fn try_get(&self, tenant: &Tenant, policy_id: u64) -> Option<CachedDataMaskPolicy> {
        let key = (tenant.clone(), policy_id);
        self.cache.get(&key).and_then(|cell| cell.get().cloned())
    }

    /// Invalidate one cached definition when the underlying policy is dropped.
    pub fn invalidate(&self, tenant: &Tenant, policy_id: u64) {
        let key = (tenant.clone(), policy_id);
        self.cache.remove(&key);
    }

    /// Async path — used on cache miss. Guarantees singleflight: concurrent
    /// callers for the same `(tenant, policy_id)` share one in-flight RPC.
    /// Errors are never cached.
    pub async fn get_or_load(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<CachedDataMaskPolicy> {
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
                    let handler = get_datamask_handler();
                    let seq_v = handler
                        .get_data_mask_by_id(meta_api, &tenant, policy_id)
                        .await?;
                    let meta = seq_v.data;

                    // Parse the policy body into AST once.
                    // Use PostgreSQL dialect as the canonical dialect for policy bodies,
                    // since they are authored as Databend SQL at creation time.
                    let tokens = tokenize_sql(&meta.body)?;
                    let expr = parse_expr(&tokens, Dialect::PostgreSQL)?;

                    info!(
                        "data_mask_cache: loaded and parsed policy_id={} from meta store",
                        policy_id
                    );

                    Ok::<_, databend_common_exception::ErrorCode>(CachedDataMaskPolicy {
                        expr,
                        args: meta.args,
                    })
                }
            })
            .await?;

        Ok(result.clone())
    }
}
