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

use std::future::Future;
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
use log::debug;
use log::info;
use tokio::sync::OnceCell;

/// All policies are parsed with PostgreSQL dialect regardless of the session's
/// `sql_dialect` setting. This avoids per-dialect cache duplication and ensures
/// consistent AST representation.
const POLICY_DIALECT: Dialect = Dialect::PostgreSQL;

/// Cached parsed representation of a security policy (row access policy or data mask).
/// Stores the parsed AST expression and parameter metadata so that
/// cache hits skip both the metastore RPC and tokenize/parse.
#[derive(Clone, Debug)]
pub struct CachedSecurityPolicy {
    /// Parsed policy body expression.
    pub expr: Expr,
    /// Parameter definitions: Vec<(param_name, param_type)>.
    /// Parameter names are already normalized to lowercase at creation time.
    pub args: Vec<(String, String)>,
}

/// Distinguishes which internal cache to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyType {
    RowAccessPolicy,
    DataMask,
}

impl PolicyType {
    fn label(&self) -> &'static str {
        match self {
            PolicyType::RowAccessPolicy => "row_access_policy_cache",
            PolicyType::DataMask => "data_mask_cache",
        }
    }
}

/// Raw policy definition fetched from the meta store.
/// The cache layer handles tokenize/parse uniformly.
pub struct RawPolicyDef {
    pub body: String,
    pub args: Vec<(String, String)>,
}

/// Cache key: (tenant, policy_id).
type CacheKey = (Tenant, u64);

/// Background cleanup interval for orphaned cache entries.
/// Entries for dropped policies are never looked up again (the caller always
/// reads the latest table meta first to obtain the current policy_id), so
/// stale hits cannot occur. This periodic clear only reclaims memory.
const CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

/// Singleflight cache for parsed security policy expressions.
struct PolicyCache {
    cache: DashMap<CacheKey, Arc<OnceCell<Arc<CachedSecurityPolicy>>>>,
}

impl PolicyCache {
    fn new() -> Self {
        Self {
            cache: DashMap::new(),
        }
    }

    fn try_get(&self, tenant: &Tenant, policy_id: u64) -> Option<Arc<CachedSecurityPolicy>> {
        let key = (tenant.clone(), policy_id);
        self.cache.get(&key).and_then(|cell| cell.get().cloned())
    }

    fn invalidate(&self, tenant: &Tenant, policy_id: u64) {
        self.cache.remove(&(tenant.clone(), policy_id));
    }

    async fn get_or_load<F, Fut>(
        &self,
        tenant: &Tenant,
        policy_id: u64,
        policy_type: PolicyType,
        fetcher: F,
    ) -> Result<Arc<CachedSecurityPolicy>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<RawPolicyDef>>,
    {
        let key = (tenant.clone(), policy_id);

        let cell = self
            .cache
            .entry(key)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();

        let label = policy_type.label();
        let result = cell
            .get_or_try_init(|| async {
                let raw = fetcher().await?;
                let tokens = tokenize_sql(&raw.body)?;
                let expr = parse_expr(&tokens, POLICY_DIALECT)?;
                info!(
                    "{}: loaded and parsed policy_id={} from meta store",
                    label, policy_id
                );
                Ok::<_, databend_common_exception::ErrorCode>(Arc::new(CachedSecurityPolicy {
                    expr,
                    args: raw
                        .args
                        .into_iter()
                        .map(|(k, v)| (k.to_lowercase(), v))
                        .collect(),
                }))
            })
            .await?;
        Ok(result.clone())
    }

    fn clear(&self) -> usize {
        let count = self.cache.len();
        if count > 0 {
            self.cache.clear();
        }
        count
    }
}

/// Unified cache manager for both row access policies and data masks.
///
/// Registered as a single `GlobalInstance`. Call sites use
/// `SecurityPolicyCacheManager::instance()` and select the cache via
/// [`PolicyType`].
pub struct SecurityPolicyCacheManager {
    row_access_policy: PolicyCache,
    data_mask: PolicyCache,
}

impl SecurityPolicyCacheManager {
    pub fn init() -> Result<()> {
        let mgr = Arc::new(Self {
            row_access_policy: PolicyCache::new(),
            data_mask: PolicyCache::new(),
        });

        // Start a single background cleanup task for both caches.
        let this = Arc::clone(&mgr);
        databend_common_base::runtime::spawn(async move {
            loop {
                tokio::time::sleep(CACHE_CLEANUP_INTERVAL).await;
                let rap = this.row_access_policy.clear();
                let dm = this.data_mask.clear();
                if rap > 0 {
                    debug!(
                        "row_access_policy_cache: cleared {} orphaned/stale entries",
                        rap
                    );
                }
                if dm > 0 {
                    debug!("data_mask_cache: cleared {} orphaned/stale entries", dm);
                }
            }
        });

        GlobalInstance::set(mgr);
        Ok(())
    }

    pub fn instance() -> Arc<SecurityPolicyCacheManager> {
        GlobalInstance::get()
    }

    fn cache_for(&self, policy_type: PolicyType) -> &PolicyCache {
        match policy_type {
            PolicyType::RowAccessPolicy => &self.row_access_policy,
            PolicyType::DataMask => &self.data_mask,
        }
    }

    /// Synchronous fast path — returns `Some` only when the parsed policy is
    /// already cached. The binder calls this first to avoid `block_on`
    /// overhead on cache hits.
    pub fn try_get(
        &self,
        policy_type: PolicyType,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Option<Arc<CachedSecurityPolicy>> {
        self.cache_for(policy_type).try_get(tenant, policy_id)
    }

    /// Invalidate one cached definition when the underlying policy is dropped.
    pub fn invalidate(&self, policy_type: PolicyType, tenant: &Tenant, policy_id: u64) {
        self.cache_for(policy_type).invalidate(tenant, policy_id);
    }

    /// Async path — used on cache miss. Guarantees singleflight: concurrent
    /// callers for the same `(tenant, policy_id)` share one in-flight RPC.
    /// Errors are never cached.
    ///
    /// The `fetcher` closure performs the feature-specific metastore RPC and
    /// returns the raw `(body, args)`. Tokenize, parse, and logging are
    /// handled uniformly by the cache layer.
    pub async fn get_or_load<F, Fut>(
        &self,
        policy_type: PolicyType,
        tenant: &Tenant,
        policy_id: u64,
        fetcher: F,
    ) -> Result<Arc<CachedSecurityPolicy>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<RawPolicyDef>>,
    {
        self.cache_for(policy_type)
            .get_or_load(tenant, policy_id, policy_type, fetcher)
            .await
    }

    /// Synchronous convenience method that combines the sync fast path with
    /// a `block_on` fallback for cache misses. This eliminates the repeated
    /// `try_get` → `block_on(get_or_load)` + logging pattern at call sites.
    pub fn get_cached_or_load_sync<F, Fut>(
        &self,
        policy_type: PolicyType,
        tenant: &Tenant,
        policy_id: u64,
        fetcher: F,
    ) -> Result<Arc<CachedSecurityPolicy>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<RawPolicyDef>>,
    {
        let start = std::time::Instant::now();
        let label = policy_type.label();

        if let Some(cached) = self.try_get(policy_type, tenant, policy_id) {
            debug!(
                "{}: policy_id={}, cache_hit, elapsed_ms={:.3}",
                label,
                policy_id,
                start.elapsed().as_secs_f64() * 1000.0,
            );
            return Ok(cached);
        }

        let loaded = databend_common_base::runtime::block_on(self.get_or_load(
            policy_type,
            tenant,
            policy_id,
            fetcher,
        ))?;
        info!(
            "{}: policy_id={}, cache_miss, fetch_ms={:.3}",
            label,
            policy_id,
            start.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;

    use super::*;

    fn test_tenant() -> Tenant {
        Tenant::new_literal("test")
    }

    fn ok_fetcher(
        body: &'static str,
    ) -> impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<RawPolicyDef>> + Send>> {
        move || {
            Box::pin(async move {
                Ok(RawPolicyDef {
                    body: body.to_string(),
                    args: vec![("a".to_string(), "INT".to_string())],
                })
            })
        }
    }

    fn err_fetcher()
    -> impl FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<RawPolicyDef>> + Send>> {
        || Box::pin(async { Err(ErrorCode::Internal("fetch failed")) })
    }

    #[tokio::test]
    async fn test_cache_hit_after_load() {
        let cache = PolicyCache::new();
        let tenant = test_tenant();

        // Cold miss.
        assert!(cache.try_get(&tenant, 1).is_none());

        // Load.
        cache
            .get_or_load(&tenant, 1, PolicyType::DataMask, ok_fetcher("a + 1"))
            .await
            .unwrap();

        // Warm hit.
        let hit = cache.try_get(&tenant, 1);
        assert!(hit.is_some());
        assert_eq!(hit.unwrap().args, vec![("a".into(), "INT".into())]);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache = PolicyCache::new();
        let tenant = test_tenant();

        cache
            .get_or_load(&tenant, 1, PolicyType::DataMask, ok_fetcher("a + 1"))
            .await
            .unwrap();
        cache
            .get_or_load(&tenant, 2, PolicyType::DataMask, ok_fetcher("a + 2"))
            .await
            .unwrap();

        cache.invalidate(&tenant, 1);

        // policy_id=1 gone, policy_id=2 still present.
        assert!(cache.try_get(&tenant, 1).is_none());
        assert!(cache.try_get(&tenant, 2).is_some());
    }

    #[tokio::test]
    async fn test_singleflight() {
        let cache = Arc::new(PolicyCache::new());
        let tenant = test_tenant();
        let call_count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let tenant = tenant.clone();
            let cc = call_count.clone();
            handles.push(databend_common_base::runtime::spawn(async move {
                cache
                    .get_or_load(&tenant, 1, PolicyType::DataMask, move || {
                        cc.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async {
                            Ok(RawPolicyDef {
                                body: "a + 1".to_string(),
                                args: vec![],
                            })
                        })
                    })
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // OnceCell guarantees only one fetcher runs.
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_error_not_cached() {
        let cache = PolicyCache::new();
        let tenant = test_tenant();

        // First call fails.
        let res = cache
            .get_or_load(&tenant, 1, PolicyType::DataMask, err_fetcher())
            .await;
        assert!(res.is_err());

        // Entry should not be cached — a retry with a good fetcher should succeed.
        let res = cache
            .get_or_load(&tenant, 1, PolicyType::DataMask, ok_fetcher("a + 1"))
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_clear() {
        let cache = PolicyCache::new();
        let tenant = test_tenant();

        cache
            .get_or_load(&tenant, 1, PolicyType::DataMask, ok_fetcher("a + 1"))
            .await
            .unwrap();
        cache
            .get_or_load(&tenant, 2, PolicyType::DataMask, ok_fetcher("a + 2"))
            .await
            .unwrap();

        let cleared = cache.clear();
        assert_eq!(cleared, 2);
        assert!(cache.try_get(&tenant, 1).is_none());
        assert!(cache.try_get(&tenant, 2).is_none());
    }
}
