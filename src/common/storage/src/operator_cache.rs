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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::time::Instant;

use dashmap::DashMap;
use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_gauge;
use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::Gauge;
use databend_common_exception::Result as DatabendResult;
use databend_common_meta_app::storage::StorageParams;
use log::debug;
use log::info;
use opendal::Operator;

use crate::operator::init_operator_uncached;

static CACHE_HIT_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("storage_operator_cache_hit_total"));
static CACHE_MISS_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("storage_operator_cache_miss_total"));
static CACHE_SIZE: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("storage_operator_cache_size"));

#[derive(Clone)]
struct CachedOperator {
    operator: Operator,
    created_at: Instant,
    ttl: Duration,
}

/// OperatorCache provides caching for storage operators to avoid
/// frequent recreation and token refresh operations.
pub struct OperatorCache {
    cache: Arc<DashMap<u64, CachedOperator>>,
    default_ttl: Duration,
}

impl OperatorCache {
    pub fn new() -> Self {
        let default_ttl = std::env::var("DATABEND_OPERATOR_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(3000)); // Default 50 minutes

        info!("Initializing operator cache with TTL: {:?}", default_ttl);

        Self {
            cache: Arc::new(DashMap::new()),
            default_ttl,
        }
    }

    /// Get or create an operator from cache
    pub async fn get_or_create(&self, params: &StorageParams) -> DatabendResult<Operator> {
        let cache_key = self.compute_cache_key(params);

        // Check if we have a valid cached operator
        if let Some(entry) = self.cache.get(&cache_key) {
            if entry.created_at.elapsed() < entry.ttl {
                debug!("Operator cache hit for key: {}", cache_key);
                CACHE_HIT_COUNT.inc();
                return Ok(entry.operator.clone());
            } else {
                debug!("Operator cache expired for key: {}", cache_key);
                // Remove expired entry
                drop(entry);
                self.cache.remove(&cache_key);
                CACHE_SIZE.dec();
            }
        }

        // Cache miss, create new operator
        debug!("Operator cache miss for key: {}", cache_key);
        CACHE_MISS_COUNT.inc();

        let operator = init_operator_uncached(params)?;

        // Determine TTL based on storage type
        let ttl = self.get_ttl_for_params(params);

        let cached = CachedOperator {
            operator: operator.clone(),
            created_at: Instant::now(),
            ttl,
        };

        self.cache.insert(cache_key, cached);
        CACHE_SIZE.inc();

        Ok(operator)
    }

    /// Clear all cached operators
    pub fn clear(&self) {
        let size = self.cache.len();
        self.cache.clear();
        CACHE_SIZE.set(CACHE_SIZE.get() - size as i64);
        info!("Cleared {} operators from cache", size);
    }

    /// Remove specific operator from cache
    pub fn invalidate(&self, params: &StorageParams) {
        let cache_key = self.compute_cache_key(params);
        if self.cache.remove(&cache_key).is_some() {
            CACHE_SIZE.dec();
            debug!("Invalidated operator cache for key: {}", cache_key);
        }
    }

    /// Get current cache size
    pub fn size(&self) -> usize {
        self.cache.len()
    }

    /// Compute a stable hash key for storage params
    fn compute_cache_key(&self, params: &StorageParams) -> u64 {
        let mut hasher = DefaultHasher::new();
        params.hash(&mut hasher);
        hasher.finish()
    }

    /// Get TTL for specific storage params
    fn get_ttl_for_params(&self, params: &StorageParams) -> Duration {
        match params {
            // AWS STS tokens typically last 1 hour, cache for 50 minutes
            StorageParams::S3(cfg) if !cfg.role_arn.is_empty() => Duration::from_secs(3000),
            // GCS tokens might have different TTL
            StorageParams::Gcs(_) => Duration::from_secs(3300),
            // For storage without temporary tokens, cache longer
            StorageParams::Fs(_) => Duration::from_secs(86400), // 24 hours
            // Default TTL for other storage types
            _ => self.default_ttl,
        }
    }
}

impl Default for OperatorCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Global operator cache instance
pub fn get_operator_cache() -> Arc<OperatorCache> {
    static INSTANCE: LazyLock<Arc<OperatorCache>> =
        LazyLock::new(|| Arc::new(OperatorCache::new()));
    INSTANCE.clone()
}
