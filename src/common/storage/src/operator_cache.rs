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

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::Gauge;
use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_gauge;
use databend_common_exception::Result as DatabendResult;
use databend_common_meta_app::storage::StorageParams;
use log::debug;
use log::info;
use lru::LruCache;
use opendal::Operator;

use crate::operator::init_operator_uncached;

// Internal metrics for monitoring cache effectiveness
static CACHE_HIT_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("storage_operator_cache_hit"));
static CACHE_MISS_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("storage_operator_cache_miss"));
static CACHE_SIZE: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("storage_operator_cache_size"));

const DEFAULT_CACHE_SIZE: usize = 1024;

/// OperatorCache provides caching for storage operators to avoid
/// frequent recreation and token refresh operations.
pub(crate) struct OperatorCache {
    cache: Arc<Mutex<LruCache<StorageParams, Operator>>>,
}

impl OperatorCache {
    pub(crate) fn new() -> Self {
        let cache_size = std::env::var("DATABEND_OPERATOR_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CACHE_SIZE);

        info!("Initializing operator cache with size: {}", cache_size);

        Self {
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_size).expect("cache size must be greater than 0"),
            ))),
        }
    }

    /// Get or create an operator from cache
    pub(crate) fn get_or_create(&self, params: &StorageParams) -> DatabendResult<Operator> {
        // Check if we have a cached operator
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(operator) = cache.get(params) {
                debug!("Operator cache hit for params: {:?}", params);
                CACHE_HIT_COUNT.inc();
                return Ok(operator.clone());
            }
        }

        // Cache miss, create new operator
        debug!("Operator cache miss for params: {:?}", params);
        CACHE_MISS_COUNT.inc();

        let operator = init_operator_uncached(params)?;

        // Insert into cache
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put(params.clone(), operator.clone());
            CACHE_SIZE.set(cache.len() as i64);
        }

        Ok(operator)
    }
}

impl Default for OperatorCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Global operator cache instance
pub(crate) fn get_operator_cache() -> Arc<OperatorCache> {
    static INSTANCE: LazyLock<Arc<OperatorCache>> =
        LazyLock::new(|| Arc::new(OperatorCache::new()));
    INSTANCE.clone()
}
