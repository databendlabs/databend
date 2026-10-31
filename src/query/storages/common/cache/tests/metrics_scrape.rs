// Copyright 2022 Datafuse Labs.
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

//! Scraping the global metrics registry right after the cache manager starts, before any cache
//! is used. Its own test binary: the hang this guards against needs metric statics that no other
//! test has touched yet in the process.

use std::sync::mpsc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_config::CacheConfig;
use databend_storages_common_cache::CacheManager;

/// The cache metrics collector runs under the registry's lock; reading a hit count there used
/// to register its metric family, which takes the same lock, and the scrape never returned.
#[test]
fn test_first_scrape_returns() {
    // Production mode: the scrape runs on another thread, as it does in the server.
    GlobalInstance::init_production();
    let config = CacheConfig {
        shared_memory_enabled: true,
        ..CacheConfig::default()
    };
    CacheManager::init(&config, &(1 << 30), "test_tenant", false).unwrap();

    let (done, scraped) = mpsc::channel();
    Thread::spawn(move || {
        done.send(GLOBAL_METRICS_REGISTRY.render_metrics().unwrap())
            .unwrap();
    });
    let text = scraped
        .recv_timeout(Duration::from_secs(30))
        .expect("the metrics scrape hangs");
    assert!(text.contains("cache_resident_bytes"));
    assert!(text.contains("cache_pool_capacity_bytes"));
}
