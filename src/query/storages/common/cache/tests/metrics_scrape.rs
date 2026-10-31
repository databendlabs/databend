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

//! The global metrics registry with the cache manager, before any cache metric is used. Its own
//! test binary: the hangs it guards against need metric statics that no other test has touched
//! yet in the process.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_config::CacheConfig;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::kinds::column;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

/// Reclaims cache memory while the registry is locked for a scrape, as an allocation made under
/// that lock would under memory pressure.
#[derive(Debug)]
struct ReclaimDuringScrape {
    freed: Arc<AtomicUsize>,
}

impl Collector for ReclaimDuringScrape {
    fn encode(&self, _encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let report = CacheManager::instance().shrink_for_pressure(&|| false);
        self.freed.fetch_add(report.freed, Ordering::Relaxed);
        Ok(())
    }
}

fn scrape() -> String {
    let (done, scraped) = mpsc::channel();
    Thread::spawn(move || {
        done.send(GLOBAL_METRICS_REGISTRY.render_metrics().unwrap())
            .unwrap();
    });
    scraped
        .recv_timeout(Duration::from_secs(30))
        .expect("the metrics scrape hangs")
}

/// Neither the cache metrics collector nor a reclaim may touch a metric while the registry is
/// locked: the first use of one registers it, which takes the registry lock again. Reading hit
/// counts in the collector once hung every scrape, and logins with it.
#[test]
fn test_scrape_and_reclaim_under_the_registry_lock() {
    // Production mode: the scrape runs on another thread, as it does in the server.
    GlobalInstance::init_production();
    let config = CacheConfig {
        shared_memory_enabled: true,
        kind_max_bytes: [("memory_cache_column_data".to_string(), 1 << 20)].into(),
        ..CacheConfig::default()
    };
    CacheManager::init(&config, &(1 << 30), "test_tenant", false).unwrap();
    let columns = CacheManager::instance().cache::<column::Data>();
    for i in 0..8 {
        columns.put(
            &i.to_string(),
            ColumnData::from_bytes(Bytes::from(vec![0; 4096])),
        );
    }

    let freed = Arc::new(AtomicUsize::new(0));
    GLOBAL_METRICS_REGISTRY.register_collector(Box::new(ReclaimDuringScrape {
        freed: freed.clone(),
    }));
    let text = scrape();
    assert!(freed.load(Ordering::Relaxed) >= 8 * 4096);
    assert!(text.contains("cache_resident_bytes"));
    assert!(text.contains("cache_pool_capacity_bytes"));
    assert!(text.contains("cache_memory_reclaim_count"));
}
