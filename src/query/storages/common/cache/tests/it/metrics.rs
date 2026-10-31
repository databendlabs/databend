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

//! The cache metrics: gauges of what each kind and pool holds, and the eviction, admission,
//! rejection and reclaim counters of the shared backend.

use std::sync::Arc;

use databend_common_metrics::cache::get_cache_admission_deferred_count;
use databend_common_metrics::cache::get_cache_eviction_bytes;
use databend_common_metrics::cache::get_cache_eviction_count;
use databend_common_metrics::cache::get_cache_insert_rejected_count;
use databend_common_metrics::cache::get_cache_memory_reclaim_bytes;
use databend_common_metrics::cache::get_cache_memory_reclaim_count;
use databend_storages_common_cache::CacheMetricsCollector;
use databend_storages_common_cache::CacheRow;
use databend_storages_common_cache::kinds::table;
use futures::executor::block_on;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;

use crate::fakes::FakeReader;
use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::data_kind;
use crate::fakes::meta_kind;
use crate::fakes::shared_store;
use crate::kinds::samples::Sample;
use crate::kinds::setup::Backend;
use crate::kinds::setup::manager;

const MEMORY: u64 = 1 << 30;

fn scrape(collector: CacheMetricsCollector) -> String {
    let mut registry = Registry::default();
    registry.register_collector(Box::new(collector));
    let mut text = String::new();
    encode(&mut text, &registry).unwrap();
    text
}

fn sample(text: &str, name: &str, labels: &str) -> Option<u64> {
    let prefix = format!("{name}{{{labels}}} ");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .map(|value| value.parse().unwrap())
}

fn kind_labels(row: &CacheRow) -> String {
    let pool = row.pool.map_or("disk", |pool| pool.name());
    format!("cache_name=\"{}\",pool=\"{pool}\"", row.name)
}

/// Every row of `system.caches` is in the gauges, with the same numbers.
#[test]
fn test_gauges_follow_cache_rows() {
    for backend in Backend::ALL {
        let manager = Arc::new(manager(&backend.config(MEMORY), MEMORY, false));
        let snapshots = manager.cache::<table::Snapshot>();
        let _held = snapshots.put("a", table::Snapshot::sample(1));
        snapshots.put("b", table::Snapshot::sample(2));
        snapshots.evict("a");

        let text = scrape(CacheMetricsCollector::of(manager.clone()));
        let rows = manager.rows();
        for row in rows.iter().filter(|row| row.kind.is_some()) {
            let labels = kind_labels(row);
            let get = |name| sample(&text, name, &labels);
            assert_eq!(get("cache_items"), Some(row.num_items), "{}", row.name);
            assert_eq!(get("cache_resident_bytes"), Some(row.size), "{}", row.name);
            assert_eq!(
                get("cache_in_cache_bytes"),
                Some(row.in_cache_size),
                "{}",
                row.name
            );
            assert_eq!(
                get("cache_enabled"),
                Some(u64::from(row.enabled)),
                "{}",
                row.name
            );
            let capacity = sample(
                &text,
                "cache_capacity",
                &format!("{labels},unit=\"{}\"", row.unit),
            );
            assert_eq!(capacity, row.capacity, "{}", row.name);
        }
        let pools: Vec<_> = rows.iter().filter(|row| row.kind.is_none()).collect();
        assert_eq!(pools.is_empty(), backend == Backend::Legacy);
        for row in pools {
            let labels = format!("pool=\"{}\"", row.pool.unwrap().name());
            let get = |name| sample(&text, name, &labels);
            assert_eq!(get("cache_pool_items"), Some(row.num_items));
            assert_eq!(get("cache_pool_resident_bytes"), Some(row.size));
            assert_eq!(get("cache_pool_in_cache_bytes"), Some(row.in_cache_size));
            assert_eq!(get("cache_pool_capacity_bytes"), row.capacity);
        }

        // The held, evicted snapshot is charged but no longer cached.
        let snapshot = rows
            .iter()
            .find(|row| row.name == "memory_cache_table_snapshot")
            .unwrap();
        assert_eq!(snapshot.num_items, 1);
        assert!(snapshot.size > snapshot.in_cache_size);
    }
}

/// The counters are global and other tests move them too, so these check increases only.
#[test]
fn test_shared_store_counters() {
    let name = meta_kind().name();
    let store = shared_store(1000, 0, 1000);
    let meta = cache(&store, meta_kind());
    let live = Live::default();

    // Room for new entries.
    let (count, bytes) = (
        get_cache_eviction_count(name, "capacity"),
        get_cache_eviction_bytes(name, "capacity"),
    );
    for i in 0..15 {
        meta.put(&i.to_string(), live.item(100));
    }
    assert!(get_cache_eviction_count(name, "capacity") >= count + 5);
    assert!(get_cache_eviction_bytes(name, "capacity") >= bytes + 500);

    // Reclaimed under memory pressure.
    let (count, reclaims, freed) = (
        get_cache_eviction_count(name, "memory_pressure"),
        get_cache_memory_reclaim_count(),
        get_cache_memory_reclaim_bytes(),
    );
    let report = store.shrink_for_pressure(&|| false);
    assert_eq!(report.evicted, 10);
    assert!(get_cache_eviction_count(name, "memory_pressure") >= count + 10);
    assert!(get_cache_memory_reclaim_count() > reclaims);
    assert!(get_cache_memory_reclaim_bytes() >= freed + 1000);

    // Rejected: too large for the pool.
    let oversized = get_cache_insert_rejected_count(name, "oversized");
    assert!(!meta.put("big", live.item(251)).is_charged());
    assert!(get_cache_insert_rejected_count(name, "oversized") > oversized);

    // A data value is cached on its second read; the first is deferred.
    let data = cache(&store, data_kind());
    let deferred = get_cache_admission_deferred_count(data_kind().name());
    let reader = FakeReader::new(100);
    block_on(data.get(&reader, "k".to_string())).unwrap();
    assert!(get_cache_admission_deferred_count(data_kind().name()) > deferred);
}
