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

//! The shared backend must not hang: reclaim runs inside allocations on any thread, possibly
//! one in the middle of a cache operation or a metrics scrape. Each test runs under a deadline
//! and fails instead of hanging.

use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CacheMetricsCollector;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::fakes::data_kind;
use crate::fakes::index_kind;
use crate::fakes::meta_kind;
use crate::fakes::shared_store;
use crate::kinds::samples::Sample;
use crate::kinds::setup::Backend;
use crate::kinds::setup::manager;

/// Runs `f` on another thread and fails if it does not return within `secs` seconds.
fn within(secs: u64, f: impl FnOnce() + Send + 'static) {
    let (done, finished) = mpsc::channel();
    Thread::spawn(move || {
        f();
        let _ = done.send(());
    });
    finished
        .recv_timeout(Duration::from_secs(secs))
        .expect("deadlock: the test did not finish in time");
}

/// A value that reclaims cache memory when dropped, as an allocation under memory pressure in
/// its destructor would.
struct Reclaiming {
    store: Weak<dyn KindStore>,
    reclaims: Arc<AtomicUsize>,
}

impl CachedItem for Reclaiming {
    fn mem_bytes(&self) -> usize {
        100
    }
}

impl Drop for Reclaiming {
    fn drop(&mut self) {
        if let Some(store) = self.store.upgrade() {
            store.shrink_for_pressure(&|| false);
            self.reclaims.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Values dropped while the store evicts, replaces or reclaims start a reclaim of their own.
#[test]
fn test_reclaim_from_dropped_values() {
    within(30, || {
        let store = shared_store(1000, 1000, 1000);
        let reclaims = Arc::new(AtomicUsize::new(0));
        let value = || Reclaiming {
            store: Arc::downgrade(&store),
            reclaims: reclaims.clone(),
        };
        let caches: Vec<MemCache<Reclaiming>> = [meta_kind(), index_kind(), data_kind()]
            .into_iter()
            .map(|kind| MemCache::new(store.clone(), kind))
            .collect();
        for i in 0..200 {
            let cache = &caches[i % 3];
            let key = (i % 30).to_string();
            cache.put(&key, value());
            if i % 7 == 0 {
                cache.evict(&key);
            }
        }
        store.shrink_for_pressure(&|| false);
        assert!(reclaims.load(Ordering::Relaxed) > 0);
    });
}

/// Traffic on every pool, reclaims, capacity changes and scrapes, all at once.
#[test]
fn test_concurrent_reclaim_scrape_and_traffic() {
    within(60, || {
        let manager = Arc::new(manager(&Backend::Shared.small_config(), 8 << 20, false));
        let stop = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::new();
        for seed in 0..6u64 {
            let (manager, stop) = (manager.clone(), stop.clone());
            threads.push(Thread::spawn(move || traffic(&manager, &stop, seed)));
        }
        {
            let (manager, stop) = (manager.clone(), stop.clone());
            threads.push(Thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    manager.shrink_for_pressure(&|| false);
                }
            }));
        }
        {
            let (manager, stop) = (manager.clone(), stop.clone());
            threads.push(Thread::spawn(move || {
                let mut registry = Registry::default();
                registry.register_collector(Box::new(CacheMetricsCollector::of(manager.clone())));
                while !stop.load(Ordering::Relaxed) {
                    let mut text = String::new();
                    encode(&mut text, &registry).unwrap();
                    assert!(!manager.rows().is_empty());
                }
            }));
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        stop.store(true, Ordering::Relaxed);
        for thread in threads {
            thread.join().unwrap();
        }
    });
}

fn traffic(manager: &CacheManager, stop: &AtomicBool, seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let snapshots = manager.cache::<table::Snapshot>();
    let segments = manager.cache::<segment::Info>();
    let columns = manager.cache::<column::Data>();
    let mut held = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let key = rng.gen_range(0..64).to_string();
        match rng.gen_range(0..8) {
            0 => drop(snapshots.put(&key, table::Snapshot::sample(rng.r#gen()))),
            1 => drop(segments.fill(&key, segment::Info::sample(rng.r#gen()))),
            2 => held.push(columns.put(&key, column::Data::sample(rng.r#gen()))),
            3 => drop(snapshots.lookup(&key)),
            4 => drop(columns.lookup(&key)),
            5 => {
                segments.evict(&key);
            }
            6 => {
                let kind = CacheKind::of::<column::Data>();
                let capacity = [0, 64 << 10, 1 << 20][rng.gen_range(0..3)];
                manager.set_cache_capacity(kind.name(), capacity).unwrap();
            }
            _ => held.clear(),
        }
        if held.len() > 16 {
            held.remove(0);
        }
    }
}
