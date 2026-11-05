// Copyright 2026 Datafuse Labs
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

use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::shared::SharedStore;

use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::data_kind;
use crate::fakes::index_kind;
use crate::fakes::meta_kind;
use crate::fakes::shared_store;

fn resident(store: &dyn databend_storages_common_cache::backend::KindStore, pool: Pool) -> usize {
    store
        .pool_stats()
        .into_iter()
        .find(|s| s.pool == pool)
        .unwrap()
        .resident
}

#[test]
fn test_shrink_order_and_early_stop() {
    let store = shared_store(10_000, 10_000, 10_000);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    let index = cache(&store, index_kind());
    let data = cache(&store, data_kind());
    for i in 0..10 {
        meta.put(&i.to_string(), live.item(100));
        index.put(&i.to_string(), live.item(100));
        data.put(&i.to_string(), live.item(100));
    }

    let report = store.shrink_for_pressure(&|| resident(&*store, Pool::Data) == 0);
    assert_eq!(report.freed, 1000);
    assert_eq!(resident(&*store, Pool::Index), 1000);
    assert_eq!(resident(&*store, Pool::Meta), 1000);

    let report = store.shrink_for_pressure(&|| false);
    assert_eq!(report.freed, 2000);
    assert_eq!(live.count(), 0);
}

#[test]
fn test_shrink_skips_held_values() {
    let store = shared_store(10_000, 0, 0);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    let held = meta.put("held", live.item(100));
    meta.put("free", live.item(50));

    let report = store.shrink_for_pressure(&|| false);
    assert_eq!((report.freed, report.evicted), (50, 1));
    assert!(meta.contains("held"));
    drop(held);
    assert_eq!(store.shrink_for_pressure(&|| false).freed, 100);
}

#[test]
fn test_relieved_is_checked_after_each_batch() {
    let store = shared_store(10_000, 0, 0);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    for i in 0..100 {
        meta.put(&i.to_string(), live.item(10));
    }
    let checks = Cell::new(0);
    let report = store.shrink_for_pressure(&|| {
        checks.set(checks.get() + 1);
        false
    });
    assert_eq!(report.freed, 1000);
    assert_eq!(report.batches, checks.get());
}

/// Test-only scheduling point: pause one reclaim after it removes its batch but before its
/// values have finished dropping, allowing another reclaim's charges to be released first.
struct DropPause {
    armed: AtomicBool,
    started: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
}

struct PausedValue(Arc<DropPause>);

impl CachedItem for PausedValue {
    fn mem_bytes(&self) -> usize {
        100
    }
}

impl Drop for PausedValue {
    fn drop(&mut self) {
        if self.0.armed.swap(false, Ordering::Relaxed) {
            self.0.started.send(()).unwrap();
            self.0
                .resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .expect("reclaim was not resumed");
        }
    }
}

#[test]
fn test_concurrent_reclaims_count_only_their_own_charges() {
    let store = Arc::new(SharedStore::new(
        PoolCaps {
            meta: 100_000,
            index: 0,
            data: 0,
        },
        [],
        1,
    ));
    let cache = MemCache::new(store.clone(), meta_kind());
    let (started, waiting) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let pause = Arc::new(DropPause {
        armed: AtomicBool::new(false),
        started,
        resume: Mutex::new(resumed),
    });
    for i in 0..128 {
        cache.put(&i.to_string(), PausedValue(pause.clone()));
    }
    pause.armed.store(true, Ordering::Relaxed);
    let (finished, result) = mpsc::channel();
    let first_store = store.clone();
    let thread = Thread::spawn(move || {
        finished
            .send(first_store.shrink_for_pressure(&|| true))
            .unwrap();
    });
    waiting.recv_timeout(Duration::from_secs(10)).unwrap();
    let second = store.shrink_for_pressure(&|| true);
    resume.send(()).unwrap();
    let first = result.recv_timeout(Duration::from_secs(10)).unwrap();
    thread.join().unwrap();

    assert!(first.evicted > 0 && second.evicted > 0);
    assert_eq!(first.evicted + second.evicted, 128);
    assert_eq!(first.freed + second.freed, 128 * 100);
    assert_eq!(store.reclaim_stats().unwrap().freed_bytes, 128 * 100);
    assert_eq!(store.kind_stats(meta_kind()).resident, 0);
}
