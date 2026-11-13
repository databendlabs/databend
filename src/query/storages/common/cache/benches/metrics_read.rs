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

//! What reading the cache metrics costs the caches: the hit path with and without a thread
//! reading the metrics without pause, far more often than a scrape or `system.metrics` query,
//! and the cost of one read. Run with
//! `cargo bench -p databend-storages-common-cache --bench metrics_read`.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadJoinHandle;
use databend_common_base::runtime::metrics::MetricSample;
use databend_common_base::runtime::metrics::SampleCollector;
use databend_common_config::CacheConfig;
use databend_storages_common_cache::CacheDef;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CacheMetricsCollector;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::kinds::column;
use divan::black_box;

const KEYS: usize = 10_000;
const VALUE_BYTES: usize = 64;
const CAP: u64 = 1 << 30;
const MEMORY: u64 = 8 << 30;
const THREADS: [usize; 3] = [1, 8, 32];

fn main() {
    divan::main()
}

static KEY_NAMES: LazyLock<Vec<String>> = LazyLock::new(|| {
    (0..KEYS)
        .map(|i| format!("1/2/_b/block_{i:08}.parquet"))
        .collect()
});

static THREAD_START: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static NEXT: Cell<usize> = Cell::new(THREAD_START.fetch_add(1237, Ordering::Relaxed));
}

fn next_key() -> &'static str {
    let i = NEXT.with(|n| {
        let i = n.get();
        n.set(i.wrapping_add(7919));
        i
    });
    &KEY_NAMES[i % KEYS]
}

struct Filled {
    manager: Arc<CacheManager>,
    cache: MemCache<ColumnData>,
}

fn filled(shared: bool) -> Filled {
    let config = if shared {
        CacheConfig {
            shared_memory_enabled: true,
            kind_max_bytes: [(column::Data::NAME.to_string(), CAP)].into(),
            ..CacheConfig::default()
        }
    } else {
        CacheConfig {
            shared_memory_enabled: false,
            table_data_deserialized_data_bytes: CAP,
            ..CacheConfig::default()
        }
    };
    let manager = Arc::new(CacheManager::try_new(&config, &MEMORY, "bench", false).unwrap());
    if !shared {
        manager.set_cache_capacity(column::Data::NAME, CAP).unwrap();
    }
    let cache = manager.cache::<column::Data>();
    for key in KEY_NAMES.iter() {
        cache.put(
            key,
            ColumnData::from_bytes(Bytes::from(vec![0u8; VALUE_BYTES])),
        );
    }
    Filled { manager, cache }
}

static LEGACY: LazyLock<Filled> = LazyLock::new(|| filled(false));
static SHARED: LazyLock<Filled> = LazyLock::new(|| filled(true));

/// Reads the metrics of `manager` in a loop until dropped.
struct Reader {
    stop: Arc<AtomicBool>,
    thread: Option<ThreadJoinHandle<u64>>,
}

impl Reader {
    fn start(manager: &Arc<CacheManager>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let collector = CacheMetricsCollector::of(manager.clone());
        let thread = {
            let stop = stop.clone();
            Thread::spawn(move || {
                let mut reads = 0;
                let mut samples: Vec<MetricSample> = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    samples.clear();
                    collector.sample(&mut samples);
                    black_box(&samples);
                    reads += 1;
                }
                reads
            })
        };
        Reader {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let reads = thread.join().unwrap();
            assert!(reads > 0, "the metrics reader never ran");
        }
    }
}

fn hit(bencher: divan::Bencher, filled: &'static Filled, reading: bool) {
    let _reader = reading.then(|| Reader::start(&filled.manager));
    bencher.bench(|| black_box(filled.cache.lookup(next_key())));
}

#[divan::bench(threads = THREADS, args = [false, true], sample_count = 2000, sample_size = 200)]
fn hit_legacy(bencher: divan::Bencher, reading: bool) {
    hit(bencher, &LEGACY, reading);
}

#[divan::bench(threads = THREADS, args = [false, true], sample_count = 2000, sample_size = 200)]
fn hit_shared(bencher: divan::Bencher, reading: bool) {
    hit(bencher, &SHARED, reading);
}

fn read(bencher: divan::Bencher, filled: &'static Filled) {
    let collector = CacheMetricsCollector::of(filled.manager.clone());
    bencher.bench(|| {
        let mut samples = Vec::new();
        collector.sample(&mut samples);
        black_box(samples)
    });
}

#[divan::bench(sample_count = 200)]
fn read_legacy(bencher: divan::Bencher) {
    read(bencher, &LEGACY);
}

#[divan::bench(sample_count = 200)]
fn read_shared(bencher: divan::Bencher) {
    read(bencher, &SHARED);
}
