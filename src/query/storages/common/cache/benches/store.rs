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

//! Hit-path cost of the current `InMemoryLruCache` versus `MemCache` on both new backends.
//! Run with `cargo bench -p databend-storages-common-cache --bench store`.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::LazyLock;

use bytes::Bytes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::store::KindStore;
use databend_storages_common_cache::store::LegacyCapacity;
use databend_storages_common_cache::store::LegacyStore;
use databend_storages_common_cache::store::SharedStore;
use divan::black_box;

const KEYS: usize = 10_000;
const VALUE_BYTES: usize = 64;
const CAP: usize = 1 << 30;
const KIND: CacheKind = CacheKind::SegmentInfo;
const THREADS: [usize; 4] = [1, 8, 32, 64];

fn main() {
    divan::main()
}

static KEY_NAMES: LazyLock<Vec<String>> = LazyLock::new(|| {
    (0..KEYS)
        .map(|i| format!("1/2/_sg/segment_{i:08}.mpk"))
        .collect()
});

fn value() -> ColumnData {
    ColumnData::from_bytes(Bytes::from(vec![0u8; VALUE_BYTES]))
}

thread_local! {
    static NEXT: Cell<usize> = const { Cell::new(0) };
}

fn next_key() -> &'static str {
    let i = NEXT.with(|n| {
        let i = n.get();
        n.set(i.wrapping_add(7919));
        i
    });
    &KEY_NAMES[i % KEYS]
}

static CURRENT: LazyLock<InMemoryLruCache<ColumnData>> = LazyLock::new(|| {
    let cache = InMemoryLruCache::with_bytes_capacity("bench".to_string(), CAP);
    for key in KEY_NAMES.iter() {
        cache.insert(key.clone(), value());
    }
    cache
});

fn filled(store: Arc<dyn KindStore>) -> MemCache<ColumnData> {
    let cache = MemCache::new(store, KIND);
    for key in KEY_NAMES.iter() {
        cache.put(key, value());
    }
    cache
}

static LEGACY: LazyLock<MemCache<ColumnData>> = LazyLock::new(|| {
    filled(Arc::new(LegacyStore::new([(
        KIND,
        LegacyCapacity::Bytes(CAP),
    )])))
});

static SHARED: LazyLock<MemCache<ColumnData>> = LazyLock::new(|| {
    let caps = PoolCaps {
        meta: CAP,
        index: 0,
        data: 0,
    };
    let shards = std::thread::available_parallelism().map_or(8, |n| n.get().max(8));
    filled(Arc::new(SharedStore::new(caps, [], shards)))
});

#[divan::bench(threads = THREADS, sample_count = 2000, sample_size = 200)]
fn hit_current(bencher: divan::Bencher) {
    LazyLock::force(&CURRENT);
    bencher.bench(|| black_box(CURRENT.get(next_key())));
}

#[divan::bench(threads = THREADS, sample_count = 2000, sample_size = 200)]
fn hit_legacy(bencher: divan::Bencher) {
    LazyLock::force(&LEGACY);
    bencher.bench(|| black_box(LEGACY.lookup(next_key())));
}

#[divan::bench(threads = THREADS, sample_count = 2000, sample_size = 200)]
fn hit_shared(bencher: divan::Bencher) {
    LazyLock::force(&SHARED);
    bencher.bench(|| black_box(SHARED.lookup(next_key())));
}
