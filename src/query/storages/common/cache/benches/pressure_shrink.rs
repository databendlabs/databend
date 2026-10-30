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

//! Cost of reclaiming a full 10 GiB pool under memory pressure: the whole pool, and one batch
//! (an upper bound of how long one shard lock is held). Dropping the store is not timed.
//! Run with `cargo bench -p databend-storages-common-cache --bench pressure_shrink`.

use std::cell::Cell;
use std::sync::Arc;

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::shared::SharedStore;

const POOL_BYTES: usize = 10 << 30;
const ENTRY_BYTES: usize = 1 << 20;
const SHARDS: usize = 16;

fn main() {
    divan::main()
}

/// Claims `ENTRY_BYTES` without allocating them, so the bench measures bookkeeping only.
struct Claim;

impl CachedItem for Claim {
    fn mem_bytes(&self) -> usize {
        ENTRY_BYTES
    }
}

fn full_pool() -> Arc<dyn KindStore> {
    let caps = PoolCaps {
        meta: 0,
        index: POOL_BYTES,
        data: 0,
    };
    let store: Arc<dyn KindStore> = Arc::new(SharedStore::new(caps, [], SHARDS));
    let cache = MemCache::new(store.clone(), CacheKind::BloomFilter);
    for i in 0..POOL_BYTES / ENTRY_BYTES {
        cache.put(&format!("1/2/_i_b/bloom_{i:08}.parquet"), Claim);
    }
    store
}

#[divan::bench(sample_count = 10)]
fn shrink_whole_pool(bencher: divan::Bencher) {
    bencher.with_inputs(full_pool).bench_values(|store| {
        let report = store.shrink_for_pressure(&|| false);
        // Return the store so its drop is not timed.
        (store, report)
    });
}

#[divan::bench(sample_count = 20)]
fn shrink_one_batch(bencher: divan::Bencher) {
    bencher.with_inputs(full_pool).bench_values(|store| {
        let first = Cell::new(true);
        let report = store.shrink_for_pressure(&|| first.replace(false));
        (store, report)
    });
}
