// Copyright 2026 Datafuse Labs.
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

//! Kinds share a pool with no per-kind budget: S3-FIFO keeps what is read again. One-time
//! values (a bloom filter scan) pass through probation, while a set of values read again and
//! again (HNSW indexes) stays cached, as separate caches per kind used to ensure.

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::vector;

use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::shared_store;

const CAP: usize = 100_000;
const HOT: usize = 20;
const HOT_BYTES: usize = 1000;
const SCAN_PER_ROUND: usize = 50;
const SCAN_BYTES: usize = 500;
const ROUNDS: usize = 200;
const WARMUP: usize = 20;

#[test]
fn test_hot_index_survives_scans_of_another_kind() {
    let store = shared_store(0, CAP, 0);
    let (bloom, hnsw) = (
        CacheKind::of::<bloom::Filter>(),
        CacheKind::of::<vector::Index>(),
    );
    let (scans, hot) = (cache(&store, bloom), cache(&store, hnsw));
    let live = Live::default();
    let (mut hits, mut reads) = (0, 0);
    for round in 0..ROUNDS {
        // Each round scans a quarter of the pool of bloom filters never read again.
        for i in 0..SCAN_PER_ROUND {
            scans.put(&format!("bloom-{round}-{i}"), live.item(SCAN_BYTES));
        }
        // Read-through of the hot set: a miss loads the value again.
        for i in 0..HOT {
            let key = format!("hnsw-{i}");
            let hit = hot.lookup(&key).is_some();
            if !hit {
                hot.put(&key, live.item(HOT_BYTES));
            }
            if round >= WARMUP {
                reads += 1;
                hits += usize::from(hit);
            }
        }
    }
    let hit_rate = hits as f64 / reads as f64;
    assert!(hit_rate >= 0.95, "hot hit rate {hit_rate}");

    // The scans still use the rest of the pool: the pool is shared, not split.
    let scanned = store.kind_stats(bloom).in_cache;
    assert!(scanned > 0, "bloom filters were not cached");
    let pool = store.pool_stats()[Pool::Index.index()];
    assert!(pool.in_cache <= CAP && pool.in_cache > CAP / 2, "{pool:?}");
}
