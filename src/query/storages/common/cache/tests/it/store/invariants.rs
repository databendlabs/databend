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

use std::sync::Arc;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::store::KindStore;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use crate::fakes::Live;
use crate::fakes::META;
use crate::fakes::META2;
use crate::fakes::cache;
use crate::fakes::shared_store;

const CAP: usize = 10_000;

fn run(store: Arc<dyn KindStore>, live: Live, seed: u64) {
    let a = cache(&store, META);
    let b = cache(&store, META2);
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut held = Vec::new();
    let mut held_b = Vec::new();
    for _ in 0..5_000 {
        let key = rng.gen_range(0..200).to_string();
        let bytes = rng.gen_range(1..200);
        let value = match rng.gen_range(0..8) {
            0 => Some(a.put(&key, live.item(bytes))),
            1 => Some(a.fill(&key, live.item(bytes))),
            2 => {
                held_b.push(b.put(&key, live.item(bytes)));
                None
            }
            3 => a.lookup(&key),
            4 => {
                b.evict(&key);
                None
            }
            5 => {
                let limit = if rng.gen_bool(0.5) {
                    KindLimit::UNLIMITED
                } else {
                    KindLimit::bytes(rng.gen_range(0..CAP))
                };
                store.set_kind_limit(CacheKind::SegmentInfo, limit);
                None
            }
            6 => {
                store.shrink_for_pressure(&|| true);
                None
            }
            _ => {
                held.clear();
                held_b.clear();
                None
            }
        };
        if let Some(value) = value
            && held.len() < 16
        {
            held.push(value);
        }
    }
}

#[test]
fn test_counters_stay_consistent_under_concurrency() {
    let store = shared_store(CAP, 0, 0);
    let live = Live::default();
    let threads: Vec<_> = (0..8)
        .map(|seed| {
            let (store, live) = (store.clone(), live.clone());
            Thread::spawn(move || run(store, live, seed))
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    store.set_kind_limit(CacheKind::SegmentInfo, KindLimit::UNLIMITED);
    cache(&store, META).put("last", live.item(1));
    let meta = &store.pool_stats()[0];
    assert_eq!(meta.resident, meta.in_cache);
    assert!(meta.in_cache <= CAP);
    assert_eq!(live.count(), meta.items);

    store.shrink_for_pressure(&|| false);
    let meta = &store.pool_stats()[0];
    assert_eq!((meta.resident, meta.in_cache, meta.items), (0, 0, 0));
    assert_eq!(live.count(), 0);
}
