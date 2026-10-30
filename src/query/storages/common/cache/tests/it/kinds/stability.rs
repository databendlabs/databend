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

use std::sync::Arc;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::Unit;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use super::case::KindCase;
use super::case::cases;
use super::setup::Backend;
use super::setup::manager;

const THREADS: u64 = 8;
const OPS: usize = 2_000;

/// Threads mixing every kind on a small cache, while capacities change and memory pressure
/// reclaims. Once nobody holds a value, every byte charged is a byte cached, within capacity;
/// once every kind is turned off, nothing stays charged.
#[test]
fn test_every_kind_under_concurrency() {
    for backend in Backend::ALL {
        let manager = Arc::new(manager(&backend.small_config(), 8 << 20, false));
        let cases: Arc<Vec<Box<dyn KindCase>>> = Arc::new(cases());
        let threads: Vec<_> = (0..THREADS)
            .map(|seed| {
                let (manager, cases) = (manager.clone(), cases.clone());
                Thread::spawn(move || run(&manager, &cases, seed))
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        for row in manager
            .rows()
            .iter()
            .filter(|r| r.kind.is_some() && r.pool.is_some())
        {
            assert_eq!(row.size, row.in_cache_size, "{backend:?} {}", row.name);
            match (row.capacity, row.unit) {
                (Some(cap), Unit::Bytes) => assert!(row.in_cache_size <= cap, "{}", row.name),
                (Some(cap), Unit::Count) => assert!(row.num_items <= cap, "{}", row.name),
                (None, _) => {}
            }
        }
        for pool in manager.rows().iter().filter(|r| r.kind.is_none()) {
            assert!(pool.size <= pool.capacity.unwrap(), "{}", pool.name);
        }

        for kind in CacheKind::all() {
            manager.set_cache_capacity(kind.name(), 0).unwrap();
        }
        for row in manager.rows() {
            assert_eq!(
                (row.size, row.num_items),
                (0, 0),
                "{backend:?} {}",
                row.name
            );
        }
    }
}

fn run(manager: &CacheManager, cases: &[Box<dyn KindCase>], seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut held = Vec::new();
    for _ in 0..OPS {
        let case = &cases[rng.gen_range(0..cases.len())];
        match rng.gen_range(0..40) {
            0 => held.clear(),
            1 => {
                let capacity = [0, 8, 64 << 10, 1 << 20][rng.gen_range(0..4)];
                manager
                    .set_cache_capacity(case.kind().name(), capacity)
                    .unwrap();
            }
            2 => {
                manager.shrink_for_pressure(&|| false);
            }
            _ => case.random_op(manager, &mut rng, &mut held),
        }
        if held.len() > 32 {
            held.swap_remove(0);
        }
    }
}
