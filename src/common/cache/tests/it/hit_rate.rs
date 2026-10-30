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

//! Hit rate of `S3Fifo` against the existing `LruCache` on the same request streams.

use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_cache::MemSized;
use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

const CAPACITY: usize = 1_000;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key(u64);

impl MemSized for Key {
    fn mem_bytes(&self) -> usize {
        0
    }
}

struct One;

impl MemSized for One {
    fn mem_bytes(&self) -> usize {
        1
    }
}

trait Policy {
    /// Returns whether `key` was a hit; inserts it otherwise.
    fn access(&mut self, key: u64) -> bool;
}

struct Lru(LruCache<Key, One>);

impl Policy for Lru {
    fn access(&mut self, key: u64) -> bool {
        if self.0.get(&Key(key)).is_some() {
            return true;
        }
        self.0.insert(Key(key), One);
        false
    }
}

struct Fifo(S3Fifo<u64, ()>);

impl Policy for Fifo {
    fn access(&mut self, key: u64) -> bool {
        if self.0.get(&key).is_some() {
            return true;
        }
        self.0.admit(key, (), 1);
        while self.0.bytes() > CAPACITY {
            if self.0.pop_evictable(|_| false, usize::MAX).is_none() {
                break;
            }
        }
        false
    }
}

fn lru() -> Lru {
    Lru(LruCache::with_items_capacity(CAPACITY))
}

fn fifo() -> Fifo {
    Fifo(S3Fifo::new(S3FifoConfig {
        ghost_entries: CAPACITY,
        ..Default::default()
    }))
}

/// Zipf(1.0) sampler over `n` keys.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize) -> Self {
        let mut acc = 0.0;
        let mut cdf: Vec<f64> = (1..=n)
            .map(|rank| {
                acc += 1.0 / rank as f64;
                acc
            })
            .collect();
        cdf.iter_mut().for_each(|c| *c /= acc);
        Zipf { cdf }
    }

    fn sample(&self, rng: &mut SmallRng) -> u64 {
        let u: f64 = rng.r#gen();
        self.cdf.partition_point(|c| *c < u) as u64
    }
}

fn zipf_hit_rate(policy: &mut impl Policy) -> f64 {
    let zipf = Zipf::new(100 * CAPACITY);
    let mut rng = SmallRng::seed_from_u64(42);
    let requests = 200_000;
    let hits = (0..requests)
        .filter(|_| policy.access(zipf.sample(&mut rng)))
        .count();
    hits as f64 / requests as f64
}

/// Hot set that fits the cache, interrupted by one-off scans five times its size.
/// Measures hot-key hits right after each scan.
fn hot_hit_rate_under_scans(policy: &mut impl Policy) -> f64 {
    let hot = (CAPACITY / 2) as u64;
    let mut rng = SmallRng::seed_from_u64(7);
    let mut scan_key = 1_000_000u64;
    let (mut hits, mut total) = (0, 0);
    for round in 0..20 {
        for i in 0..5_000 {
            let hit = policy.access(rng.gen_range(0..hot));
            if round > 0 && i < hot {
                hits += usize::from(hit);
                total += 1;
            }
        }
        for _ in 0..5 * CAPACITY {
            policy.access(scan_key);
            scan_key += 1;
        }
    }
    hits as f64 / total as f64
}

#[test]
fn test_zipf_hit_rate_not_below_lru() {
    let (lru, fifo) = (zipf_hit_rate(&mut lru()), zipf_hit_rate(&mut fifo()));
    assert!(fifo >= lru, "s3fifo {fifo:.3} < lru {lru:.3}");
}

#[test]
fn test_hot_set_survives_scans_better_than_lru() {
    let lru = hot_hit_rate_under_scans(&mut lru());
    let fifo = hot_hit_rate_under_scans(&mut fifo());
    assert!(fifo > lru + 0.5, "s3fifo {fifo:.3} vs lru {lru:.3}");
}
