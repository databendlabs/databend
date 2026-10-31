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

//! Phased churn on the shared backend, checking memory control while it runs and the whole
//! accounting between phases. Self-contained: the short test in `shared::soak` and the long
//! soak in `tests/soak.rs` both include it.
//!
//! Worker threads mix every operation on kinds of all three pools: puts, fills, lookups, loads
//! through a reader (with the data pool's admission), evictions, held values, reclaims, and
//! limit changes on one kind. A monitor checks, the whole time, that no pool and no kind with
//! a fixed limit is ever charged more than its budget, held values included. Between phases
//! the workers drop what they hold and stop; the accounting must then be exact, the internal
//! structures bounded, and every value alive must be one the cache holds. Pool caps change
//! between phases.

// Parts are only used by the long soak.
#![allow(dead_code)]

use std::borrow::Cow;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::Thread;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::shared::PoolStructure;
use databend_storages_common_cache::shared::SharedStore;
use futures::executor::block_on;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

/// How long and how hard to churn.
#[derive(Clone, Copy, Debug)]
pub struct SoakConfig {
    pub threads: usize,
    pub phases: Phases,
    pub ops_per_phase: usize,
    /// Starting cap of each pool, in bytes.
    pub cap: usize,
    pub keys: usize,
    pub seed: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum Phases {
    Count(usize),
    For(Duration),
}

/// What one phase did and left, checked before the next phase starts.
#[derive(Clone, Debug)]
pub struct PhaseReport {
    pub phase: usize,
    pub ops: u64,
    pub elapsed: Duration,
    pub caps: [usize; 3],
    pub in_cache: [usize; 3],
    pub structures: [PoolStructure; 3],
    pub live_values: usize,
    pub monitor_samples: u64,
}

impl PhaseReport {
    pub fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.elapsed.as_secs_f64()
    }
}

/// A barrier that fails instead of waiting forever, so a thread that panicked or hangs fails the
/// soak rather than hanging it.
struct Gate {
    parties: usize,
    state: Mutex<(u64, usize)>,
    turned: Condvar,
}

const GATE_TIMEOUT: Duration = Duration::from_secs(120);

impl Gate {
    fn new(parties: usize) -> Self {
        Gate {
            parties,
            state: Mutex::new((0, 0)),
            turned: Condvar::new(),
        }
    }

    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        let generation = state.0;
        state.1 += 1;
        if state.1 == self.parties {
            *state = (generation + 1, 0);
            self.turned.notify_all();
            return;
        }
        let (_state, timeout) = self
            .turned
            .wait_timeout_while(state, GATE_TIMEOUT, |s| s.0 == generation)
            .unwrap();
        assert!(
            !timeout.timed_out(),
            "a soak thread did not reach the end of the phase: it panicked or hangs"
        );
    }
}

/// Values of every kind in the soak, counting how many are alive and their bytes.
#[derive(Default)]
struct Live {
    values: AtomicUsize,
    bytes: AtomicUsize,
}

struct Value {
    bytes: usize,
    live: Arc<Live>,
}

impl Value {
    fn new(live: &Arc<Live>, bytes: usize) -> Self {
        live.values.fetch_add(1, Ordering::Relaxed);
        live.bytes.fetch_add(bytes, Ordering::Relaxed);
        Value {
            bytes,
            live: live.clone(),
        }
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        self.live.values.fetch_sub(1, Ordering::Relaxed);
        self.live.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

impl CachedItem for Value {
    fn mem_bytes(&self) -> usize {
        self.bytes
    }
}

struct ValueReader {
    live: Arc<Live>,
    bytes: usize,
}

impl Reader for ValueReader {
    type Key = String;
    type Item = Value;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, _key: &String) -> Result<Value> {
        Ok(Value::new(&self.live, self.bytes))
    }
}

/// Kinds in the soak: two per pool. `FIXED` keeps a byte limit the monitor checks; workers keep
/// changing the limit of `LIMITED`, which the monitor therefore does not check.
fn kinds() -> [CacheKind; 6] {
    [
        CacheKind::of::<table::Snapshot>(),
        CacheKind::of::<segment::Info>(),
        CacheKind::of::<bloom::Filter>(),
        CacheKind::of::<inverted::Lookup>(),
        CacheKind::of::<column::Data>(),
        CacheKind::of::<column::Array>(),
    ]
}

const FIXED: usize = 1;
const LIMITED: usize = 3;
const LIMIT: usize = 1 << 20;

/// Runs the soak, calling `on_phase` after each phase's checks pass.
pub fn soak(config: SoakConfig, mut on_phase: impl FnMut(&PhaseReport)) -> Vec<PhaseReport> {
    let cap = config.cap;
    let kinds = kinds();
    let mut limits: Vec<_> = kinds.iter().map(|k| (*k, KindLimit::UNLIMITED)).collect();
    limits[FIXED].1 = KindLimit::bytes(LIMIT);
    limits[LIMITED].1 = KindLimit::bytes(LIMIT);
    let store: Arc<SharedStore> = Arc::new(SharedStore::new(
        PoolCaps {
            meta: cap,
            index: cap,
            data: cap,
        },
        limits,
        4,
    ));
    let live = Arc::new(Live::default());
    let ops = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));
    // Workers done with the current phase; the monitor checks until all are.
    let done = Arc::new(AtomicUsize::new(0));
    // Workers and the monitor wait at the start and at the end of each phase.
    let barrier = Arc::new(Gate::new(config.threads + 2));
    // What the monitor saw over budget; it records rather than panics, so the soak can stop.
    let violations = Arc::new(Mutex::new(Vec::new()));

    let workers: Vec<_> = (0..config.threads)
        .map(|i| {
            let shared = Shared {
                store: store.clone(),
                live: live.clone(),
                ops: ops.clone(),
                running: running.clone(),
                done: done.clone(),
                barrier: barrier.clone(),
                violations: violations.clone(),
            };
            let seed = config.seed * 1000 + i as u64;
            Thread::spawn(move || worker(shared, config, seed))
        })
        .collect();
    let monitor_samples = Arc::new(AtomicU64::new(0));
    let monitor = {
        let shared = Shared {
            store: store.clone(),
            live: live.clone(),
            ops: ops.clone(),
            running: running.clone(),
            done: done.clone(),
            barrier: barrier.clone(),
            violations: violations.clone(),
        };
        let samples = monitor_samples.clone();
        Thread::spawn(move || monitor(shared, config.threads, samples))
    };

    let mut rng = SmallRng::seed_from_u64(config.seed);
    let started = Instant::now();
    let mut reports = Vec::new();
    for phase in 0.. {
        let more = match config.phases {
            Phases::Count(n) => phase < n,
            Phases::For(d) => phase == 0 || started.elapsed() < d,
        };
        if !more {
            break;
        }
        let (ops_before, samples_before) = (
            ops.load(Ordering::Relaxed),
            monitor_samples.load(Ordering::Relaxed),
        );
        let phase_started = Instant::now();
        done.store(0, Ordering::Relaxed);
        barrier.wait();
        barrier.wait();
        let elapsed = phase_started.elapsed();
        let seen: Vec<String> = violations.lock().unwrap().clone();
        if !seen.is_empty() {
            running.store(false, Ordering::Relaxed);
            barrier.wait();
            panic!("phase {phase}, over budget while running: {seen:#?}");
        }

        let report = check_quiescent(&store, &live, phase, PhaseTotals {
            ops: ops.load(Ordering::Relaxed) - ops_before,
            elapsed,
            monitor_samples: monitor_samples.load(Ordering::Relaxed) - samples_before,
        });
        on_phase(&report);
        reports.push(report);

        // New caps for the next phase; lowering one evicts down to it now.
        for pool in Pool::ALL {
            let cap = [cap / 2, cap, cap * 2][rng.gen_range(0..3)];
            store.set_pool_cap(pool, cap).unwrap();
            let stats = pool_stats(&*store, pool);
            assert!(
                stats.in_cache <= cap,
                "{pool:?} over its new cap: {stats:?}"
            );
        }
    }

    running.store(false, Ordering::Relaxed);
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    monitor.join().unwrap();

    // Turning every pool off frees every value.
    for pool in Pool::ALL {
        store.set_pool_cap(pool, 0).unwrap();
        assert_eq!(pool_stats(&*store, pool).items, 0, "{pool:?}");
    }
    assert_eq!(
        live.values.load(Ordering::Relaxed),
        0,
        "values outlive the cache"
    );
    assert_eq!(live.bytes.load(Ordering::Relaxed), 0);
    reports
}

/// What the workers and the monitor share.
struct Shared {
    store: Arc<SharedStore>,
    live: Arc<Live>,
    ops: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    done: Arc<AtomicUsize>,
    barrier: Arc<Gate>,
    violations: Arc<Mutex<Vec<String>>>,
}

struct PhaseTotals {
    ops: u64,
    elapsed: Duration,
    monitor_samples: u64,
}

fn pool_stats(
    store: &dyn KindStore,
    pool: Pool,
) -> databend_storages_common_cache::backend::PoolStats {
    store
        .pool_stats()
        .into_iter()
        .find(|s| s.pool == pool)
        .unwrap()
}

/// With nothing held and nothing running: every charged byte is cached, within its cap, every
/// value alive is cached, and the structures behind the pools are bounded.
fn check_quiescent(
    store: &SharedStore,
    live: &Live,
    phase: usize,
    totals: PhaseTotals,
) -> PhaseReport {
    let mut caps = [0; 3];
    let mut in_cache = [0; 3];
    let mut structures = [PoolStructure::default(); 3];
    let mut items = 0;
    for pool in Pool::ALL {
        let stats = pool_stats(store, pool);
        let structure = store.pool_structure(pool);
        let at = format!("phase {phase}, {pool:?}: {stats:?} {structure:?}");
        assert_eq!(stats.resident, stats.in_cache, "{at}");
        assert!(stats.in_cache <= stats.cap, "{at}");
        assert_eq!(stats.items, structure.entries, "{at}");
        // Free slots are released past twice the entries plus a slack per shard.
        assert!(
            structure.slots <= 2 * structure.entries + 1100 * structure.shards,
            "{at}"
        );
        assert!(structure.ghost_entries <= structure.ghost_capacity, "{at}");
        // A shard keeps its newest record even past its share of the byte bound. A value is
        // at most a quarter of the cap it was recorded under, which is at most 4 times this one.
        assert!(
            structure.ghost_bytes <= structure.ghost_byte_capacity + structure.shards * stats.cap,
            "{at}"
        );

        let kinds: Vec<_> = kinds().into_iter().filter(|k| k.pool() == pool).collect();
        let kind_bytes: usize = kinds.iter().map(|k| store.kind_stats(*k).in_cache).sum();
        let kind_items: usize = kinds.iter().map(|k| store.kind_stats(*k).items).sum();
        assert_eq!(
            (kind_bytes, kind_items),
            (stats.in_cache, stats.items),
            "{at}"
        );
        for kind in kinds {
            let kind_stats = store.kind_stats(kind);
            assert_eq!(kind_stats.resident, kind_stats.in_cache, "{at} {kind:?}");
        }

        let i = pool.index();
        (caps[i], in_cache[i], structures[i]) = (stats.cap, stats.in_cache, structure);
        items += stats.items;
    }
    let live_values = live.values.load(Ordering::Relaxed);
    assert_eq!(
        live_values, items,
        "phase {phase}: values alive that the cache does not hold"
    );
    assert_eq!(
        live.bytes.load(Ordering::Relaxed),
        in_cache.iter().sum::<usize>()
    );
    PhaseReport {
        phase,
        ops: totals.ops,
        elapsed: totals.elapsed,
        caps,
        in_cache,
        structures,
        live_values,
        monitor_samples: totals.monitor_samples,
    }
}

/// While workers run: no pool, and no kind with a fixed limit, is ever charged more than its
/// budget, counting values evicted but still held.
fn monitor(shared: Shared, threads: usize, samples: Arc<AtomicU64>) {
    let kinds = kinds();
    loop {
        shared.barrier.wait();
        if !shared.running.load(Ordering::Relaxed) {
            return;
        }
        let violation = |what: String| {
            let mut seen = shared.violations.lock().unwrap();
            if seen.len() < 10 {
                seen.push(what);
            }
        };
        while shared.done.load(Ordering::Relaxed) < threads {
            for stats in shared.store.pool_stats() {
                if stats.resident > stats.cap {
                    violation(format!("{stats:?} over its cap"));
                }
            }
            let fixed = shared.store.kind_stats(kinds[FIXED]);
            if fixed.resident > LIMIT {
                violation(format!("{fixed:?} over its limit"));
            }
            samples.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_micros(200));
        }
        shared.barrier.wait();
    }
}

fn worker(shared: Shared, config: SoakConfig, seed: u64) {
    let Shared {
        store,
        live,
        ops,
        running,
        done,
        barrier,
        violations: _,
    } = shared;
    let store: Arc<dyn KindStore> = store;
    let kinds = kinds();
    let caches: Vec<MemCache<Value>> = kinds
        .iter()
        .map(|kind| MemCache::new(store.clone(), *kind))
        .collect();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut held = Vec::new();
    loop {
        barrier.wait();
        if !running.load(Ordering::Relaxed) {
            return;
        }
        for _ in 0..config.ops_per_phase {
            let k = rng.gen_range(0..kinds.len());
            let cache = &caches[k];
            let key = rng.gen_range(0..config.keys).to_string();
            // Mostly small values, now and then one near the oversized threshold.
            let bytes = if rng.gen_bool(0.02) {
                rng.gen_range(config.cap / 8..config.cap / 3)
            } else {
                rng.gen_range(64..config.cap / 256)
            };
            // Out of 200: reclaims and limit changes are rare, so pools fill up to their caps.
            let value = match rng.gen_range(0..200) {
                0..=59 => Some(cache.put(&key, Value::new(&live, bytes))),
                60..=89 => Some(cache.fill(&key, Value::new(&live, bytes))),
                90..=139 => cache.lookup(&key),
                140..=169 => {
                    let reader = ValueReader {
                        live: live.clone(),
                        bytes,
                    };
                    Some(block_on(cache.get(&reader, key)).unwrap())
                }
                170..=179 => {
                    cache.evict(&key);
                    None
                }
                180 => {
                    let batches = Cell::new(rng.gen_range(1..4));
                    store.shrink_for_pressure(&|| {
                        batches.set(batches.get() - 1);
                        batches.get() == 0
                    });
                    None
                }
                181..=182 => {
                    let limit = [0, LIMIT / 4, LIMIT, usize::MAX][rng.gen_range(0..4)];
                    let limit = match limit {
                        usize::MAX => KindLimit::UNLIMITED,
                        n => KindLimit::bytes(n),
                    };
                    store.set_kind_limit(kinds[LIMITED], limit);
                    None
                }
                _ => {
                    held.clear();
                    None
                }
            };
            if let Some(value) = value
                && rng.gen_bool(0.3)
            {
                held.push(value);
                if held.len() > 32 {
                    held.swap_remove(rng.gen_range(0..held.len()));
                }
            }
            ops.fetch_add(1, Ordering::Relaxed);
        }
        held.clear();
        store.set_kind_limit(kinds[LIMITED], KindLimit::bytes(LIMIT));
        done.fetch_add(1, Ordering::Relaxed);
        barrier.wait();
    }
}
