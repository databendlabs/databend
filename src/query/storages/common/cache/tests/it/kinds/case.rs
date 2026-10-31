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

//! What the tests do with one kind, generic over its definition, and the list of every kind.

use std::any::Any;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CacheRef;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::iceberg;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::parquet;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::spatial;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::kinds::virtual_column;
use futures::executor::block_on;
use rand::Rng;
use rand::rngs::SmallRng;

use super::samples::Accuracy;
use super::samples::Sample;
use crate::fakes::wait_for;
use crate::mem_bytes_accuracy::measure;

/// One kind, for tests that run over every kind.
pub trait KindCase: Send + Sync {
    fn kind(&self) -> CacheKind;

    /// Put, look up, replace, read-only lookups, loading and eviction in memory. `admit_after`
    /// is the number of loads the backend needs before caching a read value; `legacy` says the
    /// backend charges `legacy_bytes`.
    fn memory_round_trip(&self, manager: &CacheManager, admit_after: usize, legacy: bool);

    /// `legacy_bytes` of a sample value.
    fn legacy_bytes(&self) -> usize;

    /// The disk tier, for kinds that define one: the value survives encoding, and is read back
    /// from disk once the memory tier is off.
    fn disk_round_trip(&self, manager: &CacheManager);

    /// `mem_bytes` over the bytes a sample value holds, and how close it should be.
    fn mem_bytes_ratio(&self) -> (f64, Accuracy);

    /// One random operation; values it keeps hold of go to `held`.
    fn random_op(&self, manager: &CacheManager, rng: &mut SmallRng, held: &mut Vec<Held>);
}

pub type Held = Box<dyn Any + Send>;

/// Every registered kind, in registration order.
pub fn cases() -> Vec<Box<dyn KindCase>> {
    vec![
        case::<table::Snapshot>(),
        case::<table::SnapshotStats>(),
        case::<segment::Info>(),
        case::<segment::ColumnOriented>(),
        case::<segment::BlockMetas>(),
        case::<segment::Stats>(),
        case::<bloom::IndexMeta>(),
        case::<inverted::Meta>(),
        case::<vector::Meta>(),
        case::<spatial::Meta>(),
        case::<virtual_column::Meta>(),
        case::<parquet::Meta>(),
        case::<iceberg::Table>(),
        case::<table::PruneResult>(),
        case::<bloom::Filter>(),
        case::<vector::Index>(),
        case::<vector::Refine>(),
        case::<inverted::Lookup>(),
        case::<inverted::Payload>(),
        case::<inverted::FullPayload>(),
        case::<spatial::Index>(),
        case::<column::Data>(),
        case::<column::Array>(),
    ]
}

fn case<K: Sample>() -> Box<dyn KindCase> {
    Box::new(Case::<K>(PhantomData))
}

struct Case<K>(PhantomData<fn() -> K>);

/// Loads sample values, counting the loads.
struct SampleReader<K> {
    reads: AtomicUsize,
    kind: PhantomData<fn() -> K>,
}

impl<K> SampleReader<K> {
    fn new() -> Self {
        SampleReader {
            reads: AtomicUsize::new(0),
            kind: PhantomData,
        }
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

impl<K: Sample> Reader for SampleReader<K> {
    type Key = String;
    type Item = K::Value;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, key: &String) -> Result<K::Value> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(K::sample(key.len() as u32))
    }
}

fn same<T>(a: &CacheRef<T>, b: &CacheRef<T>) -> bool {
    std::ptr::eq::<T>(&**a, &**b)
}

impl<K: Sample> KindCase for Case<K> {
    fn kind(&self) -> CacheKind {
        CacheKind::of::<K>()
    }

    fn memory_round_trip(&self, manager: &CacheManager, admit_after: usize, legacy: bool) {
        let name = K::NAME;
        let charge = |value: &K::Value| {
            if legacy {
                value.legacy_bytes()
            } else {
                value.mem_bytes()
            }
        };
        let cache = manager.cache::<K>();
        assert!(cache.is_enabled(), "{name}");

        let first = cache.put("a", K::sample(1));
        assert!(first.is_charged(), "{name}");
        assert!(same(&first, &cache.lookup("a").unwrap()), "{name}");
        let stats = cache.stats();
        assert_eq!((stats.items, stats.in_cache), (1, charge(&first)), "{name}");

        // A replaced value stays charged while held; the cache holds the new one only.
        let second = cache.put("a", K::sample(2));
        assert!(same(&second, &cache.lookup("a").unwrap()), "{name}");
        let stats = cache.stats();
        assert_eq!(
            (stats.items, stats.in_cache),
            (1, charge(&second)),
            "{name}"
        );
        assert_eq!(stats.resident, charge(&first) + charge(&second), "{name}");

        // A read-only view hits, but does not fill.
        let view = cache.read_only();
        assert!(view.lookup("a").is_some(), "{name}");
        view.fill("b", K::sample(3));
        assert!(!cache.contains("b"), "{name}");

        // Loaded through a reader until admitted, then served from the cache.
        let reader = SampleReader::<K>::new();
        let mut loaded = Vec::new();
        for _ in 0..=admit_after {
            loaded.push(block_on(cache.get(&reader, "c".to_string())).unwrap());
        }
        assert_eq!(reader.reads(), admit_after, "{name}");
        assert!(
            same(loaded.last().unwrap(), &cache.lookup("c").unwrap()),
            "{name}"
        );

        assert!(cache.evict("a") && cache.evict("c"), "{name}");
        assert!(!cache.contains("a") && !cache.evict("a"), "{name}");
        drop((first, second, loaded));
        let stats = cache.stats();
        assert_eq!(
            (stats.items, stats.in_cache, stats.resident),
            (0, 0, 0),
            "{name}"
        );
    }

    fn disk_round_trip(&self, manager: &CacheManager) {
        let name = K::NAME;
        let Some(disk) = K::DISK else {
            return;
        };
        let value = K::sample(7);
        let encoded = disk.codec.encode(&value).unwrap();
        let decoded = disk.codec.decode(encoded.clone().into()).unwrap();
        assert_eq!(disk.codec.encode(&decoded).unwrap(), encoded, "{name}");

        let cache = manager.cache::<K>();
        cache.put("d", value);
        let disk_name = format!("disk_{}", name.trim_start_matches("memory_"));
        let on_disk = || {
            manager
                .rows()
                .iter()
                .any(|r| r.name == disk_name && r.num_items == 1)
        };
        wait_for(on_disk);

        manager.set_cache_capacity(name, 0).unwrap();
        let read_back = cache.lookup("d").expect(name);
        assert!(!read_back.is_charged(), "{name}");
        assert_eq!(disk.codec.encode(&read_back).unwrap(), encoded, "{name}");
        assert!(cache.evict("d"), "{name}");
        assert!(cache.lookup("d").is_none() && !on_disk(), "{name}");
    }

    fn legacy_bytes(&self) -> usize {
        K::sample(5).legacy_bytes()
    }

    fn mem_bytes_ratio(&self) -> (f64, Accuracy) {
        // The first value also allocates lazily initialized statics, which stay.
        drop(K::sample(5));
        let (value, truth) = measure(|| K::sample(5));
        (value.mem_bytes() as f64 / truth as f64, K::ACCURACY)
    }

    fn random_op(&self, manager: &CacheManager, rng: &mut SmallRng, held: &mut Vec<Held>) {
        let cache = manager.cache::<K>();
        let key = rng.gen_range(0..16).to_string();
        let value = match rng.gen_range(0..6) {
            0 | 1 => Some(cache.put(&key, K::sample(rng.r#gen()))),
            2 => cache.lookup(&key),
            3 => Some(block_on(cache.get(&SampleReader::<K>::new(), key)).unwrap()),
            4 => {
                cache.read_only().fill(&key, K::sample(rng.r#gen()));
                None
            }
            _ => {
                cache.evict(&key);
                None
            }
        };
        if let Some(value) = value
            && rng.gen_bool(0.5)
        {
            held.push(Box::new(value));
        }
    }
}
