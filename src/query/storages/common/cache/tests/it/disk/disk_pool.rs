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

//! `DiskPool`: reservations, eviction order, the deleting marks, the restart scan, and the
//! accounting under concurrent use.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_base::runtime::Thread;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::disk::disk_file::TMP_SUFFIX;
use databend_storages_common_cache::disk::disk_file::tmp_path;
use databend_storages_common_cache::disk::disk_pool::DiskPool;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::segment;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tempfile::TempDir;

use super::files_under;

fn data() -> CacheKind {
    CacheKind::of::<column::Data>()
}

fn meta() -> CacheKind {
    CacheKind::of::<segment::Info>()
}

fn rel(kind: CacheKind, name: &str) -> String {
    DiskKey::whole(name).rel_path(kind)
}

/// A pool over a directory that does not exist: deletes find no file, which is fine.
fn pool(cap: u64) -> DiskPool {
    DiskPool::new(Pool::Data, PathBuf::from("/nonexistent/disk-pool"), cap)
}

/// Reserves and publishes an entry of `size` bytes as a writer does, without a file. Returns
/// the victims, already deleted.
fn put(pool: &DiskPool, rel: &str, size: u64) -> Vec<String> {
    let r = pool.reserve(rel, size);
    assert!(r.reserved, "{rel}");
    pool.delete(&r.victims);
    let published = pool.publish(rel.to_string(), size, size, 0);
    assert!(published.indexed && published.victims.is_empty(), "{rel}");
    r.victims
}

/// The per-kind counts add up to the pool, and nothing is reserved.
fn assert_consistent(pool: &DiskPool) {
    let stats = pool.stats();
    let (items, bytes) = CacheKind::all()
        .map(|k| pool.kind_usage(k))
        .fold((0, 0), |(i, b), (ki, kb)| (i + ki, b + kb));
    assert_eq!((items, bytes), (stats.items, stats.bytes));
    assert_eq!(stats.writing, 0);
}

#[test]
fn test_reserve_rejects_off_pools_and_oversized_entries() {
    let pool = pool(1000);
    assert!(!pool.reserve("a", 251).reserved);
    let r = pool.reserve("a", 250);
    assert!(r.reserved && r.victims.is_empty());
    assert_eq!(pool.stats().writing, 250);
    pool.release("a", 250);
    assert_eq!(pool.stats().writing, 0);

    let off = self::pool(0);
    assert!(!off.reserve("a", 0).reserved);
    assert!(!off.reserve("a", 1).reserved);
}

/// Room is made by evicting the least recently used entries; a lookup refreshes one.
#[test]
fn test_reserve_evicts_lru_and_lookup_refreshes() {
    let pool = pool(1000);
    for name in ["a", "b", "c", "d"] {
        assert!(put(&pool, &rel(data(), name), 250).is_empty());
    }
    assert!(pool.lookup(&rel(data(), "a")).is_some());
    assert_eq!(put(&pool, &rel(data(), "e"), 200), vec![rel(data(), "b")]);
    assert_eq!(put(&pool, &rel(data(), "f"), 250), vec![rel(data(), "c")]);
    assert_eq!(pool.stats().bytes, 950);
    assert!(pool.contains(&rel(data(), "a")) && !pool.contains(&rel(data(), "b")));
    assert_consistent(&pool);
}

/// Bytes being written cannot be evicted: when only they are left, the reservation fails
/// without taking anything more out.
#[test]
fn test_reserve_fails_when_only_writes_in_flight_are_left() {
    let pool = pool(1000);
    put(&pool, &rel(data(), "a"), 200);
    let held: Vec<_> = (0..4)
        .map(|i| pool.reserve(&rel(data(), &format!("w{i}")), 225))
        .collect();
    assert!(held.iter().all(|r| r.reserved));
    assert_eq!(held.iter().map(|r| r.victims.len()).sum::<usize>(), 1);
    for r in &held {
        pool.delete(&r.victims);
    }
    assert_eq!((pool.stats().items, pool.stats().writing), (0, 900));
    let r = pool.reserve("x", 101);
    assert!(!r.reserved && r.victims.is_empty());
    for i in 0..4 {
        pool.release(&rel(data(), &format!("w{i}")), 225);
    }
    let r = pool.reserve("x", 250);
    assert!(r.reserved);
    pool.release("x", 250);
    assert_consistent(&pool);
}

/// Publishing a key already indexed, by a concurrent write or the restart scan, replaces
/// the entry instead of counting it twice.
#[test]
fn test_publish_replaces_an_indexed_entry() {
    let pool = pool(1000);
    put(&pool, &rel(data(), "a"), 100);
    put(&pool, &rel(data(), "a"), 120);
    assert_eq!((pool.stats().items, pool.stats().bytes), (1, 120));
    assert_eq!(pool.kind_usage(data()), (1, 120));
    assert_consistent(&pool);
}

/// An entry taken out cannot be reserved again until its file is deleted: a new file written
/// under its name would otherwise be the one deleted.
#[test]
fn test_entry_being_deleted_is_not_reserved_again() {
    let pool = pool(1000);
    let a = rel(data(), "a");
    for _ in 0..4 {
        put(
            &pool,
            &rel(data(), &format!("x{}", pool.stats().items)),
            250,
        );
    }
    put(&pool, &a, 100);
    let victims = pool.evict_bytes(1);
    assert_eq!(victims.len(), 1);
    let victim = victims[0].clone();
    let r = pool.reserve(&victim, 10);
    assert!(!r.reserved && r.victims.is_empty());
    pool.delete(&victims);
    let r = pool.reserve(&victim, 10);
    assert!(r.reserved);
    pool.release(&victim, 10);

    // `remove` deletes at once, so the key can be written again right away.
    assert!(pool.remove(&a));
    assert!(!pool.remove(&a));
    let r = pool.reserve(&a, 10);
    assert!(r.reserved);
    pool.release(&a, 10);
    assert_consistent(&pool);
}

#[test]
fn test_set_cap_evict_bytes_and_remove_kind() {
    let pool = pool(1000);
    for i in 0..4 {
        put(&pool, &rel(data(), &format!("d{i}")), 100);
        put(&pool, &rel(meta(), &format!("m{i}")), 100);
    }
    assert!(pool.remove(&rel(data(), "d0")));
    assert_eq!(pool.kind_usage(data()), (3, 300));

    // Oldest first, at least the bytes asked for.
    let victims = pool.evict_bytes(150);
    assert_eq!(victims, vec![rel(meta(), "m0"), rel(data(), "d1")]);
    pool.delete(&victims);
    let victims = pool.set_cap(400);
    assert_eq!(victims, vec![rel(meta(), "m1")]);
    pool.delete(&victims);
    assert_eq!((pool.cap(), pool.stats().bytes), (400, 400));

    let victims = pool.remove_kind(meta());
    assert_eq!(victims.len(), 2);
    pool.delete(&victims);
    assert_eq!(pool.kind_usage(meta()), (0, 0));
    assert_eq!(pool.kind_usage(data()), (2, 200));

    // More than it holds: all of it.
    let victims = pool.evict_bytes(10_000);
    assert_eq!(victims.len(), 2);
    pool.delete(&victims);
    assert_eq!(pool.stats().items, 0);

    put(&pool, &rel(data(), "e"), 100);
    let victims = pool.set_cap(0);
    assert_eq!(victims.len(), 1);
    pool.delete(&victims);
    assert!(!pool.reserve("f", 1).reserved);
    assert_consistent(&pool);
}

#[test]
fn test_hits_and_misses_are_counted() {
    let pool = DiskPool::new(Pool::Index, PathBuf::from("/nonexistent"), 1000);
    pool.record(true);
    pool.record(false);
    pool.record(false);
    let stats = pool.stats();
    assert_eq!((stats.hit, stats.miss, stats.cap), (1, 2, 1000));
    assert_eq!(pool.pool(), Pool::Index);
}

fn write_file(root: &Path, rel: &str, len: usize) -> PathBuf {
    let path = root.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, vec![1u8; len]).unwrap();
    path
}

/// The restart scan keeps the entries of known kinds that fit and deletes everything else:
/// files of unknown kinds, tmp files of other processes, oversized entries, and entries
/// past the capacity. Tmp files of this process are being written and are kept.
#[test]
fn test_reload_keeps_what_belongs_and_deletes_the_rest() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 2000);
    let kept: Vec<_> = (0..6)
        .map(|i| write_file(&root, &rel(data(), &format!("k{i}")), 200))
        .collect();
    let unknown = write_file(&root, "no_such_kind/abc/def", 10);
    let foreign_tmp = write_file(
        &root,
        &format!("{}/abc/x.y.1-2{TMP_SUFFIX}", data().name()),
        10,
    );
    // Named by an older layout, without an object id.
    // Left by an earlier run of a process with this pid.
    let same_pid_tmp = write_file(
        &root,
        &format!(
            "{}/abc/x.y.{}-2{TMP_SUFFIX}",
            data().name(),
            std::process::id()
        ),
        10,
    );
    let unnamed = write_file(
        &root,
        &format!("{}/abc/0123456789abcdef", data().name()),
        10,
    );
    let own = tmp_path(&root.join(rel(data(), "w")), 7);
    let own_tmp = write_file(
        &root,
        own.strip_prefix(&root).unwrap().to_str().unwrap(),
        10,
    );
    let oversized = write_file(&root, &rel(data(), "big"), 501);

    assert_eq!(pool.reload(), (6, 5));
    assert!(kept.iter().all(|p| p.exists()));
    assert!(!unknown.exists() && !foreign_tmp.exists() && !oversized.exists());
    assert!(!unnamed.exists() && !same_pid_tmp.exists());
    assert!(own_tmp.exists());
    assert_eq!(pool.kind_usage(data()), (6, 1200));
    assert_consistent(&pool);

    // Past the capacity: a smaller pool keeps what fits, in scan order.
    let small = DiskPool::new(Pool::Data, root.clone(), 800);
    assert_eq!(small.reload(), (4, 2));
    assert_eq!(kept.iter().filter(|p| p.exists()).count(), 4);
    assert_eq!(small.stats().bytes, 800);
    assert_consistent(&small);
}

/// A key indexed before the scan reaches its file, by a write meanwhile, is not counted
/// again; a key being deleted keeps its file for the delete and is not indexed.
#[test]
fn test_reload_skips_keys_indexed_or_being_deleted() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 1000);
    let a = rel(data(), "a");
    let b = rel(data(), "b");
    write_file(&root, &a, 100);
    let b_file = write_file(&root, &b, 100);
    put(&pool, &a, 100);
    put(&pool, &b, 100);
    let victims = pool.evict_bytes(1);
    assert_eq!(victims, vec![a.clone()]);

    assert_eq!(pool.reload(), (0, 0));
    assert!(!pool.contains(&a) && pool.contains(&b));
    assert!(root.join(&a).exists() && b_file.exists());
    pool.delete(&victims);
    assert!(!root.join(&a).exists());
    assert_eq!(pool.kind_usage(data()), (1, 100));
    assert_consistent(&pool);
}

/// Threads reserving, publishing, releasing, looking up, removing entries and objects, evicting
/// and resizing at once: afterwards nothing is reserved or marked for deletion, the counts add
/// up, and the pool fits its capacity, also when it shrank under writes.
#[test]
fn test_concurrent_operations_keep_the_accounting() {
    let pool = Arc::new(pool(10_000));
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let pool = pool.clone();
            Thread::spawn(move || {
                let mut rng = SmallRng::seed_from_u64(t);
                for _ in 0..5_000 {
                    let kind = if rng.gen_bool(0.5) { data() } else { meta() };
                    let name = format!("k{}", rng.gen_range(0..64));
                    let key = rel(kind, &name);
                    match rng.gen_range(0..10) {
                        0..=3 => {
                            let size = rng.gen_range(1..=2_500);
                            let r = pool.reserve(&key, size);
                            pool.delete(&r.victims);
                            if r.reserved {
                                if rng.gen_bool(0.9) {
                                    let started = rng.gen_range(0..100);
                                    pool.delete(&pool.publish(key, size, size, started).victims);
                                } else {
                                    pool.release(&key, size);
                                }
                            }
                        }
                        4 | 5 => {
                            pool.lookup(&key);
                        }
                        6 if rng.gen_bool(0.5) => {
                            pool.remove(&key);
                        }
                        6 => {
                            let at = rng.gen_range(0..100);
                            let oldest = rng.gen_bool(0.8).then(|| rng.gen_range(0..100));
                            pool.delete(&pool.remove_object(
                                &DiskKey::object_id(&name),
                                at,
                                oldest,
                            ));
                        }
                        7 => pool.delete(&pool.evict_bytes(rng.gen_range(0..3_000))),
                        8 => pool.delete(&pool.set_cap(rng.gen_range(5_000..=10_000))),
                        _ => pool.delete(&pool.remove_kind(kind)),
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_consistent(&pool);
    let stats = pool.stats();
    assert!(stats.bytes <= stats.cap, "{stats:?}");
    pool.delete(&pool.set_cap(10_000));
    // Nothing is left marked: every key can be reserved again.
    for kind in [data(), meta()] {
        for i in 0..64 {
            let key = rel(kind, &format!("k{i}"));
            let r = pool.reserve(&key, 1);
            assert!(r.reserved, "{key}");
            pool.delete(&r.victims);
            pool.release(&key, 1);
        }
    }
    assert_consistent(&pool);
}

/// `remove_object` takes every unit of an object out, in every kind, and nothing else; the
/// object ids come back with the restart scan.
#[test]
fn test_remove_object_takes_all_its_units() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 10_000);
    let units = [
        DiskKey::whole("t/a").rel_path(data()),
        DiskKey::range("t/a", 0..10).rel_path(data()),
        DiskKey::range("t/a", 10..20).rel_path(meta()),
    ];
    let other = DiskKey::whole("t/b").rel_path(data());
    for rel in units.iter().chain([&other]) {
        write_file(&root, rel, 100);
        put(&pool, rel, 100);
    }
    let mut victims = pool.remove_object(&DiskKey::object_id("t/a"), 0, None);
    victims.sort();
    let mut expected = units.to_vec();
    expected.sort();
    assert_eq!(victims, expected);
    assert!(
        pool.remove_object(&DiskKey::object_id("t/a"), 0, None)
            .is_empty()
    );
    pool.delete(&victims);
    assert!(units.iter().all(|rel| !root.join(rel).exists()));
    assert!(pool.contains(&other) && root.join(&other).exists());
    assert_consistent(&pool);

    // A restart rebuilds the ids from the file names; an evicted unit leaves its object.
    let fresh = DiskPool::new(Pool::Data, root.clone(), 10_000);
    assert_eq!(fresh.reload(), (1, 0));
    assert_eq!(
        fresh.remove_object(&DiskKey::object_id("t/b"), 0, None),
        vec![other.clone()]
    );
    fresh.delete(&[other]);
    let b = DiskKey::range("t/b", 0..1).rel_path(data());
    put(&fresh, &b, 10);
    assert_eq!(fresh.evict_bytes(1), vec![b.clone()]);
    fresh.delete(&[b]);
    assert!(
        fresh
            .remove_object(&DiskKey::object_id("t/b"), 0, None)
            .is_empty()
    );
    assert_consistent(&fresh);
}

/// A key being written cannot be reserved again, and the restart scan leaves its file alone:
/// the writer renames its own file into place and publishes it.
#[test]
fn test_reload_leaves_keys_being_written() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 1000);
    let a = rel(data(), "a");
    let r = pool.reserve(&a, 100);
    assert!(r.reserved);
    assert!(!pool.reserve(&a, 100).reserved, "a second write of the key");
    // A file of the key left by an earlier run.
    let file = write_file(&root, &a, 100);
    assert_eq!(pool.reload(), (0, 0));
    assert!(file.exists() && !pool.contains(&a));
    let published = pool.publish(a.clone(), 100, 100, 0);
    assert!(published.indexed && published.victims.is_empty());
    assert!(pool.contains(&a));
    assert_consistent(&pool);
}

/// A file the scan rejects is deleted, and its key can be written again afterwards.
#[test]
fn test_reload_rejects_then_frees_the_key() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 400);
    for i in 0..3 {
        write_file(&root, &rel(data(), &format!("k{i}")), 100);
    }
    let big = rel(data(), "big");
    write_file(&root, &big, 101);
    assert_eq!(pool.reload(), (3, 1));
    assert!(!root.join(&big).exists());
    let r = pool.reserve(&big, 100);
    assert!(r.reserved && r.victims.is_empty());
    pool.release(&big, 100);
    assert_consistent(&pool);
}

/// An entry whose fill started before its object was invalidated is rejected when published:
/// it was read before the rewrite or delete. Fills started after are published. Points no
/// running fill started before are dropped.
#[test]
fn test_publish_rejects_fills_started_before_an_invalidation() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 1000);
    let object = DiskKey::object_id("t/a");
    let a = DiskKey::range("t/a", 0..10).rel_path(data());
    let b = DiskKey::range("t/a", 10..20).rel_path(meta());

    // Fills started at 3 and 4; the object is invalidated at 5 while they run.
    for rel in [&a, &b] {
        assert!(pool.reserve(rel, 100).reserved);
    }
    assert!(pool.remove_object(&object, 5, Some(3)).is_empty());
    for (rel, started) in [(&a, 3), (&b, 4)] {
        let file = write_file(&root, rel, 100);
        let published = pool.publish(rel.clone(), 100, 100, started);
        assert!(!published.indexed);
        assert_eq!(&published.victims, std::slice::from_ref(rel));
        // The key stays marked until its file is deleted.
        assert!(!pool.reserve(rel, 100).reserved);
        pool.delete(&published.victims);
        assert!(!file.exists() && !pool.contains(rel));
    }
    // A fill started after the invalidation.
    put_file(&pool, &root, &a, 6);
    assert!(pool.contains(&a));

    // Once no fill started before it runs, the point is dropped with the next invalidation.
    assert!(
        pool.remove_object(&DiskKey::object_id("t/b"), 7, None)
            .is_empty()
    );
    put_file(&pool, &root, &b, 0);
    assert!(pool.contains(&b));
    assert_consistent(&pool);
}

/// Reserves, writes and publishes `rel` with a file, for a fill started at `started`.
fn put_file(pool: &DiskPool, root: &Path, rel: &str, started: u64) {
    let r = pool.reserve(rel, 100);
    assert!(r.reserved, "{rel}");
    pool.delete(&r.victims);
    write_file(root, rel, 100);
    let published = pool.publish(rel.to_string(), 100, 100, started);
    assert!(published.indexed, "{rel}");
    pool.delete(&published.victims);
}

/// A pool shrunk while entries are written: `set_cap` has nothing indexed to evict, so the
/// writes are checked at publish. They are taken out, the entry itself first, until the pool
/// fits; an entry over the new size limit is rejected, and at 0 every entry is. The files on
/// disk follow the index.
#[test]
fn test_publish_fits_a_pool_shrunk_under_writes() {
    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();
    let pool = DiskPool::new(Pool::Data, root.clone(), 1000);
    let xs: Vec<_> = (0..6).map(|i| rel(data(), &format!("x{i}"))).collect();
    let y = rel(data(), "y");
    for (rel, size) in xs.iter().map(|x| (x, 100)).chain([(&y, 250)]) {
        assert!(pool.reserve(rel, size).reserved);
        write_file(&root, rel, size as usize);
    }
    assert!(pool.set_cap(400).is_empty());

    let publish = |rel: &String, size: u64| {
        let published = pool.publish(rel.clone(), size, size, 0);
        pool.delete(&published.victims);
        published.indexed
    };
    // Over a quarter of 400.
    assert!(!publish(&y, 250));
    // 600 being written: the first two do not fit, the others do.
    let indexed: Vec<_> = xs.iter().map(|x| publish(x, 100)).collect();
    assert_eq!(indexed, [false, false, true, true, true, true]);
    let stats = pool.stats();
    assert_eq!((stats.bytes, stats.writing), (400, 0));
    for rel in xs.iter().chain([&y]) {
        assert_eq!(root.join(rel).exists(), pool.contains(rel), "{rel}");
    }

    let z = rel(data(), "z");
    let r = pool.reserve(&z, 100);
    assert!(r.reserved);
    pool.delete(&r.victims);
    pool.delete(&pool.set_cap(0));
    assert!(!publish(&z, 100));
    let stats = pool.stats();
    assert_eq!((stats.items, stats.bytes, stats.writing), (0, 0, 0));
    assert!(files_under(&root).is_empty());
    assert_consistent(&pool);
}
