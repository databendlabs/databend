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

use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;

fn fifo() -> S3Fifo<u32, u32> {
    S3Fifo::new(S3FifoConfig::default())
}

fn evict_to(cache: &mut S3Fifo<u32, u32>, cap: usize) {
    while cache.bytes() > cap {
        if cache.pop_evictable(|_| false, usize::MAX).is_none() {
            break;
        }
    }
}

#[test]
fn test_cached_on_first_insert() {
    let mut c = fifo();
    assert_eq!(c.insert(1, 10, 1), None);
    assert_eq!(c.get(&1), Some(&10));
    assert_eq!(c.len(), 1);
    assert_eq!(c.bytes(), 1);
}

#[test]
fn test_insert_replaces() {
    let mut c = fifo();
    assert_eq!(c.insert(1, 10, 4), None);
    assert_eq!(c.get(&1), Some(&10));
    assert_eq!(c.insert(1, 11, 6), Some(10));
    assert_eq!(c.bytes(), 6);
    assert_eq!(c.len(), 1);
}

#[test]
fn test_hot_keys_survive_scan() {
    let mut c = fifo();
    for k in 0..10 {
        c.insert(k, k, 1);
    }
    for _ in 0..3 {
        for k in 0..10 {
            c.get(&k);
        }
    }
    for k in 1000..2000 {
        c.insert(k, k, 1);
        evict_to(&mut c, 20);
    }
    for k in 0..10 {
        assert!(c.contains(&k), "hot key {k} evicted by scan");
    }
}

#[test]
fn test_ghost_hit_goes_to_main() {
    let mut c = fifo();
    c.insert(1, 1, 1);
    c.insert(2, 2, 1);
    let (k, _, _) = c.pop_evictable(|_| false, usize::MAX).unwrap();
    assert_eq!(k, 1);
    c.insert(1, 1, 1);
    for k in 100..200 {
        c.insert(k, k, 1);
        evict_to(&mut c, 3);
    }
    assert!(c.contains(&1));
}

#[test]
fn test_pop_evictable_rotates_kept_entries() {
    let mut c = fifo();
    for k in 0..4 {
        c.insert(k, k, 1);
    }
    assert!(c.pop_evictable(|_| true, 16).is_none());
    assert_eq!(c.len(), 4);

    let (k, v, bytes) = c.pop_evictable(|v| *v != 2, 16).unwrap();
    assert_eq!((k, v, bytes), (2, 2, 1));
    assert_eq!(c.len(), 3);
}

#[test]
fn test_pop_evictable_respects_max_checks() {
    let mut c = fifo();
    for k in 0..100 {
        c.insert(k, k, 1);
    }
    assert!(c.pop_evictable(|v| *v < 99, 10).is_none());
    assert!(c.pop_evictable(|v| *v < 99, 1000).is_some());
}

#[test]
fn test_removed_keys_are_skipped() {
    let mut c = fifo();
    c.insert(1, 1, 1);
    c.insert(2, 2, 1);
    assert_eq!(c.remove(&1), Some(1));
    c.insert(1, 11, 1);
    let (k, _, _) = c.pop_evictable(|_| false, usize::MAX).unwrap();
    assert_eq!(k, 2);
    let (k, v, _) = c.pop_evictable(|_| false, usize::MAX).unwrap();
    assert_eq!((k, v), (1, 11));
    assert!(c.pop_evictable(|_| false, usize::MAX).is_none());
    assert_eq!(c.bytes(), 0);
}

#[test]
fn test_large_entries_skip_small_queue() {
    let fill = |large_entry_bytes| {
        let mut c: S3Fifo<u32, u32> = S3Fifo::new(S3FifoConfig {
            large_entry_bytes,
            ..Default::default()
        });
        for k in 10..20 {
            c.insert(k, k, 1);
        }
        c.insert(1, 1, 100);
        c.pop_evictable(|_| false, usize::MAX).unwrap().0
    };
    assert_eq!(fill(100), 1);
    assert_eq!(fill(usize::MAX), 10);
}

#[test]
fn test_remove_if() {
    let mut c = fifo();
    for k in 0..10 {
        c.insert(k, k, 2);
    }
    let removed = c.remove_if(|k, _| k % 2 == 0);
    assert_eq!(removed.len(), 5);
    assert_eq!(c.len(), 5);
    assert_eq!(c.bytes(), 10);
}

/// Removals without capacity-driven eviction do not grow the slab without bound.
#[test]
fn test_removals_keep_slab_bounded() {
    let mut c = fifo();
    for round in 0..100u32 {
        for k in 0..100 {
            c.insert(round * 100 + k, k, 1);
        }
        for k in 0..100 {
            assert!(c.remove(&(round * 100 + k)).is_some());
        }
    }
    assert!(c.is_empty());
    assert!(c.slots() <= 1024 + 100, "{}", c.slots());

    for k in 0..5000 {
        c.insert(k, k, 1);
    }
    c.remove_if(|_, _| true);
    assert!(c.slots() <= 1024, "{}", c.slots());
    // Entries inserted afterwards are still evicted in order.
    c.insert(1, 1, 1);
    c.insert(2, 2, 1);
    assert_eq!(
        c.pop_evictable(|_| false, usize::MAX).map(|(k, ..)| k),
        Some(1)
    );
}

/// Entries that were all accessed recently are aged, not given up on: a victim is found
/// within a small check budget when nothing is kept.
#[test]
fn test_aging_does_not_exhaust_checks() {
    let mut c = fifo();
    for k in 0..100 {
        c.insert(k, k, 1);
    }
    for _ in 0..3 {
        for k in 0..100 {
            c.get(&k);
        }
    }
    assert!(c.pop_evictable(|_| false, 1).is_some());
    // Kept entries still count against the budget.
    assert!(c.pop_evictable(|_| true, 8).is_none());
}

fn evict_keys(
    c: &mut S3Fifo<u32, u32>,
    matches: impl Fn(&u32) -> bool,
    keep: impl Fn(&u32) -> bool,
    max_entries: usize,
) -> Vec<u32> {
    c.evict_where(matches, keep, usize::MAX, max_entries)
        .into_iter()
        .map(|(k, _, _)| k)
        .collect()
}

/// Victims among matching keys come in eviction order; other entries are left alone.
#[test]
fn test_evict_where_only_touches_matching_entries() {
    let mut c = fifo();
    for k in 0..10 {
        c.insert(k, k, 10);
    }
    // Accessed: 2 moves to the main queue, 3 keeps its access.
    c.get(&2);
    c.get(&3);
    let even = |k: &u32| k.is_multiple_of(2);
    assert_eq!(evict_keys(&mut c, even, |_| false, usize::MAX), vec![
        0, 4, 6, 8, 2
    ]);
    assert_eq!(c.len(), 5);
    assert_eq!(c.bytes(), 50);

    // 3 still has its access: a plain eviction moves it to the main queue and takes 1 first.
    let (first, _, _) = c.pop_evictable(|_| false, usize::MAX).unwrap();
    assert_eq!(first, 1);
    assert!(c.contains(&3));
}

#[test]
fn test_evict_where_stops_at_target() {
    let mut c = fifo();
    for k in 0..10 {
        c.insert(k, k, 10);
    }
    assert_eq!(evict_keys(&mut c, |_| true, |_| false, 3), vec![0, 1, 2]);
    let victims = c.evict_where(|_| true, |_| false, 25, usize::MAX);
    assert_eq!(victims.len(), 3);
    assert_eq!(c.len(), 4);
}

#[test]
fn test_evict_where_skips_kept_entries() {
    let mut c = fifo();
    for k in 0..4 {
        c.insert(k, k, 10);
    }
    let kept = |v: &u32| *v < 2;
    assert_eq!(evict_keys(&mut c, |_| true, kept, 1), vec![2]);
    assert_eq!(evict_keys(&mut c, |_| true, kept, 1), vec![3]);
    assert!(evict_keys(&mut c, |_| true, kept, 1).is_empty());
    assert_eq!(c.len(), 2);
    assert_eq!(c.bytes(), 20);
}

/// The main queue works as a clock: an accessed entry is passed over and moved to the back,
/// so the next eviction takes the following cold entry rather than aging the same one again.
#[test]
fn test_evict_where_rotates_like_pop_evictable() {
    let new = || {
        let mut c = fifo();
        for k in 0..6 {
            c.insert(k, k, 10);
            c.get(&k);
        }
        // Keeping every entry moves all to the main queue, in order, with no access left;
        // then 0 and 1 are read again.
        c.evict_where(|_| true, |_| true, usize::MAX, usize::MAX);
        c.get(&0);
        c.get(&1);
        c
    };
    let mut scoped = new();
    let mut plain = new();
    for _ in 0..4 {
        let a = evict_keys(&mut scoped, |_| true, |_| false, 1);
        let b = plain.pop_evictable(|_| false, usize::MAX).map(|(k, ..)| k);
        assert_eq!(a.first().copied(), b);
    }
    assert!(scoped.contains(&0) && scoped.contains(&1));
}

/// Repeated scoped evictions reuse slots; the slab stays bounded.
#[test]
fn test_evict_where_keeps_slab_bounded() {
    let mut c = fifo();
    for round in 0..20_000u32 {
        c.insert(round, round, 1);
        c.get(&round);
        if round >= 100 {
            assert_eq!(evict_keys(&mut c, |_| true, |_| false, 1).len(), 1);
        }
    }
    assert_eq!(c.len(), 100);
    assert!(c.slots() <= 2 * c.len() + 1024);
}

#[test]
fn test_large_entry_threshold_can_change() {
    let mut c = S3Fifo::<u32, u32>::new(S3FifoConfig {
        large_entry_bytes: 1,
        ..Default::default()
    });
    c.insert(1, 1, 100);
    assert_eq!(c.small_bytes(), 0);
    c.set_large_entry_bytes(1000);
    c.insert(2, 2, 100);
    assert_eq!(c.small_bytes(), 100);
}
