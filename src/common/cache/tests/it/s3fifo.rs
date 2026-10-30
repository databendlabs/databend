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

use databend_common_cache::Admission;
use databend_common_cache::S3Fifo;
use databend_common_cache::S3FifoConfig;

fn fifo(admit_after: u8) -> S3Fifo<u32, u32> {
    S3Fifo::new(S3FifoConfig {
        admit_after,
        ..Default::default()
    })
}

fn evict_to(cache: &mut S3Fifo<u32, u32>, cap: usize) {
    while cache.bytes() > cap {
        if cache.pop_evictable(|_| false, usize::MAX).is_none() {
            break;
        }
    }
}

fn admitted<V>(a: Admission<V>) -> bool {
    matches!(a, Admission::Admitted { .. })
}

#[test]
fn test_admit_first_access() {
    let mut c = fifo(1);
    assert!(admitted(c.admit(1, 10, 1)));
    assert_eq!(c.get(&1), Some(&10));
    assert_eq!(c.len(), 1);
    assert_eq!(c.bytes(), 1);
}

#[test]
fn test_admit_after_second_access() {
    let mut c = fifo(2);
    assert!(matches!(c.admit(1, 10, 1), Admission::Deferred(10)));
    assert!(c.get(&1).is_none());
    assert_eq!(c.ghost_len(), 1);
    assert!(admitted(c.admit(1, 10, 1)));
    assert_eq!(c.get(&1), Some(&10));
    assert_eq!(c.ghost_len(), 0);
}

#[test]
fn test_insert_bypasses_admission_and_replaces() {
    let mut c = fifo(2);
    assert_eq!(c.insert(1, 10, 4), None);
    assert_eq!(c.get(&1), Some(&10));
    assert_eq!(c.insert(1, 11, 6), Some(10));
    assert_eq!(c.bytes(), 6);
    assert_eq!(c.len(), 1);
}

#[test]
fn test_hot_keys_survive_scan() {
    let mut c = fifo(1);
    for k in 0..10 {
        c.admit(k, k, 1);
    }
    for _ in 0..3 {
        for k in 0..10 {
            c.get(&k);
        }
    }
    for k in 1000..2000 {
        c.admit(k, k, 1);
        evict_to(&mut c, 20);
    }
    for k in 0..10 {
        assert!(c.contains(&k), "hot key {k} evicted by scan");
    }
}

#[test]
fn test_ghost_hit_goes_to_main() {
    let mut c = fifo(1);
    c.admit(1, 1, 1);
    c.admit(2, 2, 1);
    let (k, _, _) = c.pop_evictable(|_| false, usize::MAX).unwrap();
    assert_eq!(k, 1);
    c.admit(1, 1, 1);
    for k in 100..200 {
        c.admit(k, k, 1);
        evict_to(&mut c, 3);
    }
    assert!(c.contains(&1));
}

#[test]
fn test_pop_evictable_rotates_kept_entries() {
    let mut c = fifo(1);
    for k in 0..4 {
        c.admit(k, k, 1);
    }
    assert!(c.pop_evictable(|_| true, 16).is_none());
    assert_eq!(c.len(), 4);

    let (k, v, bytes) = c.pop_evictable(|v| *v != 2, 16).unwrap();
    assert_eq!((k, v, bytes), (2, 2, 1));
    assert_eq!(c.len(), 3);
}

#[test]
fn test_pop_evictable_respects_max_checks() {
    let mut c = fifo(1);
    for k in 0..100 {
        c.admit(k, k, 1);
    }
    assert!(c.pop_evictable(|v| *v < 99, 10).is_none());
    assert!(c.pop_evictable(|v| *v < 99, 1000).is_some());
}

#[test]
fn test_removed_keys_are_skipped() {
    let mut c = fifo(1);
    c.admit(1, 1, 1);
    c.admit(2, 2, 1);
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
            c.admit(k, k, 1);
        }
        c.admit(1, 1, 100);
        c.pop_evictable(|_| false, usize::MAX).unwrap().0
    };
    assert_eq!(fill(100), 1);
    assert_eq!(fill(usize::MAX), 10);
}

#[test]
fn test_remove_if() {
    let mut c = fifo(1);
    for k in 0..10 {
        c.admit(k, k, 2);
    }
    let removed = c.remove_if(|k, _| k % 2 == 0);
    assert_eq!(removed.len(), 5);
    assert_eq!(c.len(), 5);
    assert_eq!(c.bytes(), 10);
}

/// Removals without capacity-driven eviction do not grow the queues without bound.
#[test]
fn test_removals_keep_queues_bounded() {
    let mut c = fifo(1);
    for round in 0..100u32 {
        for k in 0..100 {
            c.insert(round * 100 + k, k, 1);
        }
        for k in 0..100 {
            assert!(c.remove(&(round * 100 + k)).is_some());
        }
    }
    assert!(c.is_empty());
    assert!(c.queue_len() <= 1024 + 100, "{}", c.queue_len());

    for k in 0..5000 {
        c.insert(k, k, 1);
    }
    c.remove_if(|_, _| true);
    assert!(c.queue_len() <= 1024, "{}", c.queue_len());
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
    let mut c = fifo(1);
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
