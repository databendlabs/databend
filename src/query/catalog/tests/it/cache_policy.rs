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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::Thread;
use databend_common_catalog::cache_policy::CacheFill;
use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::cache_policy::CacheWriteRefusal;
use databend_common_catalog::cache_policy::TableDataCacheWriteBudget;

const FILLS: [CacheFill; 3] = [CacheFill::ReadWrite, CacheFill::ReadOnly, CacheFill::Bypass];

#[test]
fn test_fill_reads_and_writes() {
    let rights = FILLS.map(|fill| (fill.reads(), fill.writes()));
    assert_eq!(rights, [(true, true), (true, false), (false, false)]);
    assert_eq!(CacheFill::default(), CacheFill::ReadWrite);
}

/// The stricter fill wins, in either order: a call can narrow what its statement allows, never
/// widen it.
#[test]
fn test_fill_at_most_keeps_the_stricter() {
    for a in FILLS {
        for b in FILLS {
            let both = a.at_most(b);
            assert_eq!(both, b.at_most(a), "{a:?} and {b:?}");
            assert_eq!(both.reads(), a.reads() && b.reads(), "{a:?} and {b:?}");
            assert_eq!(both.writes(), a.writes() && b.writes(), "{a:?} and {b:?}");
        }
        assert_eq!(a.at_most(a), a);
        assert_eq!(a.at_most(CacheFill::ReadWrite), a);
        assert_eq!(a.at_most(CacheFill::Bypass), CacheFill::Bypass);
    }
}

#[test]
fn test_policies_of_statements() {
    use CacheFill::*;
    let policies = [
        CachePolicy::DEFAULT,
        CachePolicy::READ_ONLY,
        CachePolicy::MAINTENANCE,
        CachePolicy::BYPASS,
    ]
    .map(|p| (p.memory, p.disk));
    assert_eq!(policies, [
        (ReadWrite, ReadWrite),
        (ReadOnly, ReadOnly),
        (ReadWrite, Bypass),
        (Bypass, Bypass),
    ]);
    assert_eq!(CachePolicy::default(), CachePolicy::DEFAULT);
}

/// Each cache is bounded on its own: a background read (`READ_ONLY`) of a maintenance
/// statement (`MAINTENANCE`) looks the memory caches up without filling them, and does not
/// touch the disk cache.
#[test]
fn test_policy_at_most_is_cache_by_cache() {
    let both = CachePolicy::READ_ONLY.at_most(CachePolicy::MAINTENANCE);
    assert_eq!(both, CachePolicy {
        memory: CacheFill::ReadOnly,
        disk: CacheFill::Bypass,
    });
    assert_eq!(
        both,
        CachePolicy::MAINTENANCE.at_most(CachePolicy::READ_ONLY)
    );
    for p in [
        CachePolicy::DEFAULT,
        CachePolicy::READ_ONLY,
        CachePolicy::MAINTENANCE,
        CachePolicy::BYPASS,
    ] {
        assert_eq!(p.at_most(CachePolicy::DEFAULT), p);
        assert_eq!(p.at_most(CachePolicy::BYPASS), CachePolicy::BYPASS);
    }
}

/// The policy crosses nodes in the query environment.
#[test]
fn test_policy_round_trips_through_json() -> anyhow::Result<()> {
    for p in [
        CachePolicy::DEFAULT,
        CachePolicy::READ_ONLY,
        CachePolicy::MAINTENANCE,
        CachePolicy::BYPASS,
    ] {
        let json = serde_json::to_string(&p)?;
        assert_eq!(serde_json::from_str::<CachePolicy>(&json)?, p);
    }
    Ok(())
}

/// A statement may write half of the cache's limit, no more, whatever order its entries come
/// in; what it writes is counted exactly.
#[test]
fn test_write_budget_admits_half_of_the_limit() {
    let budget = TableDataCacheWriteBudget::default();
    assert_eq!(budget.admit(300, 1000), Ok(()));
    assert_eq!(budget.admit(200, 1000), Ok(()));
    assert_eq!(budget.written(), 500);
    assert_eq!(budget.admit(1, 1000), Err(CacheWriteRefusal::Written));
    // A smaller entry later is refused too: the allowance is spent.
    assert_eq!(budget.admit(0, 1000), Ok(()));
    assert_eq!(budget.written(), 500);
    // A larger limit, as after a cache grew, admits again.
    assert_eq!(budget.admit(100, 1400), Ok(()));
    assert_eq!(budget.written(), 600);
}

/// A single entry over the allowance is refused and leaves the count untouched, so the
/// entries after it still get their share.
#[test]
fn test_write_budget_refuses_an_oversized_entry_alone() {
    let budget = TableDataCacheWriteBudget::default();
    assert_eq!(budget.admit(501, 1000), Err(CacheWriteRefusal::Written));
    assert_eq!(budget.written(), 0);
    assert_eq!(budget.admit(500, 1000), Ok(()));
}

/// Scans expected to read more than the allowance in full end the statement's writes, from
/// the first entry on; what was written before stays counted.
#[test]
fn test_write_budget_ends_writes_when_more_is_expected() {
    let budget = TableDataCacheWriteBudget::default();
    assert_eq!(budget.admit(100, 1000), Ok(()));
    budget.expect(400, 1000);
    assert_eq!(
        budget.admit(100, 1000),
        Ok(()),
        "expected within the allowance"
    );
    budget.expect(101, 1000);
    assert_eq!(budget.admit(1, 1000), Err(CacheWriteRefusal::Expected));
    assert_eq!(budget.admit(0, 1000), Err(CacheWriteRefusal::Expected));
    assert_eq!(budget.written(), 200);
}

/// Without a limit there is no allowance: nothing is admitted. The caller passes a limit only
/// for a cache that has one.
#[test]
fn test_write_budget_without_a_limit_admits_nothing() {
    let budget = TableDataCacheWriteBudget::default();
    assert_eq!(budget.admit(1, 0), Err(CacheWriteRefusal::Written));
    assert_eq!(budget.admit(1, 1), Err(CacheWriteRefusal::Written));
}

/// Threads admitting at once never take the statement past its allowance together.
#[test]
fn test_write_budget_is_exact_under_concurrency() {
    let budget = Arc::new(TableDataCacheWriteBudget::default());
    let admitted = Arc::new(AtomicU64::new(0));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (budget, admitted) = (budget.clone(), admitted.clone());
            Thread::spawn(move || {
                for _ in 0..1000 {
                    if budget.admit(7, 10_000).is_ok() {
                        admitted.fetch_add(7, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let admitted = admitted.load(Ordering::Relaxed);
    assert_eq!(budget.written(), admitted);
    assert!(admitted <= 5000 && admitted > 5000 - 7, "{admitted}");
}
