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

use databend_common_catalog::cache_policy::CacheFill;
use databend_common_catalog::cache_policy::CachePolicy;

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
