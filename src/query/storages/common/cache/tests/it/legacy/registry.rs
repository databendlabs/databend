// Copyright 2022 Datafuse Labs.
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

use databend_common_config::CacheConfig;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyKind;
use databend_storages_common_cache::legacy::LegacyKinds;

/// Kinds the legacy backend does not support stay disabled in it.
#[test]
fn test_unregistered_kinds_are_disabled() {
    let mut kinds = LegacyKinds::default();
    kinds
        .register::<table::Snapshot>(LegacyKind::items("a", |_, _| 5))
        .register::<column::Data>(LegacyKind::bytes("b", |_, memory| memory / 2));
    let capacities = kinds.capacities(&CacheConfig::default(), 100);
    for (kind, capacity) in capacities {
        let expected = match kind {
            k if k == CacheKind::of::<table::Snapshot>() => LegacyCapacity::Items(5),
            k if k == CacheKind::of::<column::Data>() => LegacyCapacity::Bytes(50),
            _ => LegacyCapacity::Disabled,
        };
        assert_eq!(capacity, expected, "{kind:?}");
    }
    assert!(kinds.get(CacheKind::of::<table::SnapshotStats>()).is_none());
    let snapshot = kinds.get(CacheKind::of::<table::Snapshot>()).unwrap();
    assert_eq!(
        (snapshot.unit, snapshot.counts_held_values),
        (Unit::Count, false)
    );
}

#[test]
fn test_split_budget() {
    assert_eq!(LegacyKind::split_budget(800), (600, 200));
    assert_eq!(LegacyKind::split_budget(3), (3, 0));
}

/// Options sizing several kinds are named once.
#[test]
fn test_ignored_options_are_named_once() {
    let mut kinds = LegacyKinds::default();
    kinds
        .register::<table::Snapshot>(LegacyKind::items("count", |c, _| {
            c.table_meta_snapshot_count
        }))
        .register::<table::SnapshotStats>(LegacyKind::items("count", |c, _| {
            c.table_meta_snapshot_count
        }));
    let config = CacheConfig {
        table_meta_snapshot_count: 1,
        ..CacheConfig::default()
    };
    assert_eq!(kinds.ignored_options(&config, 0), ["count"]);
}

#[test]
#[should_panic(expected = "legacy cache memory_cache_table_snapshot is registered twice")]
fn test_register_rejects_a_kind_twice() {
    let mut kinds = LegacyKinds::default();
    kinds
        .register::<table::Snapshot>(LegacyKind::items("a", |_, _| 1))
        .register::<table::Snapshot>(LegacyKind::items("a", |_, _| 1));
}
