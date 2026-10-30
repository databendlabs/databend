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

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheInnerConfig;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CacheRow;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::VectorIndexFile;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::kinds::virtual_column;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyStore;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use tempfile::TempDir;

use crate::fakes::Item;
use crate::fakes::Live;
use crate::fakes::shared_store;
use crate::fakes::wait_for;

const MIB: u64 = 1 << 20;

/// Every disk tier enabled, of 1 MiB, under `dir`.
fn disk_config(dir: &TempDir) -> CacheConfig {
    CacheConfig {
        data_cache_storage: CacheStorageTypeInnerConfig::Disk,
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: MIB,
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        },
        disk_cache_table_bloom_index_data_size: MIB,
        disk_cache_table_bloom_index_meta_size: MIB,
        disk_cache_inverted_index_meta_size: MIB,
        disk_cache_inverted_index_lookup_size: MIB,
        disk_cache_inverted_index_payload_size: MIB,
        disk_cache_vector_index_meta_size: MIB,
        disk_cache_vector_index_data_size: MIB,
        disk_cache_spatial_index_meta_size: MIB,
        disk_cache_spatial_index_data_size: MIB,
        disk_cache_virtual_column_meta_size: MIB,
        ..CacheConfig::default()
    }
}

fn shared_config() -> CacheConfig {
    CacheConfig {
        shared_memory_enabled: true,
        ..CacheConfig::default()
    }
}

fn manager(config: &CacheConfig, ee_mode: bool) -> CacheManager {
    CacheManager::try_new(config, &MIB, "test_tenant", ee_mode).unwrap()
}

/// The rows `system.caches` shows: the enabled ones.
fn row(manager: &CacheManager, name: &str) -> Option<CacheRow> {
    manager
        .rows()
        .into_iter()
        .find(|r| r.name == name && r.enabled)
}

fn capacity(manager: &CacheManager, name: &str) -> Option<u64> {
    row(manager, name).and_then(|r| r.capacity)
}

fn disk_rows(manager: &CacheManager) -> usize {
    manager
        .rows()
        .iter()
        .filter(|r| r.name.starts_with("disk_"))
        .count()
}

/// In EE mode, disk tiers enabled in the config can be turned off and on at runtime; outside
/// EE mode, or disabled in the config, there are none.
#[test]
fn test_disk_tiers_follow_ee_mode_and_config() {
    let dir = TempDir::new().unwrap();
    let config = disk_config(&dir);
    let ee = manager(&config, true);
    assert_eq!(disk_rows(&ee), 11);
    ee.set_disk_allowed(false);
    assert_eq!(disk_rows(&ee), 0);
    ee.set_disk_allowed(true);
    assert_eq!(disk_rows(&ee), 11);

    let not_ee = manager(&config, false);
    not_ee.set_disk_allowed(true);
    assert_eq!(disk_rows(&not_ee), 0);
    assert_eq!(disk_rows(&manager(&CacheConfig::default(), true)), 0);
}

/// `enable_table_meta_cache = false` turns off the meta and index disk tiers of the legacy
/// backend; the shared backend ignores the option.
#[test]
fn test_meta_cache_switch_and_disk_tiers() {
    let dir = TempDir::new().unwrap();
    let config = CacheConfig {
        enable_table_meta_cache: false,
        ..disk_config(&dir)
    };
    let legacy = manager(&config, true);
    assert_eq!(disk_rows(&legacy), 1);
    assert!(row(&legacy, "disk_cache_column_data").is_some());
    let shared = CacheConfig {
        shared_memory_enabled: true,
        ..config
    };
    assert_eq!(disk_rows(&manager(&shared, true)), 11);
}

/// A kind's cache comes with its disk tier, which keeps serving when the memory tier is off.
#[test]
fn test_cache_attaches_its_disk_tier() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let manager = manager(&disk_config(&dir), true);
    let pages = manager.cache::<vector::Refine>();
    let page = || VectorIndexFile::create("p".to_string(), Bytes::from_static(b"fp16 page"));
    pages.put("a:p", page());
    wait_for(|| {
        row(&manager, "disk_cache_vector_index_refine")
            .unwrap()
            .num_items
            == 1
    });

    manager.set_cache_capacity("memory_cache_vector_index_refine", 0)?;
    assert!(row(&manager, "memory_cache_vector_index_refine").is_none());
    assert_eq!(pages.lookup("a:p").unwrap().data, page().data);

    // Kinds without a disk tier get none, whatever the config.
    assert!(row(&manager, "disk_cache_vector_index_file").is_none());
    Ok(())
}

/// The shared backend reclaims the memory tier of column data under pressure; its disk tier is
/// kept, and a later lookup refills memory from it.
#[test]
fn test_pressure_shrink_keeps_disk_tier() {
    let dir = TempDir::new().unwrap();
    let config = CacheConfig {
        shared_memory_enabled: true,
        kind_max_bytes: [("memory_cache_column_data".to_string(), MIB)].into(),
        ..disk_config(&dir)
    };
    let manager = manager(&config, true);
    let cache = manager.cache::<column::Data>();
    let items = |name| row(&manager, name).unwrap().num_items;
    cache.put("k", ColumnData::from_bytes(Bytes::from_static(b"data")));
    wait_for(|| items("disk_cache_column_data") == 1);
    assert_eq!(items("memory_cache_column_data"), 1);

    assert_eq!(manager.shrink_for_pressure(&|| false).evicted, 1);
    assert_eq!(items("memory_cache_column_data"), 0);
    assert_eq!(cache.lookup("k").unwrap().bytes(), b"data".as_slice());
    assert_eq!(items("memory_cache_column_data"), 1);
}

fn empty_snapshot() -> Arc<TableSnapshot> {
    let snapshot = TableSnapshot::try_new(
        None,
        None,
        TableSchema::empty(),
        Statistics::default(),
        vec![],
        None,
        None,
        TableMetaTimestamps::new(None, chrono::Duration::hours(1)),
    )
    .unwrap();
    Arc::new(snapshot)
}

/// The legacy backend resizes a kind in its legacy unit, under its name or its hybrid alias;
/// a kind enabled at runtime keeps its unit.
#[test]
fn test_legacy_capacity_by_name() -> Result<()> {
    let manager = manager(&CacheConfig::default(), false);
    let snapshots = manager.cache::<table::Snapshot>();
    for i in 0..3 {
        snapshots.put(&i.to_string(), empty_snapshot());
    }
    manager.set_cache_capacity("cache_table_snapshot", 1)?;
    let snapshot = row(&manager, "memory_cache_table_snapshot").unwrap();
    assert_eq!(
        (snapshot.num_items, snapshot.capacity, snapshot.unit),
        (1, Some(1), Unit::Count)
    );

    manager.set_cache_capacity("memory_cache_segment_block_metas", 0)?;
    assert!(capacity(&manager, "memory_cache_segment_block_metas").is_none());
    manager.set_cache_capacity("memory_cache_segment_block_metas", 4)?;
    let block_metas = row(&manager, "memory_cache_segment_block_metas").unwrap();
    assert_eq!(
        (block_metas.capacity, block_metas.unit),
        (Some(4), Unit::Count)
    );

    // The payload size is the budget of pages and full components, as before.
    manager.set_cache_capacity("memory_cache_inverted_index_payload", 800)?;
    assert_eq!(
        capacity(&manager, "memory_cache_inverted_index_payload"),
        Some(600)
    );
    assert_eq!(
        capacity(&manager, "memory_cache_inverted_index_full_payload"),
        Some(200)
    );

    for unknown in ["no_such_cache", "memory_cache_block_meta", "table_snapshot"] {
        assert!(manager.set_cache_capacity(unknown, 1).is_err(), "{unknown}");
    }
    // Pools exist only in the shared backend.
    assert!(manager.set_cache_capacity("pool:meta", 1).is_err());
    assert!(manager.set_cache_capacity("all", 0).is_err());
    Ok(())
}

#[test]
fn test_shared_backend_sizes_pools_by_ratio() -> Result<()> {
    let config = CacheConfig {
        // Ignored by the shared backend.
        enable_table_meta_cache: false,
        table_meta_snapshot_count: 1,
        kind_max_bytes: [
            ("cache_vector_index_file".to_string(), 1234),
            ("memory_cache_column_data".to_string(), 5000),
        ]
        .into(),
        ..shared_config()
    };
    let manager = CacheManager::try_new(&config, &1_000_000, "test", false)?;
    assert_eq!(capacity(&manager, "__pool:meta"), Some(100_000));
    assert_eq!(capacity(&manager, "__pool:index"), Some(400_000));
    assert_eq!(capacity(&manager, "__pool:data"), Some(100_000));
    let snapshot = row(&manager, "memory_cache_table_snapshot").unwrap();
    assert_eq!((snapshot.capacity, snapshot.unit), (None, Unit::Bytes));
    assert_eq!(
        capacity(&manager, "memory_cache_vector_index_file"),
        Some(1234)
    );
    assert_eq!(capacity(&manager, "memory_cache_column_data"), Some(5000));
    // Off unless given a limit.
    assert!(row(&manager, "memory_cache_table_data").is_none());
    assert!(row(&manager, "memory_cache_segment_block_metas").is_none());

    // Pools can be resized; the payload size is not split as in the legacy backend.
    manager.set_cache_capacity("pool:index", 0)?;
    assert!(row(&manager, "__pool:index").is_none());
    assert!(row(&manager, "memory_cache_bloom_index_filter").is_none());
    manager.set_cache_capacity("pool:index", 1000)?;
    manager.set_cache_capacity("memory_cache_inverted_index_payload", 800)?;
    assert_eq!(
        capacity(&manager, "memory_cache_inverted_index_payload"),
        Some(800)
    );
    assert_eq!(
        capacity(&manager, "memory_cache_inverted_index_full_payload"),
        None
    );
    assert!(manager.set_cache_capacity("pool:none", 1).is_err());
    assert!(manager.set_cache_capacity("all", 1).is_err());
    manager.set_cache_capacity("all", 0)?;
    assert!(!manager.rows().iter().any(|r| r.enabled && r.pool.is_some()));
    Ok(())
}

#[test]
fn test_shared_backend_rejects_bad_config() {
    let too_much = CacheConfig {
        meta_max_ratio: 50,
        index_max_ratio: 50,
        ..shared_config()
    };
    assert!(CacheManager::try_new(&too_much, &1_000_000, "test", false).is_err());
    for name in ["no_such_cache", "vector_index"] {
        let unknown = CacheConfig {
            kind_max_bytes: [(name.to_string(), 1)].into(),
            ..shared_config()
        };
        assert!(CacheManager::try_new(&unknown, &1_000_000, "test", false).is_err());
    }
}

fn find<'a>(rows: &'a [CacheRow], name: &str) -> &'a CacheRow {
    rows.iter().find(|r| r.name == name).unwrap()
}

#[test]
fn test_rows_cover_kinds_and_pools() {
    let store = shared_store(1000, 2000, 0);
    let manager = CacheManager::new(store.clone(), HashMap::new());
    let live = Live::default();
    let kind = CacheKind::of::<virtual_column::Meta>();
    let cache = MemCache::<Item>::new(store, kind);
    let held = cache.put("a", live.item(100));
    cache.put("b", live.item(50));
    cache.evict("a");
    for _ in 0..3 {
        cache.lookup("b");
    }
    cache.lookup("missing");

    let rows = manager.rows();
    assert_eq!(rows.len(), CacheKind::all().count() + Pool::ALL.len());

    let row = find(&rows, "memory_cache_virtual_column_file_meta_data");
    assert_eq!(row.kind, Some(kind));
    assert_eq!((row.num_items, row.size, row.in_cache_size), (1, 150, 50));
    assert_eq!((row.capacity, row.unit), (None, Unit::Bytes));
    assert!(row.enabled);
    assert!(row.access >= 4 && row.hit >= 3 && row.miss >= 1);

    // Unlimited kind, but its pool has no capacity.
    assert!(!find(&rows, "memory_cache_column_data").enabled);

    let meta = find(&rows, "__pool:meta");
    assert_eq!(
        (meta.capacity, meta.size, meta.in_cache_size),
        (Some(1000), 150, 50)
    );
    assert!(!find(&rows, "__pool:data").enabled);
    drop(held);
}

#[test]
fn test_legacy_rows_have_no_pools() {
    let store: Arc<dyn KindStore> = Arc::new(LegacyStore::new([(
        CacheKind::of::<table::Snapshot>(),
        LegacyCapacity::Items(10),
    )]));
    let rows = CacheManager::new(store, HashMap::new()).rows();
    assert_eq!(rows.len(), CacheKind::all().count());
    let snapshot = find(&rows, "memory_cache_table_snapshot");
    assert_eq!((snapshot.capacity, snapshot.unit), (Some(10), Unit::Count));
    assert!(snapshot.enabled);
    assert!(!find(&rows, "memory_cache_table_statistics").enabled);
}
