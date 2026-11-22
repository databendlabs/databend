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

use std::sync::Arc;

use bytes::Bytes;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CacheRow;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::MAX_AUTO_DISK_CACHE_BYTES;
use databend_storages_common_cache::MAX_AUTO_WATERMARK_RATIO;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::auto_max_bytes;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::virtual_column;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyStore;
use databend_storages_common_cache::split_caps;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use tempfile::TempDir;

use crate::disk::files_under;
use crate::fakes::Item;
use crate::fakes::Live;
use crate::fakes::shared_store;
use crate::fakes::wait_for;

const MIB: u64 = 1 << 20;

/// The disk cache on, of 1 MiB, under `dir`, with the default pool ratios 10/20/70, over the
/// legacy memory backend.
pub fn disk_config(dir: &TempDir) -> CacheConfig {
    CacheConfig {
        data_cache_storage: CacheStorageTypeInnerConfig::Disk,
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: Some(MIB),
            path: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        },
        ..legacy_config()
    }
}

fn legacy_config() -> CacheConfig {
    CacheConfig {
        shared_memory_enabled: false,
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

fn disk_rows(manager: &CacheManager) -> Vec<CacheRow> {
    manager.rows().into_iter().filter(|r| r.disk).collect()
}

fn disk_kinds() -> usize {
    CacheKind::all().filter(|k| k.on_disk()).count()
}

/// The disk cache exists in EE mode with `data_cache_storage = disk` and `max_bytes` over 1 KiB.
/// It then lists a row per kind kept on disk and per disk pool, and the license can turn it off
/// and on at runtime.
#[test]
fn test_disk_cache_follows_ee_mode_and_config() {
    let dir = TempDir::new().unwrap();
    let config = disk_config(&dir);
    let ee = manager(&config, true);
    assert!(ee.disk().is_some());
    assert_eq!(disk_rows(&ee).len(), disk_kinds() + Pool::ALL.len());
    assert!(row(&ee, "disk_cache_column_data").is_some());
    assert!(row(&ee, "disk_cache_compact_segment_info").is_some());
    // Kinds not kept on disk have no disk row.
    assert!(ee.rows().iter().all(|r| r.name != "disk_cache_table_data"));
    ee.set_disk_allowed(false);
    assert!(disk_rows(&ee).is_empty());
    ee.set_disk_allowed(true);
    assert_eq!(disk_rows(&ee).len(), disk_kinds() + Pool::ALL.len());

    assert!(manager(&config, false).disk().is_none());
    assert!(manager(&legacy_config(), true).disk().is_none());
    let tiny = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: Some(1024),
            ..config.disk_cache_config.clone()
        },
        ..config.clone()
    };
    assert!(manager(&tiny, true).disk().is_none());
    // All ratios 0 leave nothing to split.
    let no_pools = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            meta_max_ratio: 0,
            index_max_ratio: 0,
            data_max_ratio: 0,
            ..config.disk_cache_config.clone()
        },
        ..config
    };
    assert!(CacheManager::try_new(&no_pools, &MIB, "t", true).is_err());
}

/// Total and available bytes of the file system under `path`, as the disk cache reads them.
fn file_system_space(path: &std::path::Path) -> (u64, u64) {
    let stat = rustix::fs::statvfs(path).unwrap();
    let block = if stat.f_frsize > 0 {
        stat.f_frsize
    } else {
        stat.f_bsize
    };
    (stat.f_blocks * block, stat.f_bavail * block)
}

/// `max_bytes` unset sizes the disk cache at start from the file system under `path`: the room
/// below the watermark, split among the pools by their ratios. The files of the previous run
/// count as room, by the space they take: the cache is as large as without them.
#[test]
fn test_disk_cache_unset_max_bytes_sizes_from_the_file_system() {
    // Other tests change what the file system has available meanwhile.
    const SLACK: u64 = 24 * MIB;
    let dir = TempDir::new().unwrap();
    let (total, before) = file_system_space(dir.path());
    // Files much smaller than a block: 16384 of 100 bytes take 64 MiB of 4 KiB blocks for
    // 1.6 MB of length, so leaving them out or counting their lengths misses it by more than
    // the slack.
    let old_layout = dir.path().join("test_tenant").join("v1");
    std::fs::create_dir_all(&old_layout).unwrap();
    for i in 0..16384 {
        std::fs::write(old_layout.join(format!("entry_{i}")), [0u8; 100]).unwrap();
    }
    let config = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: None,
            // The most room, wherever the test runs; held to `MAX_AUTO_WATERMARK_RATIO`.
            watermark_ratio: 100,
            ..disk_config(&dir).disk_cache_config
        },
        ..disk_config(&dir)
    };
    let manager = manager(&config, true);
    let max_bytes = manager.disk().unwrap().max_bytes();
    let without_them = auto_max_bytes(total, before, 0, MAX_AUTO_WATERMARK_RATIO);
    let low = without_them.saturating_sub(SLACK);
    let high = without_them + SLACK;
    assert!(
        (low..=high).contains(&max_bytes),
        "{max_bytes} not in {low}..={high}"
    );
    assert!(max_bytes <= MAX_AUTO_DISK_CACHE_BYTES);
    // The writer evicts under disk pressure before the disk is full, also for entries smaller
    // than a block, which the pools count by length.
    let writer = manager.disk().unwrap().writer_options();
    assert_eq!(writer.watermark_ratio, MAX_AUTO_WATERMARK_RATIO);
    // Sized in bytes, the cache keeps the watermark it is given.
    let sized = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: Some(MIB),
            ..config.disk_cache_config.clone()
        },
        ..config.clone()
    };
    let sized = CacheManager::try_new(&sized, &MIB, "sized_tenant", true).unwrap();
    assert_eq!(sized.disk().unwrap().writer_options().watermark_ratio, 100);
    let caps = split_caps(max_bytes, [10, 20, 70]).unwrap();
    assert_eq!(capacity(&manager, "__disk_pool:meta"), Some(caps[0]));
    assert_eq!(capacity(&manager, "__disk_pool:index"), Some(caps[1]));
    assert_eq!(capacity(&manager, "__disk_pool:data"), Some(caps[2]));
}

/// `max_bytes` unset leaves the disk cache off when the file system has no room below the
/// watermark, deleting the files of the previous run, which no writer would evict; and fails
/// the start when the file system under `path` cannot be read. Turned off by `max_bytes`, the
/// cache leaves its files alone.
#[test]
fn test_disk_cache_unset_max_bytes_without_room_or_file_system() {
    let dir = TempDir::new().unwrap();
    let tenant_root = dir.path().join("test_tenant");
    let previous_run = || {
        for file in [
            tenant_root.join("v2/data/column_data/aa/x"),
            tenant_root.join("v1/y"),
        ] {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"cached").unwrap();
        }
    };
    let config = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: None,
            // The headroom reserves the whole file system: never any room.
            watermark_ratio: 1,
            ..disk_config(&dir).disk_cache_config
        },
        // Would keep the files of the previous run for the next start.
        data_cache_key_reload_policy: DiskCacheKeyReloadPolicy::Fuzzy,
        ..disk_config(&dir)
    };
    let (total, available) = file_system_space(dir.path());
    assert_eq!(auto_max_bytes(total, available, 0, 1), 0);
    previous_run();
    assert!(manager(&config, true).disk().is_none());
    wait_for(|| files_under(&tenant_root).is_empty());

    previous_run();
    let turned_off = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            max_bytes: Some(0),
            ..config.disk_cache_config.clone()
        },
        ..config.clone()
    };
    assert!(manager(&turned_off, true).disk().is_none());
    assert_eq!(files_under(&tenant_root).len(), 2);

    let file = dir.path().join("file");
    std::fs::write(&file, b"").unwrap();
    let path = file.join("cache");
    let unreadable = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            path: path.to_string_lossy().to_string(),
            ..config.disk_cache_config.clone()
        },
        ..config
    };
    let err = CacheManager::try_new(&unreadable, &MIB, "test_tenant", true)
        .err()
        .unwrap();
    assert!(err.message().contains(&*path.to_string_lossy()), "{err}");
}

/// The disk cache is independent of the memory backend and its switches; the options of the
/// pre-refactor disk tiers are accepted and do nothing.
#[test]
fn test_disk_cache_ignores_memory_options_and_legacy_disk_options() {
    let dir = TempDir::new().unwrap();
    for config in [
        CacheConfig {
            enable_table_meta_cache: false,
            ..disk_config(&dir)
        },
        CacheConfig {
            shared_memory_enabled: true,
            ..disk_config(&dir)
        },
        CacheConfig {
            disk_cache_table_bloom_index_data_size: 1,
            disk_cache_virtual_column_meta_size: 1,
            table_data_cache_population_queue_size: 1,
            ..disk_config(&dir)
        },
    ] {
        let manager = manager(&config, true);
        assert_eq!(disk_rows(&manager).len(), disk_kinds() + Pool::ALL.len());
        assert_eq!(
            capacity(&manager, "__disk_pool:index"),
            Some(MIB * 20 / 100)
        );
    }
}

/// A disk row counts the kind's lookups; its pool row sums them and holds the entries.
#[tokio::test(flavor = "multi_thread")]
async fn test_disk_rows_count_hits_and_entries() {
    let dir = TempDir::new().unwrap();
    let manager = manager(&disk_config(&dir), true);
    let disk = manager.disk().unwrap();
    let kind = CacheKind::of::<column::Data>();
    let key = DiskKey::range("1/2/_b/x.parquet", 0..10);
    assert!(
        disk.put(kind, &key, Bytes::from_static(b"0123456789"))
            .await
    );
    wait_for(|| disk.contains(kind, &key));
    assert!(disk.get(kind, &key).await.is_some());
    assert!(disk.get(kind, &DiskKey::whole("missing")).await.is_none());

    let column_data = row(&manager, "disk_cache_column_data").unwrap();
    assert_eq!((column_data.num_items, column_data.size), (1, 14));
    assert_eq!(
        (column_data.capacity, column_data.unit),
        (None, Unit::Bytes)
    );
    assert!(column_data.disk && column_data.pool.is_none());
    // From metric families shared by every disk cache of the process, other tests' included:
    // read one after the other, while they may change.
    assert!(column_data.hit >= 1 && column_data.miss >= 1);
    assert!(column_data.access >= 2);

    let pool = row(&manager, "__disk_pool:data").unwrap();
    assert_eq!((pool.num_items, pool.size, pool.in_cache_size), (1, 14, 14));
    assert_eq!(pool.pool, Some(Pool::Data));
    assert_eq!((pool.hit, pool.miss), (1, 1));
    // The memory row of the kind is separate and empty.
    let memory = manager
        .rows()
        .into_iter()
        .find(|r| r.name == "memory_cache_column_data")
        .unwrap();
    assert!(!memory.disk && memory.num_items == 0);
}

/// Pools split `max_bytes` by their ratios, normalized by their sum; 0 turns a pool off.
#[test]
fn test_disk_pools_by_ratio() {
    let dir = TempDir::new().unwrap();
    let default = manager(&disk_config(&dir), true);
    assert_eq!(capacity(&default, "__disk_pool:meta"), Some(MIB / 10));
    assert_eq!(
        capacity(&default, "__disk_pool:index"),
        Some(MIB * 20 / 100)
    );
    assert_eq!(capacity(&default, "__disk_pool:data"), Some(MIB * 70 / 100));

    let mut config = disk_config(&dir);
    config.disk_cache_config.meta_max_ratio = 1;
    config.disk_cache_config.index_max_ratio = 0;
    config.disk_cache_config.data_max_ratio = 3;
    let manager = manager(&config, true);
    assert_eq!(capacity(&manager, "__disk_pool:meta"), Some(MIB / 4));
    assert_eq!(capacity(&manager, "__disk_pool:data"), Some(MIB * 3 / 4));
    // An empty pool turns its kinds off.
    assert!(row(&manager, "__disk_pool:index").is_none());
    assert!(row(&manager, "disk_cache_bloom_index_filter").is_none());
}

/// `set_cache_capacity` on a disk pool takes bytes, `'<n>%'` of `max_bytes` or `'default'`;
/// on a kind, 0 turns its disk cache off and anything else turns it back on.
#[test]
fn test_set_disk_capacity() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let manager = manager(&disk_config(&dir), true);
    let data = "__disk_pool:data";

    manager.set_cache_capacity_arg("disk_pool:data", "50%")?;
    assert_eq!(capacity(&manager, data), Some(MIB / 2));
    manager.set_cache_capacity_arg("disk_pool:data", " 1000 ")?;
    assert_eq!(capacity(&manager, data), Some(1000));
    manager.set_cache_capacity_arg("disk_pool:data", "0")?;
    assert!(row(&manager, data).is_none());
    assert!(row(&manager, "disk_cache_column_data").is_none());
    manager.set_cache_capacity_arg("disk_pool:data", "default")?;
    assert_eq!(capacity(&manager, data), Some(MIB * 70 / 100));
    assert!(row(&manager, "disk_cache_column_data").is_some());
    // The pools together stay within max_bytes.
    assert!(
        manager
            .set_cache_capacity_arg("disk_pool:data", "90%")
            .is_err()
    );
    assert!(
        manager
            .set_cache_capacity_arg("disk_pool:data", "101%")
            .is_err()
    );
    for bad in ["-1", "1.5", "abc", "%"] {
        assert!(
            manager
                .set_cache_capacity_arg("disk_pool:data", bad)
                .is_err(),
            "{bad}"
        );
    }
    assert!(
        manager
            .set_cache_capacity_arg("disk_pool:none", "0")
            .is_err()
    );
    assert_eq!(capacity(&manager, data), Some(MIB * 70 / 100));

    let column_data = "disk_cache_column_data";
    manager.set_cache_capacity_arg("disk:memory_cache_column_data", "0")?;
    assert!(row(&manager, column_data).is_none());
    manager.set_cache_capacity_arg("disk:memory_cache_column_data", "default")?;
    assert!(row(&manager, column_data).is_some());
    manager.set_cache_capacity_arg("disk:memory_cache_column_data", "0")?;
    manager.set_cache_capacity_arg("disk:memory_cache_column_data", "1")?;
    assert!(row(&manager, column_data).is_some());
    // Kinds not kept on disk, and unknown kinds.
    assert!(
        manager
            .set_cache_capacity_arg("disk:memory_cache_table_data", "0")
            .is_err()
    );
    assert!(
        manager
            .set_cache_capacity_arg("disk:no_such_cache", "0")
            .is_err()
    );

    // Memory caches parse the same arguments.
    manager.set_cache_capacity_arg("memory_cache_table_snapshot", "1")?;
    assert_eq!(capacity(&manager, "memory_cache_table_snapshot"), Some(1));
    assert!(
        manager
            .set_cache_capacity_arg("memory_cache_table_snapshot", "10%")
            .is_err()
    );

    // Without a disk cache.
    let no_disk = self::manager(&legacy_config(), true);
    assert!(
        no_disk
            .set_cache_capacity_arg("disk_pool:data", "0")
            .is_err()
    );
    assert!(
        no_disk
            .set_cache_capacity_arg("disk:memory_cache_column_data", "0")
            .is_err()
    );
    Ok(())
}

/// The writer starts as `cache.disk` configures it.
#[test]
fn test_writer_starts_from_the_disk_config() {
    let dir = TempDir::new().unwrap();
    let config = CacheConfig {
        disk_cache_config: DiskCacheInnerConfig {
            queue_bytes: 4 * MIB,
            queue_wait_ms: 20,
            watermark_ratio: 80,
            sync_data: true,
            writer_threads: 2,
            ..disk_config(&dir).disk_cache_config
        },
        ..disk_config(&dir)
    };
    let manager = manager(&config, true);
    let options = manager.disk().unwrap().writer_options();
    assert_eq!(
        (
            options.writers,
            options.queue_bytes,
            options.queue_wait,
            options.watermark_ratio,
            options.sync_data
        ),
        (2, 4 * MIB, std::time::Duration::from_millis(20), 80, true)
    );
    // The writer settings change with the config, not at runtime.
    assert!(
        manager
            .set_cache_capacity_arg("disk.queue_bytes", "1")
            .is_err()
    );
}

/// Writer settings out of range in `cache.disk` fail the start.
#[test]
fn test_disk_options_out_of_range_fail_the_start() {
    let dir = TempDir::new().unwrap();
    let base = disk_config(&dir);
    let bad = [
        DiskCacheInnerConfig {
            queue_bytes: 0,
            ..base.disk_cache_config.clone()
        },
        DiskCacheInnerConfig {
            watermark_ratio: 0,
            ..base.disk_cache_config.clone()
        },
        DiskCacheInnerConfig {
            watermark_ratio: 101,
            ..base.disk_cache_config.clone()
        },
        DiskCacheInnerConfig {
            writer_threads: 0,
            ..base.disk_cache_config.clone()
        },
        DiskCacheInnerConfig {
            writer_threads: 257,
            ..base.disk_cache_config.clone()
        },
    ];
    for disk in bad {
        let config = CacheConfig {
            disk_cache_config: disk.clone(),
            ..base.clone()
        };
        assert!(
            CacheManager::try_new(&config, &MIB, "test_tenant", true).is_err(),
            "{disk:?}"
        );
    }
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
    let manager = manager(&legacy_config(), false);
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
        // The data pool is off by default.
        data_max_ratio: 10,
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
    // On without an entry, bounded only by the pool.
    let table_data = row(&manager, "memory_cache_table_data").unwrap();
    assert_eq!(table_data.capacity, None);
    // Off without an entry: decoded block metas are several times the segment.
    assert!(row(&manager, "memory_cache_segment_block_metas").is_none());
    let opted_in = CacheConfig {
        kind_max_bytes: [("memory_cache_segment_block_metas".to_string(), 3000)].into(),
        ..shared_config()
    };
    let opted_in = CacheManager::try_new(&opted_in, &1_000_000, "test", false)?;
    assert_eq!(
        capacity(&opted_in, "memory_cache_segment_block_metas"),
        Some(3000)
    );

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

/// A kind is turned off by 0, sized by bytes, and put back to its default by `default`: bounded
/// only by its pool, or off for raw column data and decoded block metas. The legacy backend has
/// no such default.
#[test]
fn test_capacity_default() -> Result<()> {
    let config = CacheConfig {
        // The data pool is off by default; the kinds in it need a pool to be sized within.
        data_max_ratio: 10,
        ..shared_config()
    };
    let shared = CacheManager::try_new(&config, &1_000_000, "test", false)?;
    let name = "memory_cache_table_data";
    shared.set_cache_capacity(name, 0)?;
    assert!(row(&shared, name).is_none());
    shared.set_cache_capacity_default(name)?;
    assert_eq!(row(&shared, name).unwrap().capacity, None);
    shared.set_cache_capacity(name, 5000)?;
    assert_eq!(capacity(&shared, name), Some(5000));
    shared.set_cache_capacity_default(name)?;
    assert_eq!(row(&shared, name).unwrap().capacity, None);
    for off in [
        "memory_cache_column_data",
        "memory_cache_segment_block_metas",
    ] {
        assert!(row(&shared, off).is_none(), "{off}");
        shared.set_cache_capacity(off, 5000)?;
        assert_eq!(capacity(&shared, off), Some(5000), "{off}");
        shared.set_cache_capacity_default(off)?;
        assert!(row(&shared, off).is_none(), "{off}");
    }
    for other in ["all", "pool:data", "no_such_cache"] {
        assert!(shared.set_cache_capacity_default(other).is_err(), "{other}");
    }

    let legacy = manager(&legacy_config(), false);
    assert!(
        legacy
            .set_cache_capacity_default("memory_cache_table_snapshot")
            .is_err()
    );
    let snapshot = row(&legacy, "memory_cache_table_snapshot").unwrap();
    assert_eq!(snapshot.capacity, Some(256));
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
    // Without a server memory limit the pools would be neither bounded nor reclaimed.
    assert!(CacheManager::try_new(&shared_config(), &0, "test", false).is_err());
    // The legacy backend keeps working without one.
    let legacy = CacheConfig {
        shared_memory_enabled: false,
        ..shared_config()
    };
    assert!(CacheManager::try_new(&legacy, &0, "test", false).is_ok());
}

fn find<'a>(rows: &'a [CacheRow], name: &str) -> &'a CacheRow {
    rows.iter().find(|r| r.name == name).unwrap()
}

#[test]
fn test_rows_cover_kinds_and_pools() {
    let store = shared_store(1000, 2000, 0);
    let manager = CacheManager::new(store.clone(), None);
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
    let rows = CacheManager::new(store, None).rows();
    assert_eq!(rows.len(), CacheKind::all().count());
    let snapshot = find(&rows, "memory_cache_table_snapshot");
    assert_eq!((snapshot.capacity, snapshot.unit), (Some(10), Unit::Count));
    assert!(snapshot.enabled);
    assert!(!find(&rows, "memory_cache_table_statistics").enabled);
}

/// `reset`, the default, starts the disk cache empty, so nothing cached before a restart is
/// read after it, whatever happened to the objects meanwhile; the files are deleted in the
/// background. `fuzzy` reloads what the previous run cached.
#[tokio::test(flavor = "multi_thread")]
async fn test_restart_reloads_only_with_fuzzy() {
    let dir = TempDir::new().unwrap();
    let kind = CacheKind::of::<column::Data>();
    let key = DiskKey::range("1/2/_b/x.parquet", 0..10);
    let fuzzy = CacheConfig {
        data_cache_key_reload_policy: DiskCacheKeyReloadPolicy::Fuzzy,
        ..disk_config(&dir)
    };
    {
        let manager = manager(&fuzzy, true);
        let disk = manager.disk().unwrap();
        let value = Bytes::from_static(b"0123456789");
        assert!(disk.put(kind, &key, value).await);
        wait_for(|| disk.contains(kind, &key));
    }
    {
        let manager = manager(&fuzzy, true);
        let disk = manager.disk().unwrap();
        wait_for(|| disk.contains(kind, &key));
    }
    // A layout set aside by an earlier reset that did not get to delete it is removed first.
    let tenant = dir.path().join("test_tenant");
    std::fs::create_dir_all(tenant.join("v2.reset/data")).unwrap();
    std::fs::write(tenant.join("v2.reset/data/left"), b"x").unwrap();
    let manager = manager(&disk_config(&dir), true);
    let disk = manager.disk().unwrap();
    assert!(!disk.contains(kind, &key));
    assert!(disk.get(kind, &key).await.is_none());
    wait_for(|| std::fs::read_dir(&tenant).unwrap().count() == 1);
    assert!(tenant.join("v2").is_dir());
}
