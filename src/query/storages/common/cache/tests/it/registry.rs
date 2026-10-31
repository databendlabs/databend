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

use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_storages_common_cache::CacheDef;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheRegistry;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::vector;

use crate::fakes::Item;

/// The names are the public ones of `system.caches` and metrics; their order is the row order.
#[test]
fn test_builtin_kinds() {
    let names: Vec<_> = CacheKind::all().map(CacheKind::name).collect();
    assert_eq!(names, [
        "memory_cache_table_snapshot",
        "memory_cache_table_statistics",
        "memory_cache_compact_segment_info",
        "memory_cache_column_oriented_segment_info",
        "memory_cache_segment_block_metas",
        "memory_cache_segment_statistics",
        "memory_cache_bloom_index_file_meta_data",
        "memory_cache_inverted_index_meta_data",
        "memory_cache_vector_index_file_meta_data",
        "memory_cache_spatial_index_file_meta_data",
        "memory_cache_virtual_column_file_meta_data",
        "memory_cache_parquet_meta_data",
        "memory_cache_iceberg_table",
        "memory_cache_prune_partitions",
        "memory_cache_bloom_index_filter",
        "memory_cache_vector_index_file",
        "memory_cache_vector_index_refine",
        "memory_cache_inverted_index_lookup",
        "memory_cache_inverted_index_payload",
        "memory_cache_inverted_index_full_payload",
        "memory_cache_spatial_index_file",
        "memory_cache_column_data",
        "memory_cache_table_data",
    ]);
    for (index, kind) in CacheKind::all().enumerate() {
        assert_eq!(kind.index(), index);
        assert_eq!(format!("{kind:?}"), kind.name());
    }
}

/// Meta kinds come first, then the index kinds; the column kinds are the data pool. The kinds
/// off by default in the legacy backend, block metas and the column kinds, are off in the
/// shared backend too until sized.
#[test]
fn test_pools_and_shared_limits() {
    let in_pool = |pool| {
        CacheKind::all()
            .filter(|k| k.pool() == pool)
            .map(CacheKind::name)
            .collect::<Vec<_>>()
    };
    assert_eq!(in_pool(Pool::Meta).len(), 14);
    assert_eq!(in_pool(Pool::Index).len(), 7);
    assert_eq!(in_pool(Pool::Data), [
        "memory_cache_column_data",
        "memory_cache_table_data"
    ]);
    for kind in CacheKind::all() {
        let disabled = kind.pool() == Pool::Data || kind == CacheKind::of::<segment::BlockMetas>();
        assert_eq!(kind.info().shared_limit.is_disabled(), disabled, "{kind:?}");
        if !disabled {
            assert_eq!(kind.info().shared_limit, KindLimit::UNLIMITED);
        }
    }
}

/// Disk tiers keep their pre-refactor directories, so a restart reuses their files.
#[test]
fn test_disk_tiers_keep_their_directories() {
    let dirs: Vec<_> = CacheKind::all()
        .filter_map(|k| k.info().disk.map(|disk| (k.name(), disk.dir)))
        .collect();
    assert_eq!(dirs, [
        ("memory_cache_bloom_index_file_meta_data", "bloom_meta_v1"),
        (
            "memory_cache_inverted_index_meta_data",
            "inverted_index_meta_v1"
        ),
        (
            "memory_cache_vector_index_file_meta_data",
            "vector_index_meta_v1"
        ),
        (
            "memory_cache_spatial_index_file_meta_data",
            "spatial_index_meta_v1"
        ),
        (
            "memory_cache_virtual_column_file_meta_data",
            "virtual_column_meta_v1"
        ),
        ("memory_cache_bloom_index_filter", "bloom_filter_v1"),
        ("memory_cache_vector_index_refine", "vector_index_file_v1"),
        (
            "memory_cache_inverted_index_lookup",
            "inverted_index_lookup_v1"
        ),
        (
            "memory_cache_inverted_index_payload",
            "inverted_index_payload_v1"
        ),
        ("memory_cache_spatial_index_file", "spatial_index_file_v1"),
        ("memory_cache_column_data", "v1"),
    ]);
}

/// Each tier is sized by its own option; column data only with the disk data cache storage.
#[test]
fn test_disk_tier_sizes() {
    let config = CacheConfig {
        disk_cache_table_bloom_index_data_size: 7,
        ..CacheConfig::default()
    };
    let bytes = |kind: CacheKind, config: &CacheConfig| (kind.info().disk.unwrap().bytes)(config);
    assert_eq!(bytes(CacheKind::of::<bloom::Filter>(), &config), 7);
    let column_data = CacheKind::of::<column::Data>();
    assert_eq!(bytes(column_data, &CacheConfig::default()), 0);
    let mut config = CacheConfig {
        data_cache_storage: CacheStorageTypeInnerConfig::Disk,
        ..CacheConfig::default()
    };
    config.disk_cache_config.max_bytes = 9;
    assert_eq!(bytes(column_data, &config), 9);
    assert_eq!(
        (column_data.info().disk.unwrap().reload_policy)(&config),
        config.data_cache_key_reload_policy
    );
}

#[test]
fn test_lookup_by_type_and_name() {
    let filter = CacheKind::of::<bloom::Filter>();
    assert_eq!(filter.name(), bloom::Filter::NAME);
    assert_eq!(CacheKind::from_name(bloom::Filter::NAME), Some(filter));
    // The name of the pre-refactor hybrid cache.
    assert_eq!(
        CacheKind::from_name("cache_bloom_index_filter"),
        Some(filter)
    );
    assert_eq!(CacheKind::from_name("bloom_filter"), None);
    // Two kinds may share a value type; they are still told apart by their definition.
    assert_ne!(
        CacheKind::of::<inverted::Payload>(),
        CacheKind::of::<inverted::FullPayload>()
    );
    assert_eq!(CacheKind::of::<vector::Index>().pool(), Pool::Index);
}

struct First;

impl CacheDef for First {
    type Value = Item;
    const NAME: &'static str = "memory_cache_first";
    const POOL: Pool = Pool::Meta;
}

struct Second;

impl CacheDef for Second {
    type Value = Item;
    const NAME: &'static str = "memory_cache_second";
    const POOL: Pool = Pool::Data;
    const SHARED_LIMIT: KindLimit = KindLimit::bytes(10);
}

/// A kind with the name of a registered one.
struct SameName;

impl CacheDef for SameName {
    type Value = Item;
    const NAME: &'static str = First::NAME;
    const POOL: Pool = Pool::Meta;
}

#[test]
fn test_register_numbers_kinds_in_order() {
    let mut registry = CacheRegistry::default();
    registry.register::<First>().register::<Second>();
    let second = registry.kind_of::<Second>();
    assert_eq!(registry.kinds().collect::<Vec<_>>(), [
        registry.kind_of::<First>(),
        second
    ]);
    assert_eq!(second.index(), 1);
    let info = registry.info(second);
    assert_eq!(
        (info.name, info.pool, info.shared_limit),
        ("memory_cache_second", Pool::Data, KindLimit::bytes(10))
    );
    assert!(info.disk.is_none());
    assert_eq!(registry.find("cache_second"), Some(second));
}

#[test]
#[should_panic(expected = "cache memory_cache_first is registered twice")]
fn test_register_rejects_a_name_twice() {
    let mut registry = CacheRegistry::default();
    registry.register::<First>().register::<SameName>();
}

#[test]
#[should_panic(expected = "cache memory_cache_second is not registered")]
fn test_unregistered_kind_panics() {
    let mut registry = CacheRegistry::default();
    registry.register::<First>();
    registry.kind_of::<Second>();
}
