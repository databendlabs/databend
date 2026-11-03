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
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::legacy::LegacyCapacity;
use databend_storages_common_cache::legacy::LegacyKinds;

/// Every legacy option set to a distinct, non-default value.
fn configured() -> CacheConfig {
    CacheConfig {
        table_meta_snapshot_count: 1,
        table_meta_statistic_count: 2,
        table_meta_segment_bytes: 3,
        segment_block_metas_count: 4,
        segment_statistics_bytes: 5,
        table_bloom_index_meta_count: 6,
        inverted_index_meta_count: 7,
        vector_index_meta_count: 8,
        spatial_index_meta_count: 9,
        virtual_column_meta_count: 10,
        iceberg_table_meta_count: 11,
        table_prune_partitions_count: 12,
        table_bloom_index_filter_size: 13,
        vector_index_filter_size: 14,
        vector_index_refine_size: 15,
        inverted_index_lookup_size: 16,
        inverted_index_payload_size: 400,
        spatial_index_filter_size: 17,
        data_cache_storage: CacheStorageTypeInnerConfig::Disk,
        data_cache_in_memory_bytes: 18,
        table_data_deserialized_data_bytes: 19,
        ..CacheConfig::default()
    }
}

fn capacities(config: &CacheConfig, memory: u64) -> Vec<(&'static str, LegacyCapacity)> {
    LegacyKinds::global()
        .capacities(config, memory)
        .into_iter()
        .map(|(kind, capacity)| (kind.name(), capacity))
        .collect()
}

fn capacity(config: &CacheConfig, memory: u64, name: &str) -> LegacyCapacity {
    capacities(config, memory)
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap()
        .1
}

/// Each kind is sized by its pre-refactor option, in its pre-refactor unit.
#[test]
fn test_each_kind_reads_its_option() {
    use LegacyCapacity::*;
    assert_eq!(capacities(&configured(), 1 << 30), [
        ("memory_cache_table_snapshot", Items(1)),
        ("memory_cache_table_statistics", Items(2)),
        ("memory_cache_compact_segment_info", Bytes(3)),
        ("memory_cache_column_oriented_segment_info", Bytes(3)),
        ("memory_cache_segment_block_metas", Items(4)),
        ("memory_cache_segment_statistics", Bytes(5)),
        ("memory_cache_bloom_index_file_meta_data", Items(6)),
        ("memory_cache_inverted_index_meta_data", Items(7)),
        ("memory_cache_vector_index_file_meta_data", Items(8)),
        ("memory_cache_spatial_index_file_meta_data", Items(9)),
        ("memory_cache_virtual_column_file_meta_data", Items(10)),
        ("memory_cache_parquet_meta_data", Items(3000)),
        ("memory_cache_iceberg_table", Items(11)),
        ("memory_cache_prune_partitions", Items(12)),
        ("memory_cache_bloom_index_filter", Bytes(13)),
        ("memory_cache_vector_index_file", Bytes(14)),
        ("memory_cache_vector_index_refine", Bytes(15)),
        ("memory_cache_inverted_index_lookup", Bytes(16)),
        ("memory_cache_inverted_index_payload", Bytes(300)),
        ("memory_cache_inverted_index_full_payload", Bytes(100)),
        ("memory_cache_spatial_index_file", Bytes(17)),
        ("memory_cache_column_data", Bytes(18)),
        ("memory_cache_table_data", Bytes(19)),
    ]);
}

#[test]
fn test_defaults() {
    let config = CacheConfig::default();
    let memory = 1 << 30;
    assert_eq!(
        capacity(&config, memory, "memory_cache_table_snapshot"),
        LegacyCapacity::Items(256)
    );
    // Off unless configured: block metas, and column data without the disk data cache storage.
    assert_eq!(
        capacity(&config, memory, "memory_cache_segment_block_metas"),
        LegacyCapacity::Disabled
    );
    assert_eq!(
        capacity(&config, memory, "memory_cache_column_data"),
        LegacyCapacity::Disabled
    );
}

#[test]
fn test_memory_ratio_overrides_bytes() {
    let config = CacheConfig {
        vector_index_filter_memory_ratio: 10,
        spatial_index_filter_memory_ratio: 20,
        table_data_deserialized_data_bytes: 0,
        table_data_deserialized_memory_ratio: 5,
        ..configured()
    };
    let capacity = |name| capacity(&config, 1000, name);
    assert_eq!(
        capacity("memory_cache_vector_index_file"),
        LegacyCapacity::Bytes(100)
    );
    assert_eq!(
        capacity("memory_cache_spatial_index_file"),
        LegacyCapacity::Bytes(200)
    );
    assert_eq!(
        capacity("memory_cache_table_data"),
        LegacyCapacity::Bytes(50)
    );
}

/// `enable_table_meta_cache = false` turns off every kind outside the data pool.
#[test]
fn test_meta_cache_switch() {
    let config = CacheConfig {
        enable_table_meta_cache: false,
        ..configured()
    };
    for (kind, capacity) in LegacyKinds::global().capacities(&config, 1 << 30) {
        let kept = kind.pool() == Pool::Data;
        assert_eq!(capacity != LegacyCapacity::Disabled, kept, "{kind:?}");
    }
}

/// Only the HNSW cache counts held values, and only the payload pages share their budget.
#[test]
fn test_special_rules() {
    let legacy = LegacyKinds::global();
    for kind in CacheKind::all() {
        let rules = legacy.get(kind).unwrap();
        assert_eq!(
            rules.counts_held_values,
            kind == CacheKind::of::<vector::Index>(),
            "{kind:?}"
        );
        let shares = (kind == CacheKind::of::<inverted::Payload>())
            .then(CacheKind::of::<inverted::FullPayload>);
        assert_eq!(rules.shares_budget_with, shares, "{kind:?}");
    }
}

/// The shared backend warns about every legacy option it ignores, once each.
#[test]
fn test_ignored_options() {
    let legacy = LegacyKinds::global();
    assert!(
        legacy
            .ignored_options(&CacheConfig::default(), 1 << 30)
            .is_empty()
    );
    let config = CacheConfig {
        enable_table_meta_cache: false,
        ..configured()
    };
    assert_eq!(legacy.ignored_options(&config, 1 << 30), [
        "enable_table_meta_cache",
        "table_meta_snapshot_count",
        "table_meta_statistic_count",
        "table_meta_segment_bytes",
        "segment_block_metas_count",
        "segment_statistics_bytes",
        "table_bloom_index_meta_count",
        "inverted_index_meta_count",
        "vector_index_meta_count",
        "spatial_index_meta_count",
        "virtual_column_meta_count",
        "iceberg_table_meta_count",
        "table_prune_partitions_count",
        "table_bloom_index_filter_size",
        "vector_index_filter_size, vector_index_filter_memory_ratio",
        "vector_index_refine_size",
        "inverted_index_lookup_size",
        "inverted_index_payload_size",
        "spatial_index_filter_size, spatial_index_filter_memory_ratio",
        "data_cache_in_memory_bytes",
        "table_data_deserialized_data_bytes, table_data_deserialized_memory_ratio",
    ]);
}
