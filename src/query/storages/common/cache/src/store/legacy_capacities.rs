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

use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;

use crate::Unit;
use crate::cache_kind::CacheKind;
use crate::manager::inverted_index_payload_capacities;
use crate::store::LegacyCapacity;

const PARQUET_META_ITEMS: usize = 3000;

/// Capacities of the pre-refactor caches, one per kind. A capacity of 0 disables the kind.
pub fn legacy_capacities(
    config: &CacheConfig,
    max_server_memory_usage: u64,
) -> Vec<(CacheKind, LegacyCapacity)> {
    let ratio_or = |ratio: u64, bytes: u64| match ratio {
        0 => bytes as usize,
        _ => (max_server_memory_usage as u128 * ratio as u128 / 100) as usize,
    };
    let column_array = match config.table_data_deserialized_data_bytes {
        0 => ratio_or(config.table_data_deserialized_memory_ratio, 0),
        bytes => bytes as usize,
    };
    let column_data = match config.data_cache_storage {
        CacheStorageTypeInnerConfig::Disk => config.data_cache_in_memory_bytes as usize,
        CacheStorageTypeInnerConfig::None => 0,
    };
    let (payload, full_payload) =
        inverted_index_payload_capacities(config.inverted_index_payload_size as usize);

    use LegacyCapacity::Bytes;
    use LegacyCapacity::Items;
    let mut capacities = vec![
        (CacheKind::ColumnArray, Bytes(column_array)),
        (CacheKind::ColumnData, Bytes(column_data)),
    ];
    if config.enable_table_meta_cache {
        capacities.extend([
            (
                CacheKind::TableSnapshot,
                Items(config.table_meta_snapshot_count as usize),
            ),
            (
                CacheKind::TableSnapshotStats,
                Items(config.table_meta_statistic_count as usize),
            ),
            (
                CacheKind::SegmentInfo,
                Bytes(config.table_meta_segment_bytes as usize),
            ),
            (
                CacheKind::ColumnOrientedSegment,
                Bytes(config.table_meta_segment_bytes as usize),
            ),
            (
                CacheKind::SegmentBlockMetas,
                Items(config.segment_block_metas_count as usize),
            ),
            (
                CacheKind::SegmentStats,
                Bytes(config.segment_statistics_bytes as usize),
            ),
            (
                CacheKind::BloomIndexMeta,
                Items(config.table_bloom_index_meta_count as usize),
            ),
            (
                CacheKind::InvertedIndexMeta,
                Items(config.inverted_index_meta_count as usize),
            ),
            (
                CacheKind::VectorIndexMeta,
                Items(config.vector_index_meta_count as usize),
            ),
            (
                CacheKind::SpatialIndexMeta,
                Items(config.spatial_index_meta_count as usize),
            ),
            (
                CacheKind::VirtualColumnMeta,
                Items(config.virtual_column_meta_count as usize),
            ),
            (CacheKind::ParquetMeta, Items(PARQUET_META_ITEMS)),
            (
                CacheKind::IcebergTable,
                Items(config.iceberg_table_meta_count as usize),
            ),
            (
                CacheKind::PruneResult,
                Items(config.table_prune_partitions_count as usize),
            ),
            (
                CacheKind::BloomFilter,
                Bytes(config.table_bloom_index_filter_size as usize),
            ),
            (
                CacheKind::VectorIndex,
                Bytes(ratio_or(
                    config.vector_index_filter_memory_ratio,
                    config.vector_index_filter_size,
                )),
            ),
            (
                CacheKind::VectorRefine,
                Bytes(config.vector_index_refine_size as usize),
            ),
            (
                CacheKind::InvertedIndexLookup,
                Bytes(config.inverted_index_lookup_size as usize),
            ),
            (CacheKind::InvertedIndexPayload, Bytes(payload)),
            (CacheKind::InvertedIndexFullPayload, Bytes(full_payload)),
            (
                CacheKind::SpatialIndex,
                Bytes(ratio_or(
                    config.spatial_index_filter_memory_ratio,
                    config.spatial_index_filter_size,
                )),
            ),
        ]);
    }
    capacities
        .into_iter()
        .map(|(kind, capacity)| match capacity {
            Bytes(0) | Items(0) => (kind, LegacyCapacity::Disabled),
            _ => (kind, capacity),
        })
        .collect()
}

/// Unit of a kind's pre-refactor capacity, also used when the kind starts disabled.
pub(crate) const fn legacy_unit(kind: CacheKind) -> Unit {
    use CacheKind::*;
    match kind {
        TableSnapshot | TableSnapshotStats | SegmentBlockMetas | BloomIndexMeta
        | InvertedIndexMeta | VectorIndexMeta | SpatialIndexMeta | VirtualColumnMeta
        | ParquetMeta | IcebergTable | PruneResult => Unit::Count,
        _ => Unit::Bytes,
    }
}

/// Total bytes of the byte-bounded legacy capacities, for the startup warning.
pub fn legacy_total_bytes(capacities: &[(CacheKind, LegacyCapacity)]) -> u128 {
    capacities
        .iter()
        .map(|(_, capacity)| match capacity {
            LegacyCapacity::Bytes(n) => *n as u128,
            _ => 0,
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity_of(capacities: &[(CacheKind, LegacyCapacity)], kind: CacheKind) -> LegacyCapacity {
        capacities
            .iter()
            .find(|(k, _)| *k == kind)
            .map_or(LegacyCapacity::Disabled, |(_, c)| *c)
    }

    #[test]
    fn test_defaults_match_current_cache_manager() {
        let config = CacheConfig::default();
        let caps = legacy_capacities(&config, 1 << 30);
        assert_eq!(
            capacity_of(&caps, CacheKind::TableSnapshot),
            LegacyCapacity::Items(config.table_meta_snapshot_count as usize)
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::SegmentBlockMetas),
            LegacyCapacity::Disabled
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::ColumnData),
            LegacyCapacity::Disabled
        );
        let (payload, full) =
            inverted_index_payload_capacities(config.inverted_index_payload_size as usize);
        assert_eq!(
            capacity_of(&caps, CacheKind::InvertedIndexPayload),
            LegacyCapacity::Bytes(payload)
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::InvertedIndexFullPayload),
            LegacyCapacity::Bytes(full)
        );
    }

    #[test]
    fn test_legacy_unit_matches_capacities() {
        let caps = legacy_capacities(&CacheConfig::default(), 1 << 30);
        for (kind, capacity) in caps {
            match capacity {
                LegacyCapacity::Items(_) => assert_eq!(legacy_unit(kind), Unit::Count, "{kind:?}"),
                LegacyCapacity::Bytes(_) => assert_eq!(legacy_unit(kind), Unit::Bytes, "{kind:?}"),
                LegacyCapacity::Disabled => {}
            }
        }
    }

    #[test]
    fn test_memory_ratio_overrides_bytes() {
        let config = CacheConfig {
            vector_index_filter_memory_ratio: 10,
            table_data_deserialized_data_bytes: 0,
            table_data_deserialized_memory_ratio: 5,
            ..CacheConfig::default()
        };
        let caps = legacy_capacities(&config, 1000);
        assert_eq!(
            capacity_of(&caps, CacheKind::VectorIndex),
            LegacyCapacity::Bytes(100)
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::ColumnArray),
            LegacyCapacity::Bytes(50)
        );
    }

    #[test]
    fn test_meta_cache_switch_disables_meta_and_index() {
        let config = CacheConfig {
            enable_table_meta_cache: false,
            table_data_deserialized_data_bytes: 100,
            ..CacheConfig::default()
        };
        let caps = legacy_capacities(&config, 1 << 30);
        assert_eq!(
            capacity_of(&caps, CacheKind::TableSnapshot),
            LegacyCapacity::Disabled
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::BloomFilter),
            LegacyCapacity::Disabled
        );
        assert_eq!(
            capacity_of(&caps, CacheKind::ColumnArray),
            LegacyCapacity::Bytes(100)
        );
    }
}
