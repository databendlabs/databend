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

use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;

use crate::kinds::bloom;
use crate::kinds::column;
use crate::kinds::iceberg;
use crate::kinds::inverted;
use crate::kinds::parquet;
use crate::kinds::segment;
use crate::kinds::spatial;
use crate::kinds::table;
use crate::kinds::vector;
use crate::kinds::virtual_column;
use crate::legacy::LegacyKind;
use crate::legacy::LegacyKinds;

/// Parquet footers were always bounded by count, with no option.
const PARQUET_META_ITEMS: u64 = 3000;

impl LegacyKinds {
    /// The pre-refactor caches, each sized by its pre-refactor options.
    pub fn builtin() -> LegacyKinds {
        let mut kinds = LegacyKinds::default();
        kinds
            .register::<table::Snapshot>(LegacyKind::items("table_meta_snapshot_count", |c, _| {
                c.table_meta_snapshot_count
            }))
            .register::<table::SnapshotStats>(LegacyKind::items(
                "table_meta_statistic_count",
                |c, _| c.table_meta_statistic_count,
            ))
            .register::<segment::Info>(LegacyKind::bytes("table_meta_segment_bytes", |c, _| {
                c.table_meta_segment_bytes
            }))
            .register::<segment::ColumnOriented>(LegacyKind::bytes(
                "table_meta_segment_bytes",
                |c, _| c.table_meta_segment_bytes,
            ))
            .register::<segment::BlockMetas>(LegacyKind::items(
                "segment_block_metas_count",
                |c, _| c.segment_block_metas_count,
            ))
            .register::<segment::Stats>(LegacyKind::bytes("segment_statistics_bytes", |c, _| {
                c.segment_statistics_bytes
            }))
            .register::<bloom::IndexMeta>(LegacyKind::items(
                "table_bloom_index_meta_count",
                |c, _| c.table_bloom_index_meta_count,
            ))
            .register::<inverted::Meta>(LegacyKind::items("inverted_index_meta_count", |c, _| {
                c.inverted_index_meta_count
            }))
            .register::<vector::Meta>(LegacyKind::items("vector_index_meta_count", |c, _| {
                c.vector_index_meta_count
            }))
            .register::<spatial::Meta>(LegacyKind::items("spatial_index_meta_count", |c, _| {
                c.spatial_index_meta_count
            }))
            .register::<virtual_column::Meta>(LegacyKind::items(
                "virtual_column_meta_count",
                |c, _| c.virtual_column_meta_count,
            ))
            .register::<parquet::Meta>(LegacyKind::items("", |_, _| PARQUET_META_ITEMS))
            .register::<iceberg::Table>(LegacyKind::items("iceberg_table_meta_count", |c, _| {
                c.iceberg_table_meta_count
            }))
            .register::<table::PruneResult>(LegacyKind::items(
                "table_prune_partitions_count",
                |c, _| c.table_prune_partitions_count,
            ))
            .register::<bloom::Filter>(LegacyKind::bytes(
                "table_bloom_index_filter_size",
                |c, _| c.table_bloom_index_filter_size,
            ))
            .register::<vector::Index>(
                LegacyKind::bytes(
                    "vector_index_filter_size, vector_index_filter_memory_ratio",
                    |c, memory| {
                        ratio_or(
                            memory,
                            c.vector_index_filter_memory_ratio,
                            c.vector_index_filter_size,
                        )
                    },
                )
                .counting_held_values(),
            )
            .register::<vector::Refine>(LegacyKind::bytes("vector_index_refine_size", |c, _| {
                c.vector_index_refine_size
            }))
            .register::<inverted::Lookup>(LegacyKind::bytes(
                "inverted_index_lookup_size",
                |c, _| c.inverted_index_lookup_size,
            ))
            .register::<inverted::Payload>(
                LegacyKind::bytes("inverted_index_payload_size", |c, _| {
                    LegacyKind::split_budget(c.inverted_index_payload_size as usize).0 as u64
                })
                .sharing_budget_with::<inverted::FullPayload>(),
            )
            .register::<inverted::FullPayload>(LegacyKind::bytes(
                "inverted_index_payload_size",
                |c, _| LegacyKind::split_budget(c.inverted_index_payload_size as usize).1 as u64,
            ))
            .register::<spatial::Index>(LegacyKind::bytes(
                "spatial_index_filter_size, spatial_index_filter_memory_ratio",
                |c, memory| {
                    ratio_or(
                        memory,
                        c.spatial_index_filter_memory_ratio,
                        c.spatial_index_filter_size,
                    )
                },
            ))
            .register::<column::Data>(LegacyKind::bytes(
                "data_cache_in_memory_bytes",
                |c, _| match c.data_cache_storage {
                    CacheStorageTypeInnerConfig::Disk => c.data_cache_in_memory_bytes,
                    CacheStorageTypeInnerConfig::None => 0,
                },
            ))
            .register::<column::Array>(LegacyKind::bytes(
                "table_data_deserialized_data_bytes, table_data_deserialized_memory_ratio",
                column_array_bytes,
            ));
        kinds
    }
}

/// `ratio` percent of the memory limit if set, else `bytes`.
fn ratio_or(max_server_memory_usage: u64, ratio: u64, bytes: u64) -> u64 {
    match ratio {
        0 => bytes,
        _ => (max_server_memory_usage as u128 * ratio as u128 / 100) as u64,
    }
}

/// Bytes if set, else the memory ratio.
fn column_array_bytes(config: &CacheConfig, max_server_memory_usage: u64) -> u64 {
    match config.table_data_deserialized_data_bytes {
        0 => ratio_or(
            max_server_memory_usage,
            config.table_data_deserialized_memory_ratio,
            0,
        ),
        bytes => bytes,
    }
}
