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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::PartitionStatistics;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
pub use databend_storages_common_table_meta::meta::reduce_cluster_statistics;
use databend_storages_common_table_meta::meta::validate_segment_partition_statistics;

const VIRTUAL_COLUMN_JSONB_TYPE: u8 = 0;

pub fn reduce_block_statistics<T: Borrow<StatisticsOfColumns>>(
    stats_of_columns: &[T],
) -> StatisticsOfColumns {
    // Combine statistics of a column into `Vec`, that is:
    // from : `&[HashMap<ColumnId, ColumnStatistics>]`
    // to   : `HashMap<ColumnId, Vec<&ColumnStatistics>
    let col_to_stats_lit = stats_of_columns.iter().fold(HashMap::new(), |acc, item| {
        item.borrow().iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, Vec<&ColumnStatistics>>, (col_id, col_stats)| {
                acc.entry(*col_id).or_default().push(col_stats);
                acc
            },
        )
    });

    // Reduce the `Vec<&ColumnStatistics>` into ColumnStatistics`, i.e.:
    // from : `HashMap<ColumnId, Vec<&ColumnStatistics>>`
    // to   : `type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>`
    let len = col_to_stats_lit.len();
    col_to_stats_lit
        .iter()
        .fold(HashMap::with_capacity(len), |mut acc, (id, stats)| {
            // A range is safe only when every input contributes compatible statistics for the
            // column. This also prevents a column dropped after an incompatible merge from being
            // recreated later using only newly appended blocks.
            if stats.len() == stats_of_columns.len()
                && let Some(col_stats) = reduce_column_statistics(stats)
            {
                acc.insert(*id, col_stats);
            }
            acc
        })
}

pub fn reduce_column_statistics<T: Borrow<ColumnStatistics>>(
    stats: &[T],
) -> Option<ColumnStatistics> {
    let stats = stats.iter().map(Borrow::borrow).collect::<Vec<_>>();
    ColumnStatistics::try_reduce(&stats)
}

// Generate virtual column statistics from virtual column meta.
// The virtual column must have same data type and is not Jsonb,
// because scalars with different types can not compare.
pub fn generate_virtual_column_statistics<T: Borrow<HashMap<ColumnId, VirtualColumnMeta>>>(
    stats_of_virtual_columns: &[T],
) -> StatisticsOfColumns {
    // Combine statistics of a column into `Vec`, that is:
    // from : `&[HashMap<ColumnId, VirtualColumnMeta>]`
    // to   : `HashMap<ColumnId, Vec<(data_type, &ColumnStatistics)>>`
    let col_to_stats_lit = stats_of_virtual_columns
        .iter()
        .fold(HashMap::new(), |acc, item| {
            item.borrow().iter().fold(
                acc,
                |mut acc: HashMap<ColumnId, Vec<(u8, ColumnStatistics)>>, (col_id, col_meta)| {
                    if let Some(col_stats) = &col_meta.column_stat {
                        acc.entry(*col_id)
                            .or_default()
                            .push((col_meta.data_type, col_stats.clone()));
                    }
                    acc
                },
            )
        });

    // Reduce the `Vec<(data_type, &ColumnStatistics)>` into ColumnStatistics`, i.e.:
    // from : `HashMap<ColumnId, Vec<(data_type, &ColumnStatistics)>>`
    // to   : `type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>`
    let len = col_to_stats_lit.len();
    col_to_stats_lit.iter().fold(
        HashMap::with_capacity(len),
        |mut acc, (id, types_and_stats)| {
            let data_type_set = types_and_stats
                .iter()
                .map(|(ty, _)| *ty)
                .collect::<HashSet<_>>();
            // only collect stats if all block has same type and the type is not Jsonb
            if types_and_stats.len() == stats_of_virtual_columns.len()
                && data_type_set.len() == 1
                && !data_type_set.contains(&VIRTUAL_COLUMN_JSONB_TYPE)
            {
                let stats = types_and_stats
                    .iter()
                    .map(|(_, stat)| stat.clone())
                    .collect::<Vec<_>>();
                if let Some(col_stats) = reduce_column_statistics(&stats) {
                    acc.insert(*id, col_stats);
                }
            }
            acc
        },
    )
}

// Reduce statistics from multiple virtual columns into a single summary statistic.
// When statistics is None, it indicates that this block did not generate any virtual columns.
// In this case, we do not generate a summary statistic, because missing statistics from
// some blocks would introduce errors into the summary statistic.
pub fn reduce_virtual_column_statistics<T: Borrow<Option<StatisticsOfColumns>>>(
    stats_of_columns: &[T],
) -> Option<StatisticsOfColumns> {
    for stat in stats_of_columns {
        if stat.borrow().is_none() {
            return None;
        }
    }

    let col_to_stats_lit = stats_of_columns.iter().fold(HashMap::new(), |acc, item| {
        item.borrow().as_ref().unwrap().iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, Vec<&ColumnStatistics>>, (col_id, col_stats)| {
                acc.entry(*col_id).or_default().push(col_stats);
                acc
            },
        )
    });

    let len = col_to_stats_lit.len();
    let reduced_stats_of_columns =
        col_to_stats_lit
            .iter()
            .fold(HashMap::with_capacity(len), |mut acc, (id, stats)| {
                if stats.len() == stats_of_columns.len()
                    && let Some(col_stats) = reduce_column_statistics(stats)
                {
                    acc.insert(*id, col_stats);
                }
                acc
            });
    Some(reduced_stats_of_columns)
}

pub fn reduce_spatial_statistics<T: Borrow<Option<StatisticsOfSpatialColumns>>>(
    stats_of_columns: &[T],
) -> Option<StatisticsOfSpatialColumns> {
    if stats_of_columns.is_empty() {
        return None;
    }
    for stat in stats_of_columns {
        if stat.borrow().is_none() {
            return None;
        }
    }

    let mut col_to_stats = HashMap::new();
    for stat in stats_of_columns {
        for (col_id, spatial_stat) in stat.borrow().as_ref().unwrap() {
            col_to_stats
                .entry(*col_id)
                .or_insert_with(Vec::new)
                .push(spatial_stat);
        }
    }

    let block_count = stats_of_columns.len();
    let mut merged = HashMap::with_capacity(col_to_stats.len());
    for (col_id, stats) in col_to_stats {
        if stats.len() != block_count {
            continue;
        }
        let first = stats[0];
        let mut min_x = first.min_x;
        let mut min_y = first.min_y;
        let mut max_x = first.max_x;
        let mut max_y = first.max_y;
        let srid = first.srid;
        let mut has_null = first.has_null;
        let mut has_empty_rect = first.has_empty_rect;
        let mut is_valid = first.is_valid;
        let mut srid_mixed = false;

        for stat in stats.iter().skip(1) {
            if stat.srid != srid {
                srid_mixed = true;
                break;
            }
            min_x = min_x.min(stat.min_x);
            min_y = min_y.min(stat.min_y);
            max_x = max_x.max(stat.max_x);
            max_y = max_y.max(stat.max_y);
            has_null |= stat.has_null;
            has_empty_rect |= stat.has_empty_rect;
            is_valid &= stat.is_valid;
        }

        if srid_mixed || !is_valid {
            continue;
        }

        merged.insert(col_id, SpatialStatistics {
            min_x,
            min_y,
            max_x,
            max_y,
            srid,
            has_null,
            has_empty_rect,
            is_valid,
        });
    }

    (!merged.is_empty()).then_some(merged)
}

fn merge_partition_statistics(
    left: Option<&PartitionStatistics>,
    right: Option<&PartitionStatistics>,
) -> Option<PartitionStatistics> {
    match (left, right) {
        (Some(left), Some(right)) if left == right => Some(left.clone()),
        _ => None,
    }
}

pub fn merge_statistics(
    mut l: Statistics,
    r: &Statistics,
    cluster_key_info: Option<&ClusterKeyInfo>,
) -> Statistics {
    merge_statistics_mut(&mut l, r, cluster_key_info);
    l
}

pub fn merge_statistics_mut(
    l: &mut Statistics,
    r: &Statistics,
    cluster_key_info: Option<&ClusterKeyInfo>,
) {
    l.additional_stats_meta = None;
    if l.row_count == 0 {
        l.col_stats = r.col_stats.clone();
        l.virtual_col_stats = r.virtual_col_stats.clone();
        l.spatial_stats = r.spatial_stats.clone();
        l.cluster_stats = r.cluster_stats.clone();
        l.partition_stats = r.partition_stats.clone();
    } else {
        l.col_stats = reduce_block_statistics(&[&l.col_stats, &r.col_stats]);
        l.virtual_col_stats =
            reduce_virtual_column_statistics(&[&l.virtual_col_stats, &r.virtual_col_stats]);
        l.spatial_stats = reduce_spatial_statistics(&[&l.spatial_stats, &r.spatial_stats]);
        l.cluster_stats =
            reduce_cluster_statistics(&[&l.cluster_stats, &r.cluster_stats], cluster_key_info);
        l.partition_stats =
            merge_partition_statistics(l.partition_stats.as_ref(), r.partition_stats.as_ref());
    }

    l.row_count += r.row_count;
    l.block_count += r.block_count;
    l.perfect_block_count += r.perfect_block_count;
    l.uncompressed_byte_size += r.uncompressed_byte_size;
    l.compressed_byte_size += r.compressed_byte_size;
    l.index_size += r.index_size;

    let bloom_index_size =
        l.bloom_index_size.unwrap_or_default() + r.bloom_index_size.unwrap_or_default();
    let ngram_index_size =
        l.ngram_index_size.unwrap_or_default() + r.ngram_index_size.unwrap_or_default();
    let inverted_index_size =
        l.inverted_index_size.unwrap_or_default() + r.inverted_index_size.unwrap_or_default();
    let vector_index_size =
        l.vector_index_size.unwrap_or_default() + r.vector_index_size.unwrap_or_default();
    let virtual_column_size =
        l.virtual_column_size.unwrap_or_default() + r.virtual_column_size.unwrap_or_default();

    l.bloom_index_size = Option::from(bloom_index_size).filter(|&x| x > 0);
    l.ngram_index_size = Option::from(ngram_index_size).filter(|&x| x > 0);
    l.inverted_index_size = Option::from(inverted_index_size).filter(|&x| x > 0);
    l.vector_index_size = Option::from(vector_index_size).filter(|&x| x > 0);
    l.virtual_column_size = Option::from(virtual_column_size).filter(|&x| x > 0);

    let virtual_block_count =
        l.virtual_block_count.unwrap_or_default() + r.virtual_block_count.unwrap_or_default();
    l.virtual_block_count = Option::from(virtual_block_count).filter(|&x| x > 0);
}

// Deduct statistics, only be used for calculate snapshot summary.
pub fn deduct_statistics(l: &Statistics, r: &Statistics) -> Statistics {
    let mut new = l.clone();
    deduct_statistics_mut(&mut new, r);
    new
}

// Deduct statistics, only be used for calculate snapshot summary.
pub fn deduct_statistics_mut(l: &mut Statistics, r: &Statistics) {
    // Exact partition identity cannot be reconstructed after subtraction.
    l.partition_stats = None;
    l.row_count -= r.row_count;
    l.block_count -= r.block_count;
    l.perfect_block_count -= r.perfect_block_count;
    l.uncompressed_byte_size -= r.uncompressed_byte_size;
    l.compressed_byte_size -= r.compressed_byte_size;
    l.index_size -= r.index_size;
    for (id, col_stats) in &mut l.col_stats {
        if let Some(r_col_stats) = r.col_stats.get(id) {
            // The MinMax of a column cannot be recalculated by the right statistics,
            // so we skip deduct the MinMax statistics here.
            col_stats.null_count -= r_col_stats.null_count;
            col_stats.in_memory_size -= r_col_stats.in_memory_size;
            col_stats.distinct_of_values =
                match (col_stats.distinct_of_values, r_col_stats.distinct_of_values) {
                    (Some(l), Some(r)) => l.checked_sub(r),
                    _ => None,
                };
        }
    }
    l.spatial_stats = None;

    let bloom_index_size =
        l.bloom_index_size.unwrap_or_default() - r.bloom_index_size.unwrap_or_default();
    let ngram_index_size =
        l.ngram_index_size.unwrap_or_default() - r.ngram_index_size.unwrap_or_default();
    let inverted_index_size =
        l.inverted_index_size.unwrap_or_default() - r.inverted_index_size.unwrap_or_default();
    let vector_index_size =
        l.vector_index_size.unwrap_or_default() - r.vector_index_size.unwrap_or_default();
    let spatial_index_size =
        l.spatial_index_size.unwrap_or_default() - r.spatial_index_size.unwrap_or_default();
    let virtual_column_size =
        l.virtual_column_size.unwrap_or_default() - r.virtual_column_size.unwrap_or_default();

    l.bloom_index_size = Option::from(bloom_index_size).filter(|&x| x > 0);
    l.ngram_index_size = Option::from(ngram_index_size).filter(|&x| x > 0);
    l.inverted_index_size = Option::from(inverted_index_size).filter(|&x| x > 0);
    l.vector_index_size = Option::from(vector_index_size).filter(|&x| x > 0);
    l.spatial_index_size = Option::from(spatial_index_size).filter(|&x| x > 0);
    l.virtual_column_size = Option::from(virtual_column_size).filter(|&x| x > 0);

    let virtual_block_count =
        l.virtual_block_count.unwrap_or_default() - r.virtual_block_count.unwrap_or_default();
    l.virtual_block_count = Option::from(virtual_block_count).filter(|&x| x > 0);
}

pub fn reduce_block_metas<T: Borrow<BlockMeta>>(
    block_metas: &[T],
    thresholds: BlockThresholds,
    cluster_key_info: Option<&ClusterKeyInfo>,
) -> Result<Statistics> {
    let mut row_count: u64 = 0;
    let mut block_count: u64 = 0;
    let mut uncompressed_byte_size: u64 = 0;
    let mut compressed_byte_size: u64 = 0;
    let mut index_size: u64 = 0;
    let mut bloom_index_size: u64 = 0;
    let mut ngram_index_size: u64 = 0;
    let mut inverted_index_size: u64 = 0;
    let mut vector_index_size: u64 = 0;
    let mut spatial_index_size: u64 = 0;
    let mut virtual_column_size: u64 = 0;
    let mut perfect_block_count: u64 = 0;
    let mut virtual_block_count: u64 = 0;

    let len = block_metas.len();
    let mut col_stats = Vec::with_capacity(len);
    let mut cluster_stats = Vec::with_capacity(len);
    let mut partition_stats = Vec::with_capacity(len);
    let mut virtual_col_stats = Vec::with_capacity(len);
    let mut spatial_col_stats = Vec::with_capacity(len);

    block_metas.iter().for_each(|b| {
        let b = b.borrow();
        row_count += b.row_count;
        block_count += 1;
        uncompressed_byte_size += b.block_size;
        compressed_byte_size += b.file_size;
        index_size += b.bloom_filter_index_size;
        bloom_index_size += b.bloom_filter_index_size;
        if let Some(size) = b.ngram_filter_index_size {
            // index_size don't need to add ngram_index_size,
            // because ngram_index is part of bloom_index.
            ngram_index_size += size;
        }
        if let Some(size) = b.inverted_index_size {
            index_size += size;
            inverted_index_size += size;
        }
        if let Some(size) = b.vector_index_size {
            index_size += size;
            vector_index_size += size;
        }
        if let Some(size) = b.spatial_index_size {
            index_size += size;
            spatial_index_size += size;
        }
        if let Some(virtual_block_meta) = &b.virtual_block_meta {
            index_size += virtual_block_meta.virtual_column_size;
            virtual_column_size += virtual_block_meta.virtual_column_size;
            virtual_block_count += 1;
            virtual_col_stats.push(&virtual_block_meta.virtual_column_metas);
        }
        if thresholds.check_perfect_block(
            b.row_count as usize,
            b.block_size as usize,
            b.file_size as usize,
        ) || b.cluster_stats.as_ref().is_some_and(|v| v.level != 0)
        {
            perfect_block_count += 1;
        }
        col_stats.push(&b.col_stats);
        cluster_stats.push(&b.cluster_stats);
        partition_stats.push(&b.partition_stats);
        spatial_col_stats.push(&b.spatial_stats);
    });

    let merged_col_stats = reduce_block_statistics(&col_stats);
    let merged_spatial_stats = reduce_spatial_statistics(&spatial_col_stats);
    let merged_cluster_stats = reduce_cluster_statistics(&cluster_stats, cluster_key_info);
    let merged_partition_stats = validate_segment_partition_statistics(
        partition_stats.into_iter().map(|stats| stats.as_ref()),
    )?;
    let merged_virtual_col_stats = if block_count > 0 && virtual_block_count == block_count {
        let virtual_col_stats = generate_virtual_column_statistics(&virtual_col_stats);
        Some(virtual_col_stats)
    } else {
        None
    };
    let merged_virtual_block_count = Option::from(virtual_block_count).filter(|&x| x > 0);

    let bloom_index_size = Option::from(bloom_index_size).filter(|&x| x > 0);
    let ngram_index_size = Option::from(ngram_index_size).filter(|&x| x > 0);
    let inverted_index_size = Option::from(inverted_index_size).filter(|&x| x > 0);
    let vector_index_size = Option::from(vector_index_size).filter(|&x| x > 0);
    let spatial_index_size = Option::from(spatial_index_size).filter(|&x| x > 0);
    let virtual_column_size = Option::from(virtual_column_size).filter(|&x| x > 0);

    Ok(Statistics {
        row_count,
        block_count,
        perfect_block_count,
        uncompressed_byte_size,
        compressed_byte_size,
        index_size,
        bloom_index_size,
        ngram_index_size,
        inverted_index_size,
        vector_index_size,
        spatial_index_size,
        virtual_column_size,
        col_stats: merged_col_stats,
        virtual_col_stats: merged_virtual_col_stats,
        spatial_stats: merged_spatial_stats,
        cluster_stats: merged_cluster_stats,
        partition_stats: merged_partition_stats,
        virtual_block_count: merged_virtual_block_count,
        additional_stats_meta: None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::reduce_block_statistics;
    use super::reduce_column_statistics;

    #[test]
    fn test_reduce_decimal_statistics_with_widened_precision() {
        let old_size = DecimalSize::new(10, 2).unwrap();
        let new_size = DecimalSize::new(15, 2).unwrap();
        let old_stats = ColumnStatistics::new(
            Scalar::Decimal(DecimalScalar::Decimal64(100, old_size)),
            Scalar::Decimal(DecimalScalar::Decimal64(200, old_size)),
            0,
            10,
            Some(2),
        );
        let new_stats = ColumnStatistics::new(
            Scalar::Decimal(DecimalScalar::Decimal64(700, new_size)),
            Scalar::Decimal(DecimalScalar::Decimal64(800, new_size)),
            0,
            10,
            Some(2),
        );

        let reduced = reduce_column_statistics(&[new_stats, old_stats]).unwrap();
        let view = reduced
            .try_view(&databend_common_expression::types::DataType::Decimal(
                new_size,
            ))
            .unwrap();

        assert_eq!(
            view.min(),
            &Scalar::Decimal(DecimalScalar::Decimal64(100, new_size))
        );
        assert_eq!(
            view.max(),
            &Scalar::Decimal(DecimalScalar::Decimal64(800, new_size))
        );
    }

    #[test]
    fn test_reduce_block_statistics_does_not_recreate_partial_column_stats() {
        let stats = ColumnStatistics::new(
            Scalar::Number(1_i64.into()),
            Scalar::Number(9_i64.into()),
            0,
            16,
            None,
        );
        let with_stats = HashMap::from([(1, stats)]);
        let without_stats = HashMap::new();

        assert!(reduce_block_statistics(&[with_stats, without_stats]).is_empty());
    }
}
