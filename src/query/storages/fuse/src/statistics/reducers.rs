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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::PartitionStatistics;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;
use databend_storages_common_table_meta::meta::common_stat_decimal_size;
pub use databend_storages_common_table_meta::meta::reduce_cluster_statistics;
use databend_storages_common_table_meta::meta::retag_stat_scalar;
use databend_storages_common_table_meta::meta::try_cmp_stat_scalars;
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
            // Omit the column when its bounds cannot be reduced; a partial range would be worse
            // than none, since consumers treat a missing entry as "no information".
            if let Some(col_stats) = reduce_column_statistics(stats) {
                acc.insert(*id, col_stats);
            }
            acc
        })
}

/// Reduce per-block statistics for one column into a single range.
///
/// Returns `None` when the inputs carry no usable range, which happens when a column mixes
/// genuinely incomparable bounds. Callers must then omit the column rather than persist a range
/// that cannot be trusted: `ColumnStatistics::new` asserts that min and max share a type, and
/// picking each end independently from incomparable inputs can violate that.
///
/// Decimal bounds left tagged with an older precision by a metadata-only widening are aligned to
/// the widest precision before comparison, so they still reduce normally.
pub fn reduce_column_statistics<T: Borrow<ColumnStatistics>>(
    stats: &[T],
) -> Option<ColumnStatistics> {
    if stats.is_empty() {
        return None;
    }

    let mut null_count = 0;
    let mut in_memory_size = 0;
    let mut ndvs = Vec::with_capacity(stats.len());
    let mut bounds = Vec::with_capacity(stats.len() * 2);

    for col_stats in stats.iter() {
        let col_stats = col_stats.borrow();
        bounds.push(col_stats.min());
        bounds.push(col_stats.max());
        ndvs.push(col_stats.distinct_of_values);
        null_count += col_stats.null_count;
        in_memory_size += col_stats.in_memory_size;
    }

    // Align stale decimal precisions to the widest one seen, so the comparisons below are
    // meaningful and the resulting min/max agree on a single size.
    let target_size = common_stat_decimal_size(&bounds);
    let align = |scalar: &Scalar| -> Option<Scalar> {
        match target_size {
            Some(size) => retag_stat_scalar(scalar, size),
            None => Some(scalar.clone()),
        }
    };

    let mut min: Option<Scalar> = None;
    let mut max: Option<Scalar> = None;
    for col_stats in stats.iter() {
        let col_stats = col_stats.borrow();
        if !col_stats.min().is_null() {
            let candidate = align(col_stats.min())?;
            min = match min {
                None => Some(candidate),
                Some(current) => {
                    match try_cmp_stat_scalars(&candidate.as_ref(), &current.as_ref())? {
                        Ordering::Less => Some(candidate),
                        _ => Some(current),
                    }
                }
            };
        }
        if !col_stats.max().is_null() {
            let candidate = align(col_stats.max())?;
            max = match max {
                None => Some(candidate),
                Some(current) => {
                    match try_cmp_stat_scalars(&candidate.as_ref(), &current.as_ref())? {
                        Ordering::Greater => Some(candidate),
                        _ => Some(current),
                    }
                }
            };
        }
    }

    let distinct_of_values = ndvs
        .into_iter()
        .try_fold(0, |acc, ndv| ndv.map(|v| acc + v));
    Some(ColumnStatistics::new(
        min.unwrap_or(Scalar::Null),
        max.unwrap_or(Scalar::Null),
        null_count,
        in_memory_size,
        distinct_of_values,
    ))
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
            if data_type_set.len() == 1 && !data_type_set.contains(&VIRTUAL_COLUMN_JSONB_TYPE) {
                let stats = types_and_stats
                    .iter()
                    .map(|(_, stat)| stat.clone())
                    .collect::<Vec<_>>();
                let col_stats = reduce_column_statistics(&stats);
                if let Some(col_stats) = col_stats {
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
                // Check that all non-null min and max Scalars have the same type.
                let mut type_set = HashSet::new();
                for s in stats.iter() {
                    let min = s.min();
                    let min_type = min.as_ref().infer_data_type();
                    if !matches!(min_type, DataType::Null) {
                        type_set.insert(min_type);
                    }
                    let max = s.max();
                    let max_type = max.as_ref().infer_data_type();
                    if !matches!(max_type, DataType::Null) {
                        type_set.insert(max_type);
                    }
                    if type_set.len() > 1 {
                        break;
                    }
                }

                if type_set.len() <= 1
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
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;

    use super::*;

    fn size(precision: u8, scale: u8) -> DecimalSize {
        DecimalSize::new(precision, scale).unwrap()
    }

    fn decimal(value: i64, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal64(value, size(precision, scale)))
    }

    fn stats(min: Scalar, max: Scalar) -> ColumnStatistics {
        ColumnStatistics::new(min, max, 0, 16, None)
    }

    // A metadata-only decimal precision widening leaves older blocks tagged with the previous
    // `DecimalSize`. Reducing across the boundary must still find the true extremes.
    #[test]
    fn test_reduce_column_statistics_across_widened_precision() {
        let old = stats(decimal(100, 10, 2), decimal(200, 10, 2));
        let new = stats(decimal(500, 15, 2), decimal(800, 15, 2));

        let reduced = reduce_column_statistics(&[new, old]).expect("bounds are comparable");

        // Both ends must agree on one size, otherwise `ColumnStatistics::new` would have
        // asserted; the widest precision seen wins.
        assert_eq!(reduced.min(), &decimal(100, 15, 2));
        assert_eq!(reduced.max(), &decimal(800, 15, 2));
    }

    // Without alignment, `min_by`/`max_by` take each end from a different input and produce a
    // range whose min and max disagree on their size.
    #[test]
    fn test_reduce_column_statistics_keeps_bounds_on_one_size() {
        let old = stats(decimal(900, 10, 2), decimal(950, 10, 2));
        let new = stats(decimal(100, 15, 2), decimal(200, 15, 2));

        let reduced = reduce_column_statistics(&[old, new]).expect("bounds are comparable");

        let min_size = reduced.min().as_decimal().unwrap().size();
        let max_size = reduced.max().as_decimal().unwrap().size();
        assert_eq!(min_size, max_size);
        assert_eq!(reduced.min(), &decimal(100, 15, 2));
        assert_eq!(reduced.max(), &decimal(950, 15, 2));
    }

    // A scale change is not a metadata-only widening: the raw value denotes a different number,
    // so there is no sound way to reduce the bounds.
    #[test]
    fn test_reduce_column_statistics_rejects_incompatible_scale() {
        let a = stats(decimal(100, 10, 2), decimal(200, 10, 2));
        let b = stats(decimal(100, 10, 4), decimal(200, 10, 4));

        assert!(reduce_column_statistics(&[a, b]).is_none());
        assert!(reduce_column_statistics::<ColumnStatistics>(&[]).is_none());
    }

    // Columns whose bounds cannot be reduced are omitted rather than persisted with a range that
    // cannot be trusted.
    #[test]
    fn test_reduce_block_statistics_omits_incomparable_column() {
        let comparable = HashMap::from([(1u32, stats(decimal(100, 10, 2), decimal(200, 10, 2)))]);
        let incomparable = HashMap::from([
            (1u32, stats(decimal(300, 15, 2), decimal(400, 15, 2))),
            (2u32, stats(decimal(100, 10, 2), decimal(200, 10, 2))),
        ]);
        let scale_change = HashMap::from([(2u32, stats(decimal(100, 10, 4), decimal(200, 10, 4)))]);

        let reduced = reduce_block_statistics(&[comparable, incomparable, scale_change]);

        // Column 1 widened cleanly and survives; column 2 mixes scales and is dropped.
        assert_eq!(reduced.get(&1).map(|s| s.min()), Some(&decimal(100, 15, 2)));
        assert!(!reduced.contains_key(&2));
    }
}
