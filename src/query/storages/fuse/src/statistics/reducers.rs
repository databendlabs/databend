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

use databend_common_expression::types::DataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VirtualColumnMeta;

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
            let col_stats = reduce_column_statistics(stats);
            acc.insert(*id, col_stats);
            acc
        })
}

pub fn reduce_column_statistics<T: Borrow<ColumnStatistics>>(stats: &[T]) -> ColumnStatistics {
    let mut min_stats = Vec::with_capacity(stats.len());
    let mut max_stats = Vec::with_capacity(stats.len());
    let mut ndvs = Vec::with_capacity(stats.len());
    let mut null_count = 0;
    let mut in_memory_size = 0;

    for col_stats in stats.iter() {
        let col_stats = col_stats.borrow();
        min_stats.push(col_stats.min().clone());
        max_stats.push(col_stats.max().clone());
        ndvs.push(col_stats.distinct_of_values);
        null_count += col_stats.null_count;
        in_memory_size += col_stats.in_memory_size;
    }

    let min = min_stats
        .into_iter()
        .filter(|s| !s.is_null())
        .min_by(|x, y| x.cmp(y))
        .unwrap_or(Scalar::Null);

    let max = max_stats
        .into_iter()
        .filter(|s| !s.is_null())
        .max_by(|x, y| x.cmp(y))
        .unwrap_or(Scalar::Null);
    let distinct_of_values = ndvs
        .into_iter()
        .try_fold(0, |acc, ndv| ndv.map(|v| acc + v));
    ColumnStatistics::new(min, max, null_count, in_memory_size, distinct_of_values)
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
                acc.insert(*id, col_stats);
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

                if type_set.len() <= 1 {
                    let col_stats = reduce_column_statistics(stats);
                    acc.insert(*id, col_stats);
                }
                acc
            });
    Some(reduced_stats_of_columns)
}

pub fn reduce_cluster_statistics<T: Borrow<Option<ClusterStatistics>>>(
    blocks_cluster_stats: &[T],
    default_cluster_key_id: Option<u32>,
) -> Option<ClusterStatistics> {
    if blocks_cluster_stats.is_empty() || default_cluster_key_id.is_none() {
        return None;
    }

    let cluster_key_id = default_cluster_key_id.unwrap();
    let len = blocks_cluster_stats.len();
    let mut min_stats = Vec::with_capacity(len);
    let mut max_stats = Vec::with_capacity(len);
    let mut levels = Vec::with_capacity(len);

    for cluster_stats in blocks_cluster_stats.iter() {
        if let Some(stat) = cluster_stats.borrow() {
            if stat.cluster_key_id != cluster_key_id {
                return None;
            }

            min_stats.push(stat.min());
            max_stats.push(stat.max());
            levels.push(stat.level);
        } else {
            return None;
        }
    }

    let min = min_stats
        .into_iter()
        .min_by(|x, y| {
            x.iter()
                .map(Scalar::as_ref)
                .cmp(y.iter().map(Scalar::as_ref))
        })
        .unwrap();
    let max = max_stats
        .into_iter()
        .max_by(|x, y| {
            x.iter()
                .map(Scalar::as_ref)
                .cmp(y.iter().map(Scalar::as_ref))
        })
        .unwrap();
    let level = levels.into_iter().max().unwrap_or(0);

    Some(ClusterStatistics::new(
        cluster_key_id,
        min.clone(),
        max.clone(),
        level,
        None,
    ))
}

pub fn merge_statistics(
    mut l: Statistics,
    r: &Statistics,
    default_cluster_key_id: Option<u32>,
) -> Statistics {
    merge_statistics_mut(&mut l, r, default_cluster_key_id);
    l
}

pub fn merge_statistics_mut(
    l: &mut Statistics,
    r: &Statistics,
    default_cluster_key_id: Option<u32>,
) {
    l.additional_stats_meta = None;
    if l.row_count == 0 {
        l.col_stats = r.col_stats.clone();
        l.virtual_col_stats = r.virtual_col_stats.clone();
        l.cluster_stats = r.cluster_stats.clone();
    } else {
        l.col_stats = reduce_block_statistics(&[&l.col_stats, &r.col_stats]);
        l.virtual_col_stats =
            reduce_virtual_column_statistics(&[&l.virtual_col_stats, &r.virtual_col_stats]);
        l.cluster_stats = reduce_cluster_statistics(
            &[&l.cluster_stats, &r.cluster_stats],
            default_cluster_key_id,
        );
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

    let bloom_index_size =
        l.bloom_index_size.unwrap_or_default() - r.bloom_index_size.unwrap_or_default();
    let ngram_index_size =
        l.ngram_index_size.unwrap_or_default() - r.ngram_index_size.unwrap_or_default();
    let inverted_index_size =
        l.inverted_index_size.unwrap_or_default() - r.inverted_index_size.unwrap_or_default();
    let vector_index_size =
        l.vector_index_size.unwrap_or_default() - r.vector_index_size.unwrap_or_default();
    let virtual_column_size =
        l.virtual_column_size.unwrap_or_default() - r.virtual_column_size.unwrap_or_default();

    l.bloom_index_size = Option::from(bloom_index_size).filter(|&x| x > 0);
    l.ngram_index_size = Option::from(ngram_index_size).filter(|&x| x > 0);
    l.inverted_index_size = Option::from(inverted_index_size).filter(|&x| x > 0);
    l.vector_index_size = Option::from(vector_index_size).filter(|&x| x > 0);
    l.virtual_column_size = Option::from(virtual_column_size).filter(|&x| x > 0);

    let virtual_block_count =
        l.virtual_block_count.unwrap_or_default() - r.virtual_block_count.unwrap_or_default();
    l.virtual_block_count = Option::from(virtual_block_count).filter(|&x| x > 0);
}

pub fn reduce_block_metas<T: Borrow<BlockMeta>>(
    block_metas: &[T],
    thresholds: BlockThresholds,
    default_cluster_key_id: Option<u32>,
) -> Statistics {
    let mut row_count: u64 = 0;
    let mut block_count: u64 = 0;
    let mut uncompressed_byte_size: u64 = 0;
    let mut compressed_byte_size: u64 = 0;
    let mut index_size: u64 = 0;
    let mut bloom_index_size: u64 = 0;
    let mut ngram_index_size: u64 = 0;
    let mut inverted_index_size: u64 = 0;
    let mut vector_index_size: u64 = 0;
    let mut virtual_column_size: u64 = 0;
    let mut perfect_block_count: u64 = 0;
    let mut virtual_block_count: u64 = 0;

    let len = block_metas.len();
    let mut col_stats = Vec::with_capacity(len);
    let mut cluster_stats = Vec::with_capacity(len);
    let mut virtual_col_stats = Vec::with_capacity(len);

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
    });

    let merged_col_stats = reduce_block_statistics(&col_stats);
    let merged_cluster_stats = reduce_cluster_statistics(&cluster_stats, default_cluster_key_id);
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
    let virtual_column_size = Option::from(virtual_column_size).filter(|&x| x > 0);

    Statistics {
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
        virtual_column_size,
        col_stats: merged_col_stats,
        virtual_col_stats: merged_virtual_col_stats,
        cluster_stats: merged_cluster_stats,
        virtual_block_count: merged_virtual_block_count,
        additional_stats_meta: None,
    }
}
