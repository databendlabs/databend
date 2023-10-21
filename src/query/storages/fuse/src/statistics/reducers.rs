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

use common_expression::BlockThresholds;
use common_expression::ColumnId;
use common_expression::Scalar;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::table_functions::cmp_with_null;

pub fn reduce_block_statistics<T: Borrow<StatisticsOfColumns>>(
    stats_of_columns: &[T],
) -> StatisticsOfColumns {
    // Combine statistics of a column into `Vec`, that is:
    // from : `&[HashMap<ColumnId, ColumnStatistics>]`
    // to   : `HashMap<ColumnId, Vec<&ColumnStatistics>)>`
    let col_to_stats_lit = stats_of_columns.iter().fold(HashMap::new(), |acc, item| {
        item.borrow().iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, Vec<&ColumnStatistics>>, (col_id, col_stats)| {
                acc.entry(*col_id).or_default().push(col_stats);
                acc
            },
        )
    });

    // Reduce the `Vec<&ColumnStatistics` into ColumnStatistics`, i.e.:
    // from : `HashMap<ColumnId, Vec<&ColumnStatistics>)>`
    // to   : `type BlockStatistics = HashMap<ColumnId, ColumnStatistics>`
    let len = col_to_stats_lit.len();
    col_to_stats_lit
        .iter()
        .fold(HashMap::with_capacity(len), |mut acc, (id, stats)| {
            let mut min_stats = Vec::with_capacity(stats.len());
            let mut max_stats = Vec::with_capacity(stats.len());
            let mut null_count = 0;
            let mut in_memory_size = 0;

            for col_stats in stats {
                min_stats.push(col_stats.min().clone());
                max_stats.push(col_stats.max().clone());

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

            acc.insert(
                *id,
                ColumnStatistics::new(min, max, null_count, in_memory_size, None),
            );
            acc
        })
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
        .min_by(|x, y| x.iter().cmp_by(y.iter(), cmp_with_null))
        .unwrap();
    let max = max_stats
        .into_iter()
        .max_by(|x, y| x.iter().cmp_by(y.iter(), cmp_with_null))
        .unwrap();
    let level = levels.into_iter().max().unwrap_or(0);

    Some(ClusterStatistics::new(
        cluster_key_id,
        min,
        max,
        level,
        None,
    ))
}

pub fn merge_statistics(
    l: &Statistics,
    r: &Statistics,
    default_cluster_key_id: Option<u32>,
) -> Statistics {
    let mut new = l.clone();
    merge_statistics_mut(&mut new, r, default_cluster_key_id);
    new
}

pub fn merge_statistics_mut(
    l: &mut Statistics,
    r: &Statistics,
    default_cluster_key_id: Option<u32>,
) {
    if l.row_count == 0 {
        l.col_stats = r.col_stats.clone();
        l.cluster_stats = r.cluster_stats.clone();
    } else {
        l.col_stats = reduce_block_statistics(&[&l.col_stats, &r.col_stats]);
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
        }
    }
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
    let mut perfect_block_count: u64 = 0;

    let len = block_metas.len();
    let mut col_stats = Vec::with_capacity(len);
    let mut cluster_stats = Vec::with_capacity(len);

    block_metas.iter().for_each(|b| {
        let b = b.borrow();
        row_count += b.row_count;
        block_count += 1;
        uncompressed_byte_size += b.block_size;
        compressed_byte_size += b.file_size;
        index_size += b.bloom_filter_index_size;
        if thresholds.check_large_enough(b.row_count as usize, b.block_size as usize)
            || b.cluster_stats.as_ref().is_some_and(|v| v.level != 0)
        {
            perfect_block_count += 1;
        }
        col_stats.push(&b.col_stats);
        cluster_stats.push(&b.cluster_stats);
    });

    let merged_col_stats = reduce_block_statistics(&col_stats);
    let merged_cluster_stats = reduce_cluster_statistics(&cluster_stats, default_cluster_key_id);

    Statistics {
        row_count,
        block_count,
        perfect_block_count,
        uncompressed_byte_size,
        compressed_byte_size,
        index_size,
        col_stats: merged_col_stats,
        cluster_stats: merged_cluster_stats,
    }
}
