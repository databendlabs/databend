//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_datavalues::DataValue;
use common_exception::Result;

use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::Statistics;
use crate::storages::index::ClusterStatistics;
use crate::storages::index::ColumnStatistics;
use crate::storages::index::ColumnsStatistics;

pub fn reduce_block_stats<T: Borrow<ColumnsStatistics>>(stats: &[T]) -> Result<ColumnsStatistics> {
    let len = stats.len();

    // transpose Vec<HashMap<_,(_,_)>> to HashMap<_, (_, Vec<_>)>
    let col_stat_list = stats.iter().fold(HashMap::new(), |acc, item| {
        item.borrow().iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, Vec<&ColumnStatistics>>, (col_id, stats)| {
                let entry = acc.entry(*col_id);
                match entry {
                    Entry::Occupied(_) => {
                        entry.and_modify(|v| v.push(stats));
                    }
                    Entry::Vacant(_) => {
                        entry.or_insert_with(|| vec![stats]);
                    }
                }
                acc
            },
        )
    });

    col_stat_list
        .iter()
        .try_fold(HashMap::with_capacity(len), |mut acc, (id, stats)| {
            let mut min_stats = Vec::with_capacity(stats.len());
            let mut max_stats = Vec::with_capacity(stats.len());
            let mut null_count = 0;
            let mut in_memory_size = 0;

            for col_stats in stats {
                // to be optimized, with DataType and the value of data, we may
                // able to compare the min/max here
                min_stats.push(col_stats.min.clone());
                max_stats.push(col_stats.max.clone());

                null_count += col_stats.null_count;
                in_memory_size += col_stats.in_memory_size;
            }

            // TODO
            // for some data types, we shall balance the accuracy and the length
            // e.g. for a string col, which max value is "abcdef....", we record the max as something like "b"
            let min = min_stats
                .iter()
                .filter(|s| !s.is_null())
                .min_by(|&x, &y| x.partial_cmp(y).unwrap_or(Ordering::Equal))
                .cloned()
                .unwrap_or(DataValue::Null);

            let max = max_stats
                .iter()
                .filter(|s| !s.is_null())
                .max_by(|&x, &y| x.partial_cmp(y).unwrap_or(Ordering::Equal))
                .cloned()
                .unwrap_or(DataValue::Null);

            acc.insert(*id, ColumnStatistics {
                min,
                max,
                null_count,
                in_memory_size,
            });
            Ok(acc)
        })
}

pub fn reduce_cluster_stats<T: Borrow<Option<ClusterStatistics>>>(
    stats: &[T],
) -> Option<ClusterStatistics> {
    if stats.iter().any(|s| s.borrow().is_none()) {
        return None;
    }

    let stat = stats[0].borrow().clone().unwrap();
    let mut min = stat.min.clone();
    let mut max = stat.max;
    for stat in stats.iter().skip(1) {
        let stat = stat.borrow().clone().unwrap();
        for (l, r) in min.iter().zip(stat.min.iter()) {
            match (l.is_null(), r.is_null()) {
                (true, _) => {
                    min = stat.min.clone();
                    break;
                }
                (_, true) => break,
                _ => {
                    if let Some(cmp) = l.partial_cmp(r) {
                        match cmp {
                            Ordering::Equal => continue,
                            Ordering::Less => break,
                            Ordering::Greater => {
                                min = stat.min.clone();
                                break;
                            }
                        }
                    }
                }
            }
        }

        for (l, r) in max.iter().zip(stat.max.iter()) {
            match (l.is_null(), r.is_null()) {
                (true, _) => {
                    max = stat.max.clone();
                    break;
                }
                (_, true) => break,
                _ => {
                    if let Some(cmp) = l.partial_cmp(r) {
                        match cmp {
                            Ordering::Equal => continue,
                            Ordering::Less => {
                                max = stat.max.clone();
                                break;
                            }
                            Ordering::Greater => break,
                        }
                    }
                }
            }
        }
    }
    Some(ClusterStatistics { min, max })
}

pub fn merge_statistics(l: &Statistics, r: &Statistics) -> Result<Statistics> {
    let s = Statistics {
        row_count: l.row_count + r.row_count,
        block_count: l.block_count + r.block_count,
        uncompressed_byte_size: l.uncompressed_byte_size + r.uncompressed_byte_size,
        compressed_byte_size: l.compressed_byte_size + r.compressed_byte_size,
        col_stats: reduce_block_stats(&[&l.col_stats, &r.col_stats])?,
        cluster_stats: reduce_cluster_stats(&[&l.cluster_stats, &r.cluster_stats]),
    };
    Ok(s)
}
