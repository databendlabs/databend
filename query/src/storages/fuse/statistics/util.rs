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
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::storages::fuse::meta::ColumnId;
use crate::storages::fuse::meta::Stats;
use crate::storages::fuse::operations::AppendOperationLogEntry;
use crate::storages::index::BlockStatistics;
use crate::storages::index::ColumnStatistics;

pub fn reduce_block_stats<T: Borrow<BlockStatistics>>(
    stats: &[T],
    schema: &DataSchema,
) -> Result<BlockStatistics> {
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

            // TODO panic
            let data_type = schema.field((*id) as usize).data_type();

            // TODO
            // for some data types, we shall balance the accuracy and the length
            // e.g. for a string col, which max value is "abcdef....", we record the max as something like "b"
            let min =
                common_datavalues::DataValue::try_into_data_array(min_stats.as_slice(), data_type)?
                    .min()?;

            let max =
                common_datavalues::DataValue::try_into_data_array(max_stats.as_slice(), data_type)?
                    .max()?;

            acc.insert(*id, ColumnStatistics {
                min,
                max,
                null_count,
                in_memory_size,
            });
            Ok(acc)
        })
}

pub fn merge_stats(schema: &DataSchema, l: &Stats, r: &Stats) -> Result<Stats> {
    let s = Stats {
        row_count: l.row_count + r.row_count,
        block_count: l.block_count + r.block_count,
        uncompressed_byte_size: l.uncompressed_byte_size + r.uncompressed_byte_size,
        compressed_byte_size: l.compressed_byte_size + r.compressed_byte_size,
        col_stats: reduce_block_stats(&[&l.col_stats, &r.col_stats], schema)?,
    };
    Ok(s)
}

pub fn merge_append_operations(
    schema: &DataSchema,
    append_log_entries: Vec<AppendOperationLogEntry>,
) -> Result<(Vec<String>, Stats)> {
    let (s, seg_locs) = append_log_entries.iter().try_fold(
        (
            Stats::default(),
            Vec::with_capacity(append_log_entries.len()),
        ),
        |(mut acc, mut seg_acc), log_entry| {
            let loc = &log_entry.segment_location;
            let stats = &log_entry.segment_info.summary;
            acc.row_count += stats.row_count;
            acc.block_count += stats.block_count;
            acc.uncompressed_byte_size += stats.uncompressed_byte_size;
            acc.compressed_byte_size += stats.compressed_byte_size;
            acc.col_stats = reduce_block_stats(&[&acc.col_stats, &stats.col_stats], schema)?;
            seg_acc.push(loc.clone());
            Ok::<_, ErrorCode>((acc, seg_acc))
        },
    )?;

    Ok((seg_locs, s))
}
