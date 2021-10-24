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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;

use crate::datasources::table::fuse::statistics::BlockStats;
use crate::datasources::table::fuse::ColStats;
use crate::datasources::table::fuse::ColumnId;
use crate::datasources::table::fuse::Stats;

pub fn block_stats(data_block: &DataBlock) -> common_exception::Result<BlockStats> {
    // NOTE:
    // column id is FAKED, this is OK as long as table schema is NOT changed (which is not realistic)
    // we should extend DataField with column_id ...
    (0..)
        .into_iter()
        .zip(data_block.columns().iter())
        .map(|(idx, col)| {
            let min = match col {
                DataColumn::Array(s) => s.min(),
                DataColumn::Constant(v, _) => Ok(v.clone()),
            }?;

            let max = match col {
                DataColumn::Array(s) => s.max(),
                DataColumn::Constant(v, _) => Ok(v.clone()),
            }?;

            let null_count = match col {
                DataColumn::Array(s) => s.null_count(),
                DataColumn::Constant(v, _) => {
                    if v.is_null() {
                        1
                    } else {
                        0
                    }
                }
            };

            let col_stats = ColStats {
                min,
                max,
                null_count,
            };

            Ok((idx, col_stats))
        })
        .collect()
}

pub fn column_stats_reduce_with_schema(
    stats: &[HashMap<ColumnId, ColStats>],
    schema: &DataSchema,
) -> common_exception::Result<HashMap<ColumnId, ColStats>> {
    let len = stats.len();

    // transpose Vec<HashMap<_,(_,_)>> to HashMap<_, (_, Vec<_>)>
    let col_stat_list = stats.iter().fold(HashMap::new(), |acc, item| {
        item.iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, Vec<&ColStats>>, (col_id, stats)| {
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

            for col_stats in stats {
                // to be optimized, with DataType and the value of data, we may
                // able to compare the min/max here
                min_stats.push(col_stats.min.clone());
                max_stats.push(col_stats.max.clone());

                null_count += col_stats.null_count;
            }

            // TODO panic
            let data_type = schema.field((*id) as usize).data_type();

            // TODO
            // for some data types, we shall balance the accuracy and the length
            // e.g. for a string col, which max value is "xxxxxxxxxxxx....", we record the max as something like "y"
            let min =
                common_datavalues::DataValue::try_into_data_array(min_stats.as_slice(), data_type)?
                    .min()?;

            let max =
                common_datavalues::DataValue::try_into_data_array(max_stats.as_slice(), data_type)?
                    .max()?;

            acc.insert(*id, ColStats {
                min,
                max,
                null_count,
            });
            Ok(acc)
        })
}

pub fn merge_stats(schema: &DataSchema, l: &Stats, r: &Stats) -> common_exception::Result<Stats> {
    let s = Stats {
        row_count: l.row_count + r.row_count,
        block_count: l.block_count + r.block_count,
        uncompressed_byte_size: l.uncompressed_byte_size + r.uncompressed_byte_size,
        compressed_byte_size: l.compressed_byte_size + r.compressed_byte_size,
        col_stats: column_stats_reduce_with_schema(
            &[l.col_stats.clone(), r.col_stats.clone()],
            schema,
        )?,
    };
    Ok(s)
}
