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

use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::datasources::table::fuse::operations::AppendOperation;
use crate::datasources::table::fuse::util;
use crate::datasources::table::fuse::BlockLocation;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::ColStats;
use crate::datasources::table::fuse::ColumnId;
use crate::datasources::table::fuse::Stats;

// TODO move this to other crate
pub type BlockStats = HashMap<ColumnId, ColStats>;

#[derive(Default)]
pub struct StatisticsAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
    pub blocks_stats: Vec<BlockStats>,
    pub summary_row_count: u64,
    pub summary_block_count: u64,
    pub in_memory_size: u64,
    pub file_size: u64,
    last_block_rows: u64,
    last_block_size: u64,
    last_block_col_stats: Option<HashMap<ColumnId, ColStats>>,
}

impl StatisticsAccumulator {
    pub fn new() -> Self {
        Default::default()
    }
}

impl StatisticsAccumulator {
    pub fn acc(&mut self, block: &DataBlock) -> Result<()> {
        let row_count = block.num_rows() as u64;
        let block_in_memory_size = block.memory_size() as u64;

        self.summary_block_count += 1;
        self.summary_row_count += row_count;
        self.in_memory_size += block_in_memory_size;
        self.last_block_rows = block.num_rows() as u64;
        self.last_block_size = block.memory_size() as u64;
        let block_stats = block_stats(block)?;
        self.last_block_col_stats = Some(block_stats.clone());
        self.blocks_stats.push(block_stats);
        Ok(())
    }
}

#[derive(Default)]
pub struct BlockMetaAccumulator {
    pub blocks_metas: Vec<BlockMeta>,
}

impl BlockMetaAccumulator {
    pub fn new() -> Self {
        Default::default()
    }
}

impl BlockMetaAccumulator {
    pub fn acc(&mut self, file_size: u64, location: String, stats: &mut StatisticsAccumulator) {
        stats.file_size += file_size;
        let block_meta = BlockMeta {
            location: BlockLocation {
                location,
                meta_size: 0,
            },
            row_count: stats.last_block_rows,
            block_size: stats.last_block_size,
            col_stats: stats.last_block_col_stats.take().unwrap_or_default(),
        };
        self.blocks_metas.push(block_meta);
    }
}

pub(super) fn block_stats(data_block: &DataBlock) -> Result<BlockStats> {
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
            } as u64;

            let in_memory_size = col.get_array_memory_size() as u64;

            let col_stats = ColStats {
                min,
                max,
                null_count,
                in_memory_size,
            };

            Ok((idx, col_stats))
        })
        .collect()
}

pub fn column_stats_reduce_with_schema<T: Borrow<HashMap<ColumnId, ColStats>>>(
    stats: &[T],
    schema: &DataSchema,
) -> Result<HashMap<ColumnId, ColStats>> {
    let len = stats.len();

    // transpose Vec<HashMap<_,(_,_)>> to HashMap<_, (_, Vec<_>)>
    let col_stat_list = stats.iter().fold(HashMap::new(), |acc, item| {
        item.borrow().iter().fold(
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

            acc.insert(*id, ColStats {
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
        col_stats: util::column_stats_reduce_with_schema(&[&l.col_stats, &r.col_stats], schema)?,
    };
    Ok(s)
}

pub fn merge_appends(
    schema: &DataSchema,
    append_log_entries: Vec<AppendOperation>,
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
            acc.col_stats =
                util::column_stats_reduce_with_schema(&[&acc.col_stats, &stats.col_stats], schema)?;
            seg_acc.push(loc.clone());
            Ok::<_, ErrorCode>((acc, seg_acc))
        },
    )?;

    Ok((seg_locs, s))
}
