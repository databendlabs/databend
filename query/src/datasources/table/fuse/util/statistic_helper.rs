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

use common_catalog::BlockLocation;
use common_catalog::BlockMeta;
use common_catalog::ColStats;
use common_catalog::ColumnId;
use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::DataType;
use common_exception::Result;

type BlockStats = HashMap<ColumnId, (DataType, ColStats)>;

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
        let col_stats = block_stats
            .iter()
            .map(|(idx, v)| (*idx, v.1.clone()))
            .collect::<HashMap<ColumnId, ColStats>>();
        self.blocks_stats.push(block_stats);
        self.last_block_col_stats = Some(col_stats);
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

pub fn column_stats_reduce(
    stats: Vec<HashMap<ColumnId, (DataType, ColStats)>>,
) -> Result<HashMap<ColumnId, ColStats>> {
    let len = stats.len();

    // transpose Vec<HashMap<_,(_,_)>> to HashMap<_, (_, Vec<_>)>
    let col_stat_list = stats.into_iter().fold(HashMap::new(), |acc, item| {
        item.into_iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, (DataType, Vec<ColStats>)>,
             (col_id, (data_type, stats))| {
                let entry = acc.entry(col_id);
                match entry {
                    Entry::Occupied(_) => {
                        entry.and_modify(|v| v.1.push(stats));
                    }
                    Entry::Vacant(_) => {
                        entry.or_insert((data_type, vec![stats]));
                    }
                }
                acc
            },
        )
    });

    col_stat_list.iter().try_fold(
        HashMap::with_capacity(len),
        |mut acc, (id, (data_type, stats))| {
            let mut min_stats = Vec::with_capacity(stats.len());
            let mut max_stats = Vec::with_capacity(stats.len());
            let mut null_count = 0;
            let mut row_count = 0;

            for col_stats in stats {
                // to be optimized, with DataType and the value of data, we may
                // able to compare the min/max here
                min_stats.push(col_stats.min.clone());
                max_stats.push(col_stats.max.clone());

                null_count += col_stats.null_count;
                row_count += col_stats.row_count;
            }

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
                row_count,
            });
            Ok(acc)
        },
    )
}

pub(super) fn block_stats(
    data_block: &DataBlock,
) -> Result<HashMap<ColumnId, (DataType, ColStats)>> {
    let row_count = data_block.num_rows();

    // NOTE:
    // column id is FAKED, this is OK as long as table schema is NOT changed (which is not realistic)
    // we should extend DataField with column_id ...
    (0..).into_iter().zip(data_block.columns().iter()).try_fold(
        HashMap::new(),
        |mut res, (idx, col)| {
            let data_type = col.data_type();
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
                row_count,
            };

            res.insert(idx, (data_type, col_stats));
            Ok(res)
        },
    )
}
