// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_datavalues::prelude::DataColumn;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::DataBlock;

// Table for <group_key, (indices, keys) >
pub type GroupIndicesTable = HashMap<Vec<u8>, (Vec<u32>, Vec<DataValue>), ahash::RandomState>;
// Table for <(group_key, keys, block)>
type GroupBlocksTable = Vec<(Vec<u8>, Vec<DataValue>, DataBlock)>;

impl DataBlock {
    /// Hash group based on row index then return indices and keys.
    /// For example:
    /// row_idx, A
    /// 0, 1
    /// 1, 2
    /// 2, 3
    /// 3, 4
    /// 4, 5
    ///
    /// grouping by [A%3]
    /// 1)
    /// row_idx, group_key, A
    /// 0, 1, 1
    /// 1, 2, 2
    /// 2, 0, 3
    /// 3, 1, 4
    /// 4, 2, 5
    ///
    /// 2) make indices group(for vector compute)
    /// group_key, indices
    /// 0, [2]
    /// 1, [0, 3]
    /// 2, [1, 4]
    ///
    pub fn group_by_get_indices(
        block: &DataBlock,
        column_names: &[String],
    ) -> Result<GroupIndicesTable> {
        let mut group_indices = GroupIndicesTable::default();

        // 1. Get group by columns.
        let mut group_columns = Vec::with_capacity(column_names.len());
        {
            for col in column_names {
                group_columns.push(block.try_column_by_name(col)?);
            }
        }

        // 2. Build serialized keys
        let mut group_keys = Vec::with_capacity(block.num_rows());
        {
            let mut group_key_len = 0;
            for col in &group_columns {
                let typ = col.data_type();
                if common_datavalues::is_integer(&typ) {
                    group_key_len += common_datavalues::numeric_byte_size(&typ)?;
                } else {
                    group_key_len += 4;
                }
            }

            for _i in 0..block.num_rows() {
                group_keys.push(Vec::with_capacity(group_key_len));
            }

            for col in &group_columns {
                DataColumn::serialize(col, &mut group_keys)?;
            }
        }

        // 2. Make group with indices.
        {
            for (row, group_key) in group_keys.iter().enumerate().take(block.num_rows()) {
                match group_indices.get_mut(group_key) {
                    None => {
                        let mut group_values = Vec::with_capacity(group_columns.len());
                        for col in &group_columns {
                            group_values.push(col.try_get(row)?);
                        }
                        group_indices.insert(group_key.clone(), (vec![row as u32], group_values));
                    }
                    Some((v, _)) => {
                        v.push(row as u32);
                    }
                }
            }
        }

        Ok(group_indices)
    }

    /// Hash group based on row index by column names.
    ///
    /// group_by_get_indices and make blocks.
    pub fn group_by(block: &DataBlock, column_names: &[String]) -> Result<GroupBlocksTable> {
        let group_indices = Self::group_by_get_indices(block, column_names)?;
        let mut group_blocks = GroupBlocksTable::default();
        for (group_key, (group_indices, group_keys)) in group_indices {
            let take_block = DataBlock::block_take_by_indices(block, &group_indices)?;
            group_blocks.push((group_key, group_keys, take_block));
        }

        Ok(group_blocks)
    }
}
