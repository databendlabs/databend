// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn scatter_block(
        block: &DataBlock,
        indices: &DataColumn,
        scatter_size: usize,
    ) -> Result<Vec<DataBlock>> {
        let mut indices = indices.to_array()?.u32()?.into_no_null_iter();

        let columns_size = block.num_columns();
        let mut scattered_columns = Vec::with_capacity(columns_size * scatter_size);

        for column_index in 0..columns_size {
            let column = block.column(column_index).to_array();
            let columns = unsafe {
                block
                    .column(column_index)
                    .scatter_unchecked(&mut indices, scatter_size)
            }?;
            scattered_columns.extend_from_slice(&columns);
        }

        let mut scattered_blocks = Vec::with_capacity(scatter_size);
        for index in 0..scatter_size {
            let begin_index = index * columns_size;
            let end_index = (index + 1) * columns_size;

            let mut block_columns = vec![];
            for scattered_column in &scattered_columns[begin_index..end_index] {
                block_columns.push(scattered_column.clone());
            }
            scattered_blocks.push(DataBlock::create(block.schema().clone(), block_columns));
        }

        Ok(scattered_blocks)
    }
}
