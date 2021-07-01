// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn scatter_block(
        block: &DataBlock,
        indices: &DataColumn,
        scatter_size: usize,
    ) -> Result<Vec<DataBlock>> {
        let array = indices.to_array()?;
        let array = array.u64()?;

        let columns_size = block.num_columns();
        let mut scattered_columns = Vec::with_capacity(scatter_size);

        for column_index in 0..columns_size {
            let mut indices = array.into_no_null_iter();

            let columns = unsafe {
                block
                    .column(column_index)
                    .scatter_unchecked(&mut indices, scatter_size)
            }?;
            scattered_columns.push(columns);
        }

        let mut scattered_blocks = Vec::with_capacity(scatter_size);
        for index in 0..scatter_size {
            let mut block_columns = vec![];

            for item in scattered_columns.iter() {
                block_columns.push(item[index].clone())
            }
            scattered_blocks.push(DataBlock::create(block.schema().clone(), block_columns));
        }

        Ok(scattered_blocks)
    }
}
