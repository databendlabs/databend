// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataArrayScatter;
use common_datavalues::DataColumnarValue;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn scatter_block(
        block: &DataBlock,
        indices: &DataColumnarValue,
        scatter_size: usize
    ) -> Result<Vec<DataBlock>> {
        let columns_size = block.num_columns();
        let mut scattered_columns: Vec<Option<DataColumnarValue>> = vec![];

        scattered_columns.resize_with(scatter_size * columns_size, || None);

        for column_index in 0..columns_size {
            let column = block.column(column_index);
            let scattered_column = DataArrayScatter::scatter(&column, &indices, scatter_size)?;

            for scattered_index in 0..scattered_column.len() {
                scattered_columns[scattered_index * columns_size + column_index] =
                    Some(scattered_column[scattered_index].clone());
            }
        }

        let mut scattered_blocks = Vec::with_capacity(scatter_size);
        for index in 0..scatter_size {
            let begin_index = index * columns_size;
            let end_index = (index + 1) * columns_size;

            let mut block_columns = vec![];
            for scattered_column in &scattered_columns[begin_index..end_index] {
                match scattered_column {
                    None => {
                        return Err(ErrorCodes::LogicalError(
                            "Logical Error: scattered column is None."
                        ));
                    }
                    Some(scattered_column) => {
                        block_columns.push(scattered_column.clone());
                    }
                };
            }

            scattered_blocks.push(DataBlock::create(block.schema().clone(), block_columns));
        }

        Ok(scattered_blocks)
    }
}
