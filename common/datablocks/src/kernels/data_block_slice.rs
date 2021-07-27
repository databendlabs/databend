// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::util::bit_util::ceil;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn split_block_by_size(block: &DataBlock, max_block_size: usize) -> Result<Vec<DataBlock>> {
        let size = block.num_rows();
        let mut blocks = Vec::with_capacity(ceil(size, max_block_size));

        let mut offset = 0;
        while offset < size {
            let remain = size - offset;
            let length = if remain >= max_block_size {
                max_block_size
            } else {
                remain
            };
            blocks.push(DataBlock::slice_block(block, offset, length));
            offset += length;
        }

        Ok(blocks)
    }

    #[inline]
    pub fn slice_block(block: &DataBlock, offset: usize, length: usize) -> DataBlock {
        let mut columns = Vec::with_capacity(block.num_columns());
        for column_index in 0..block.num_columns() {
            let column = block.column(column_index);
            columns.push(column.slice(offset, length));
        }
        DataBlock::create(block.schema().clone(), columns)
    }
}
