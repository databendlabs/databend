// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn concat_blocks(blocks: &[DataBlock]) -> Result<DataBlock> {
        if blocks.is_empty() {
            return Result::Err(ErrorCodes::EmptyData("Can't concat empty blocks"));
        }

        let first_block = &blocks[0];
        for block in blocks.iter() {
            if block.schema().ne(first_block.schema()) {
                return Result::Err(ErrorCodes::DataStructMissMatch("Schema not matched"));
            }
        }

        let mut arrays = Vec::with_capacity(first_block.num_columns());
        for (i, _f) in blocks[0].schema().fields().iter().enumerate() {
            let mut arr = Vec::with_capacity(blocks.len());
            for block in blocks.iter() {
                arr.push(block.column(i).to_array()?);
            }

            let arr: Vec<&dyn Array> = arr.iter().map(|c| c.as_ref()).collect();
            arrays.push(compute::concat(&arr)?);
        }

        Ok(DataBlock::create_by_array(
            first_block.schema().clone(),
            arrays
        ))
    }
}
