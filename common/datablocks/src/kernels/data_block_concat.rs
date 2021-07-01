// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn concat_blocks(blocks: &[DataBlock]) -> Result<DataBlock> {
        if blocks.is_empty() {
            return Result::Err(ErrorCode::EmptyData("Can't concat empty blocks"));
        }

        let first_block = &blocks[0];
        for block in blocks.iter() {
            if block.schema().ne(first_block.schema()) {
                return Result::Err(ErrorCode::DataStructMissMatch("Schema not matched"));
            }
        }

        let mut concat_columns = Vec::with_capacity(first_block.num_columns());
        for (i, _f) in blocks[0].schema().fields().iter().enumerate() {
            let mut columns = Vec::with_capacity(blocks.len());
            for block in blocks.iter() {
                columns.push(block.column(i).clone());
            }

            concat_columns.push(DataColumnCommon::concat(&columns)?);
        }

        Ok(DataBlock::create(
            first_block.schema().clone(),
            concat_columns,
        ))
    }
}
