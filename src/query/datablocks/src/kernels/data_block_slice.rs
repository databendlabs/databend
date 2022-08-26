// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_datavalues::prelude::ceil;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn split_block_by_size(block: &DataBlock, max_block_size: usize) -> Result<Vec<DataBlock>> {
        let size = block.num_rows();
        if max_block_size == 0 {
            return if size != 0 {
                Err(ErrorCode::LogicalError("illegal max_block_size."))
            } else {
                Ok(vec![])
            };
        }
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
