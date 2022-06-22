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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn except_blocks(first_block: &DataBlock, second_block: &DataBlock) -> Result<DataBlock> {
        if first_block.schema().ne(second_block.schema()) {
            return Err(ErrorCode::DataStructMissMatch("Schema not matched"));
        }
        let mut first_block_rows = Vec::with_capacity(first_block.num_rows());
        for idx in 0..first_block.num_rows() {
            first_block_rows.push(first_block.slice(idx, 1));
        }

        let mut second_block_rows = Vec::with_capacity(second_block.num_rows());
        for idx in 0..second_block.num_rows() {
            second_block_rows.push(second_block.slice(idx, 1));
        }
        first_block_rows.retain(|row| !second_block_rows.contains(row));
        if first_block_rows.is_empty() {
            return Ok(DataBlock::empty());
        }
        DataBlock::concat_blocks(&first_block_rows)
    }
}
