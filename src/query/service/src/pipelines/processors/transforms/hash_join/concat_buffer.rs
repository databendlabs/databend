// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

/// Buffer for concatenating input data blocks, to improve cache locality.
pub struct ConcatBuffer {
    buffer: Vec<DataBlock>,
    num_rows: usize,
    concat_threshold: usize,
}

impl ConcatBuffer {
    pub fn new(concat_threshold: usize) -> Self {
        Self {
            buffer: Vec::new(),
            num_rows: 0,
            concat_threshold,
        }
    }

    pub fn add_block(&mut self, data_block: DataBlock) -> Result<Option<DataBlock>> {
        self.num_rows += data_block.num_rows();
        self.buffer.push(data_block);
        if self.num_rows < self.concat_threshold {
            Ok(None)
        } else {
            self.concat().map(Some)
        }
    }

    pub fn take_remaining(&mut self) -> Result<Option<DataBlock>> {
        if self.num_rows == 0 {
            Ok(None)
        } else {
            self.concat().map(Some)
        }
    }

    fn concat(&mut self) -> Result<DataBlock> {
        let buffer = std::mem::take(&mut self.buffer);
        let data_block = DataBlock::concat(&buffer)?;
        self.num_rows = 0;
        Ok(data_block)
    }
}
