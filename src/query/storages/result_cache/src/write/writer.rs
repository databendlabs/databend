// Copyright 2023 Datafuse Labs.
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

use common_exception::Result;
use common_expression::DataBlock;
use opendal::Operator;
use uuid::Uuid;

use crate::common::write_blocks_to_buffer;

pub(super) struct ResultCacheWriter {
    operator: Operator,
    location: String,

    current_bytes: usize,
    max_bytes: usize,
    num_rows: usize,

    blocks: Vec<DataBlock>,
}

impl ResultCacheWriter {
    pub fn create(location: String, operator: Operator, max_bytes: usize) -> Self {
        ResultCacheWriter {
            location,
            operator,
            current_bytes: 0,
            max_bytes,
            num_rows: 0,
            blocks: vec![],
        }
    }

    pub fn append_block(&mut self, block: DataBlock) {
        self.current_bytes += block.memory_size();
        self.num_rows += block.num_rows();
        self.blocks.push(block);
    }

    pub fn over_limit(&self) -> bool {
        self.current_bytes > self.max_bytes
    }

    /// Write the result cache to the storage and return the location.
    pub async fn write_to_storage(&self) -> Result<String> {
        let mut buf = Vec::with_capacity(self.current_bytes);
        write_blocks_to_buffer(&self.blocks, &mut buf)?;

        let file_location = format!("{}/{}", self.location, Uuid::new_v4().as_simple());
        let object = self.operator.object(&file_location);

        object.write(buf).await?;
        Ok(file_location)
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}
