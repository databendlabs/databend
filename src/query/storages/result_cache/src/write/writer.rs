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
use common_io::prelude::serialize_into_buf;
use opendal::Operator;

use crate::common::gen_result_cache_location;

pub(super) struct ResultCacheWriter {
    operator: Operator,

    current_bytes: usize,
    max_bytes: usize,
    num_rows: usize,

    blocks: Vec<DataBlock>,
}

impl ResultCacheWriter {
    pub fn create(operator: Operator, max_bytes: usize) -> Self {
        ResultCacheWriter {
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
        // Merge all blocks
        let block = DataBlock::concat(&self.blocks)?;
        block.convert_to_full();
        let cols = block
            .columns()
            .iter()
            .map(|c| c.value.as_column().unwrap().clone())
            .collect::<Vec<_>>();

        let location = gen_result_cache_location();
        let object = self.operator.object(&location);
        let mut buf = Vec::with_capacity(self.current_bytes);
        serialize_into_buf(&mut buf, &cols)?;
        object.write(buf).await?;
        Ok(location)
    }

    pub fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}
