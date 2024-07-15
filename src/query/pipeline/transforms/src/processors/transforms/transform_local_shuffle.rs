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

use std::sync::Arc;
use std::vec;

use databend_common_exception::Result;
use databend_common_expression::group_hash_columns;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;

use super::Transformer;
use crate::processors::Transform;

pub struct TransformLocalShuffle {
    local_pos: usize,
    scatter_size: usize,
    hash_key: Vec<usize>,
}

impl TransformLocalShuffle {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        local_pos: usize,
        scatter_size: usize,
        hash_key: Vec<usize>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformLocalShuffle {
                local_pos,
                scatter_size,
                hash_key,
            },
        )))
    }
}

impl Transform for TransformLocalShuffle {
    const NAME: &'static str = "LocalShuffleTransform";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let mut hashes = vec![0u64; num_rows];
        let hash_cols = InputColumns::new_block_proxy(&self.hash_key, &block);
        group_hash_columns(hash_cols, &mut hashes);

        let indices = hashes
            .iter()
            .map(|&hash| (hash % self.scatter_size as u64) as u8)
            .collect::<Vec<_>>();

        let mut scatter_blocks = DataBlock::scatter(&block, &indices, self.scatter_size)?;

        let block = scatter_blocks
            .drain(self.local_pos..self.local_pos + 1)
            .next()
            .unwrap();

        Ok(block)
    }
}
