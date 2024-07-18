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
use databend_common_expression::group_hash_columns_slice;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;

use super::Transformer;
use crate::processors::Transform;

pub struct TransformLocalShuffle {
    local_pos: u64,
    scatter_size: u64,
    hash_key: Vec<usize>,
}

impl TransformLocalShuffle {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        local_pos: u64,
        scatter_size: u64,
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

        let hash_cols = self
            .hash_key
            .iter()
            .map(|&offset| {
                let entry = block.get_by_offset(offset);
                match &entry.value {
                    Value::Scalar(s) => {
                        ColumnBuilder::repeat(&s.as_ref(), num_rows, &entry.data_type).build()
                    }
                    Value::Column(c) => c.clone(),
                }
            })
            .collect::<Vec<_>>();

        let mut hashes = vec![0u64; num_rows];
        group_hash_columns_slice(&hash_cols, &mut hashes);

        let mut indices = Vec::with_capacity(num_rows);
        for (indice, hash) in hashes.iter().take(num_rows).enumerate() {
            if hash % self.scatter_size == self.local_pos {
                indices.push(indice as u32);
            }
        }

        let has_string_column = block
            .columns()
            .iter()
            .any(|col| col.data_type.is_string_column());
        let mut string_items_buf = if has_string_column {
            Some(vec![(0, 0); indices.len()])
        } else {
            None
        };

        DataBlock::take(&block, &indices, &mut string_items_buf)
    }
}
