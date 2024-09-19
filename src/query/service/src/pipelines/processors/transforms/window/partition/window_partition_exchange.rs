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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::group_hash_columns_slice;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::Exchange;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;

use super::WindowPartitionMeta;

pub struct WindowPartitionExchange {
    hash_keys: Vec<usize>,
    num_partitions: usize,
}

impl WindowPartitionExchange {
    pub fn create(hash_keys: Vec<usize>, num_partitions: usize) -> Arc<WindowPartitionExchange> {
        Arc::new(WindowPartitionExchange {
            hash_keys,
            num_partitions,
        })
    }
}

impl Exchange for WindowPartitionExchange {
    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let num_rows = data_block.num_rows();

        // Extract the columns used for hash computation.
        let hash_cols = self
            .hash_keys
            .iter()
            .map(|&offset| {
                let entry = data_block.get_by_offset(offset);
                match &entry.value {
                    Value::Scalar(s) => {
                        ColumnBuilder::repeat(&s.as_ref(), num_rows, &entry.data_type).build()
                    }
                    Value::Column(c) => c.clone(),
                }
            })
            .collect::<Vec<_>>();

        // Compute the hash value for each row.
        let mut hashes = vec![0u64; num_rows];
        group_hash_columns_slice(&hash_cols, &mut hashes);

        // Scatter the data block to different partitions.
        let indices = hashes
            .iter()
            .map(|&hash| (hash % self.num_partitions as u64) as u8)
            .collect::<Vec<_>>();
        let scatter_blocks = DataBlock::scatter(&data_block, &indices, self.num_partitions)?;

        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, data_block) in scatter_blocks.into_iter().enumerate() {
            output_data_blocks[partition_id % n].push((partition_id, data_block));
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}
