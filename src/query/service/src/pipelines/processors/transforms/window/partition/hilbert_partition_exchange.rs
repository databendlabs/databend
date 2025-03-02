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

// Some variables and functions are named and designed with reference to ClickHouse.
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;

use crate::pipelines::processors::transforms::WindowPartitionMeta;

pub struct HilbertPartitionExchange {
    num_partitions: usize,
}

impl HilbertPartitionExchange {
    pub fn create(num_partitions: usize) -> Arc<HilbertPartitionExchange> {
        Arc::new(HilbertPartitionExchange { num_partitions })
    }
}

impl Exchange for HilbertPartitionExchange {
    const NAME: &'static str = "Hilbert";
    fn partition(&self, data_block: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        let mut data_block = data_block;
        let range_ids = data_block
            .get_last_column()
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap();

        // Scatter the data block to different partitions.
        let indices = range_ids
            .iter()
            .map(|&id| (id % self.num_partitions as u64) as u16)
            .collect::<Vec<_>>();
        data_block.pop_columns(1);
        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.num_partitions);
        // Partition the data blocks to different processors.
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, indices) in scatter_indices.iter().take(self.num_partitions).enumerate()
        {
            if indices.is_empty() {
                continue;
            }
            let block = data_block.take_with_optimize_size(indices)?;
            output_data_blocks[partition_id % n].push((partition_id, block));
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}
