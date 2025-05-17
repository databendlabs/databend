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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Exchange;

use crate::pipelines::processors::transforms::WindowPartitionMeta;

pub struct HilbertPartitionExchange {
    start: u64,
    width: usize,
}

impl HilbertPartitionExchange {
    pub fn create(start: u64, width: usize) -> Arc<HilbertPartitionExchange> {
        Arc::new(HilbertPartitionExchange { start, width })
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
            .map(|&id| (id - self.start) as u16)
            .collect::<Vec<_>>();
        data_block.pop_columns(1);

        let scatter_indices = DataBlock::divide_indices_by_scatter_size(&indices, self.width);
        // Partition the data blocks to different processors.
        let base = self.width / n;
        let remainder = self.width % n;
        let mut output_data_blocks = vec![vec![]; n];
        for (partition_id, indices) in scatter_indices.into_iter().take(self.width).enumerate() {
            if !indices.is_empty() {
                let target = if partition_id < remainder * (base + 1) {
                    partition_id / (base + 1)
                } else {
                    (partition_id - remainder) / base
                };
                let block = data_block.take_with_optimize_size(&indices)?;
                output_data_blocks[target].push((partition_id, block));
            }
        }

        // Union data blocks for each processor.
        Ok(output_data_blocks
            .into_iter()
            .map(WindowPartitionMeta::create)
            .map(DataBlock::empty_with_meta)
            .collect())
    }
}
