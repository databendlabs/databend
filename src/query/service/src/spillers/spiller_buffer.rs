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

// The spiller buffer will record each partition's unspilled data.
#[derive(Clone)]
pub struct SpillBuffer {
    partition_data: Vec<Vec<DataBlock>>,
    partition_size: Vec<usize>,
    partition_threshold: usize,
}

impl SpillBuffer {
    pub fn create(num_partitions: usize, buffer_threshold: usize) -> Self {
        let partition_threshold = buffer_threshold / num_partitions * 1000 * 1000;
        SpillBuffer {
            partition_data: vec![Vec::new(); num_partitions],
            partition_size: vec![0; num_partitions],
            partition_threshold,
        }
    }

    // Add a partition's unspilled data to the SpillBuffer.
    pub fn add_partition_data(&mut self, partition_id: usize, data_block: DataBlock) {
        let data_size = data_block.memory_size();
        self.partition_size[partition_id] += data_size;
        self.partition_data[partition_id].push(data_block);
    }

    // Check if the partition buffer is full, we will return the partition data.
    pub fn pick_data_to_spill(&mut self, partition_id: usize) -> Result<Option<DataBlock>> {
        if self.partition_size[partition_id] >= self.partition_threshold {
            let data_blocks = self.partition_data[partition_id].clone();
            self.partition_data[partition_id].clear();
            self.partition_size[partition_id] = 0;
            let data_block = DataBlock::concat(&data_blocks)?;
            Ok(Some(data_block))
        } else {
            Ok(None)
        }
    }

    // Get partition data by partition id.
    pub fn read_partition_data(&mut self, partition_id: u8, pick: bool) -> Option<Vec<DataBlock>> {
        if !self.partition_data[partition_id as usize].is_empty() {
            let data_blocks = if pick {
                let data_blocks = self.partition_data[partition_id as usize].clone();
                self.partition_data[partition_id as usize].clear();
                data_blocks
            } else {
                self.partition_data[partition_id as usize].clone()
            };
            Some(data_blocks)
        } else {
            None
        }
    }

    pub fn buffered_partitions(&self) -> Vec<u8> {
        let mut partition_ids = vec![];
        for (partition_id, data) in self.partition_data.iter().enumerate() {
            if !data.is_empty() {
                partition_ids.push(partition_id as u8);
            }
        }
        partition_ids
    }

    pub fn empty_partition(&self, partition_id: u8) -> bool {
        self.partition_data[partition_id as usize].is_empty()
    }
}
