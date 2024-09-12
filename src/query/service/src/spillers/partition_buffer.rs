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

pub enum PartitionBufferFetchOption {
    /// Read all data from the partition.
    ReadPartition,
    /// Pick data from the partition when the available data reaches or exceeds a certain threshold.
    PickPartitionWithThreshold(usize),
    /// Pick one data block from the partition.
    PickOneDataBlock,
    /// Pick at least `usize` bytes from the partition.
    PickPartitionBytes(usize),
}

// The PartitionBuffer is used to buffer partitioned data blocks in the HashJoin and Window operator.
#[derive(Clone)]
pub struct PartitionBuffer {
    memory_size: usize,
    partition_data: Vec<Vec<DataBlock>>,
    partition_size: Vec<usize>,
}

impl PartitionBuffer {
    pub fn create(num_partitions: usize) -> Self {
        PartitionBuffer {
            memory_size: 0,
            partition_data: vec![Vec::new(); num_partitions],
            partition_size: vec![0; num_partitions],
        }
    }

    // Add a partitiond data block to the PartitionBuffer.
    pub fn add_data_block(&mut self, partition_id: usize, data_block: DataBlock) {
        let data_size = data_block.memory_size();
        self.memory_size += data_size;
        self.partition_size[partition_id] += data_size;
        self.partition_data[partition_id].push(data_block);
    }

    // Fetch data blocks from the PartitionBuffer with the specified option.
    pub fn fetch_data_blocks(
        &mut self,
        partition_id: usize,
        option: &PartitionBufferFetchOption,
    ) -> Result<Option<Vec<DataBlock>>> {
        let data_blocks = match option {
            PartitionBufferFetchOption::ReadPartition => {
                if !self.partition_data[partition_id].is_empty() {
                    Some(self.partition_data[partition_id].clone())
                } else {
                    None
                }
            }
            PartitionBufferFetchOption::PickPartitionWithThreshold(threshold) => {
                if self.partition_size[partition_id] >= *threshold {
                    let data_blocks = self.partition_data[partition_id].clone();
                    self.partition_data[partition_id].clear();
                    self.memory_size -= self.partition_size[partition_id];
                    self.partition_size[partition_id] = 0;
                    Some(data_blocks)
                } else {
                    None
                }
            }
            PartitionBufferFetchOption::PickOneDataBlock => {
                if let Some(data_block) = self.partition_data[partition_id].pop() {
                    let memory_size = data_block.memory_size();
                    self.memory_size -= memory_size;
                    self.partition_size[partition_id] -= memory_size;
                    Some(vec![data_block])
                } else {
                    None
                }
            }
            PartitionBufferFetchOption::PickPartitionBytes(required_bytes) => {
                let partition_data = &mut self.partition_data[partition_id];
                let mut accumulated_bytes = 0;
                let mut data_blocks = Vec::new();
                while let Some(data_block) = partition_data.pop() {
                    accumulated_bytes += data_block.memory_size();
                    data_blocks.push(data_block);
                    if accumulated_bytes >= *required_bytes {
                        break;
                    }
                }
                self.memory_size -= accumulated_bytes;
                self.partition_size[partition_id] -= accumulated_bytes;
                Some(data_blocks)
            }
        };
        Ok(data_blocks)
    }

    pub fn memory_size(&self) -> usize {
        self.memory_size
    }

    pub fn partition_memory_size(&self, partition_id: usize) -> usize {
        self.partition_size[partition_id]
    }

    pub fn partition_ids(&self) -> Vec<usize> {
        let mut partition_ids = vec![];
        for (partition_id, data) in self.partition_data.iter().enumerate() {
            if !data.is_empty() {
                partition_ids.push(partition_id);
            }
        }
        partition_ids
    }

    pub fn is_partition_empty(&self, partition_id: usize) -> bool {
        self.partition_data[partition_id].is_empty()
    }
}
