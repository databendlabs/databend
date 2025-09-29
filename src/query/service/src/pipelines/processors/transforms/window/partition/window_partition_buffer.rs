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
use databend_common_pipeline_transforms::MemorySettings;

use crate::spillers::MergedPartition;
use crate::spillers::PartitionBuffer;
use crate::spillers::PartitionBufferFetchOption;
use crate::spillers::Spiller;

/// The `WindowPartitionBuffer` is used to control memory usage of Window operator.
pub struct WindowPartitionBuffer {
    spiller: Spiller,
    memory_settings: MemorySettings,
    partition_buffer: PartitionBuffer,
    restored_partition_buffer: PartitionBuffer,
    num_partitions: usize,
    sort_block_size: usize,
    can_spill: bool,
    next_to_restore_partition_id: isize,
    spilled_small_partitions: Vec<Vec<usize>>,
    spilled_merged_partitions: Vec<(MergedPartition, bool, bool)>,
}

impl WindowPartitionBuffer {
    pub fn new(
        spiller: Spiller,
        num_partitions: usize,
        sort_block_size: usize,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        // Create a `PartitionBuffer` to store partitioned data.
        let partition_buffer = PartitionBuffer::create(num_partitions);
        let restored_partition_buffer = PartitionBuffer::create(num_partitions);
        Ok(Self {
            spiller,
            memory_settings,
            partition_buffer,
            restored_partition_buffer,
            num_partitions,
            sort_block_size,
            can_spill: false,
            next_to_restore_partition_id: -1,
            spilled_small_partitions: vec![Vec::new(); num_partitions],
            spilled_merged_partitions: Vec::new(),
        })
    }

    pub fn need_spill(&mut self) -> bool {
        self.can_spill && self.memory_settings.check_spill()
    }

    pub fn out_of_memory_limit(&mut self) -> bool {
        self.memory_settings.check_spill()
    }

    pub fn is_empty(&self) -> bool {
        self.next_to_restore_partition_id + 1 >= self.num_partitions as isize
    }

    pub fn add_data_block(&mut self, partition_id: usize, data_block: DataBlock) {
        if data_block.is_empty() {
            return;
        }
        self.partition_buffer
            .add_data_block(partition_id, data_block);
        self.can_spill = true;
    }

    // Spill data blocks in the buffer.
    pub async fn spill(&mut self) -> Result<()> {
        let spill_unit_size = self.memory_settings.spill_unit_size;

        // Pick one partition from the last to the first to spill.
        let option = PartitionBufferFetchOption::PickPartitionWithThreshold(0);
        let next_to_restore_partition_id = (self.next_to_restore_partition_id + 1) as usize;
        for partition_id in (next_to_restore_partition_id..self.num_partitions).rev() {
            if !self.partition_buffer.is_partition_empty(partition_id)
                && self.partition_buffer.partition_memory_size(partition_id) > spill_unit_size
            {
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    return self
                        .spiller
                        .spill_with_partition(partition_id, data_blocks)
                        .await;
                }
            }
        }

        // If there is no partition with size greater than `spill_unit_size`, then merge partitions to spill.
        let mut accumulated_bytes = 0;
        let mut partitions_to_spill = Vec::new();
        for partition_id in (next_to_restore_partition_id..self.num_partitions).rev() {
            if !self.partition_buffer.is_partition_empty(partition_id) {
                let partition_memory_size =
                    self.partition_buffer.partition_memory_size(partition_id);
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    partitions_to_spill.push((partition_id, data_blocks));
                    accumulated_bytes += partition_memory_size;
                }
                if accumulated_bytes >= spill_unit_size {
                    break;
                }
            }
        }

        if accumulated_bytes == 0 {
            self.can_spill = false;
            return Ok(());
        }

        let spilled = self
            .spiller
            .spill_with_merged_partitions(partitions_to_spill)
            .await?;
        let index = self.spilled_merged_partitions.len();
        for (id, _) in &spilled.partitions {
            self.spilled_small_partitions[*id].push(index);
        }
        self.spilled_merged_partitions.push((spilled, false, false));
        Ok(())
    }

    // Restore data blocks from buffer and spilled files.
    pub async fn restore(&mut self) -> Result<Vec<DataBlock>> {
        while self.next_to_restore_partition_id + 1 < self.num_partitions as isize {
            self.next_to_restore_partition_id += 1;
            let partition_id = self.next_to_restore_partition_id as usize;
            // Restore large partitions from spilled files.
            let mut result = self.spiller.read_spilled_partition(&partition_id).await?;

            // Restore small merged partitions from spilled files.
            let spilled_small_partitions =
                std::mem::take(&mut self.spilled_small_partitions[partition_id]);
            for index in spilled_small_partitions {
                let out_of_memory_limit = self.out_of_memory_limit();
                let (merged_partitions, restored, partial_restored) =
                    &mut self.spilled_merged_partitions[index];
                if *restored {
                    continue;
                }
                let MergedPartition {
                    location,
                    partitions,
                } = merged_partitions;
                if out_of_memory_limit || *partial_restored {
                    if let Some(pos) = partitions.iter().position(|(id, _)| *id == partition_id) {
                        let data_block = self
                            .spiller
                            .read_chunk(location, &partitions[pos].1)
                            .await?;
                        self.restored_partition_buffer
                            .add_data_block(partition_id, data_block);
                        partitions.remove(pos);
                        *partial_restored = true;
                    }
                } else {
                    let partitioned_data = self
                        .spiller
                        .read_merged_partitions(merged_partitions)
                        .await?;
                    for (partition_id, data_block) in partitioned_data.into_iter() {
                        self.restored_partition_buffer
                            .add_data_block(partition_id, data_block);
                    }
                    *restored = true;
                }
            }

            if !self.partition_buffer.is_partition_empty(partition_id) {
                let option = PartitionBufferFetchOption::PickPartitionWithThreshold(0);
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    result.extend(self.concat_data_blocks(data_blocks)?);
                }
            }

            if !self
                .restored_partition_buffer
                .is_partition_empty(partition_id)
            {
                let option = PartitionBufferFetchOption::PickPartitionWithThreshold(0);
                if let Some(data_blocks) = self
                    .restored_partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    result.extend(self.concat_data_blocks(data_blocks)?);
                }
            }

            if !result.is_empty() {
                return Ok(result);
            }
        }
        Ok(vec![])
    }

    fn concat_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let mut num_rows = 0;
        let mut result = Vec::new();
        let mut current_blocks = Vec::new();

        for data_block in data_blocks.into_iter() {
            num_rows += data_block.num_rows();
            current_blocks.push(data_block);
            if num_rows >= self.sort_block_size {
                result.push(DataBlock::concat(&current_blocks)?);
                num_rows = 0;
                current_blocks.clear();
            }
        }

        if !current_blocks.is_empty() {
            result.push(DataBlock::concat(&current_blocks)?);
        }

        Ok(result)
    }
}

trait Spill {
    async fn spill(blocks: Vec<DataBlock>) -> Result<i16>;
    async fn restore(ordinal: Vec<i16>) -> Result<Vec<DataBlock>>;
}
