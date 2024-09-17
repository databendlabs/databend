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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_settings::Settings;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;

use crate::sessions::QueryContext;
use crate::spillers::PartitionBuffer;
use crate::spillers::PartitionBufferFetchOption;
use crate::spillers::SpilledData;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

/// The `WindowPartitionBuffer` is used to control memory usage of Window operator.
pub struct WindowPartitionBuffer {
    spiller: Spiller,
    spill_settings: WindowSpillSettings,
    partition_buffer: PartitionBuffer,
    num_partitions: usize,
    can_spill: bool,
    next_to_restore_partition_id: usize,
    spilled_small_partitions: Vec<Vec<usize>>,
    spilled_merged_partitions: Vec<(SpilledData, bool)>,
}

impl WindowPartitionBuffer {
    pub fn new(
        ctx: Arc<QueryContext>,
        num_partitions: usize,
        spill_settings: WindowSpillSettings,
    ) -> Result<Self> {
        // Create an inner `Spiller` to spill data.
        let spill_config = SpillerConfig::create(query_spill_prefix(
            ctx.get_tenant().tenant_name(),
            &ctx.get_id(),
        ));
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(ctx.clone(), operator, spill_config, SpillerType::Window)?;

        // Create a `PartitionBuffer` to store partitioned data.
        let partition_buffer = PartitionBuffer::create(num_partitions);
        Ok(Self {
            spiller,
            spill_settings,
            partition_buffer,
            num_partitions,
            can_spill: false,
            next_to_restore_partition_id: 0,
            spilled_small_partitions: vec![Vec::new(); num_partitions],
            spilled_merged_partitions: Vec::new(),
        })
    }

    pub fn need_spill(&mut self) -> bool {
        if !self.spill_settings.enable_spill || !self.can_spill {
            return false;
        }
        self.out_of_memory_limit()
    }

    pub fn out_of_memory_limit(&mut self) -> bool {
        // Check if processor memory usage exceeds the threshold.
        if self.partition_buffer.memory_size() > self.spill_settings.processor_memory_threshold {
            return true;
        }

        // Check if global memory usage exceeds the threshold.
        let global_memory_usage = std::cmp::max(GLOBAL_MEM_STAT.get_memory_usage(), 0) as usize;
        global_memory_usage > self.spill_settings.global_memory_threshold
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
        let spill_unit_size = self.spill_settings.spill_unit_size;

        // Pick one partition from the last to the first to spill.
        let option = PartitionBufferFetchOption::PickPartitionWithThreshold(0);
        for partition_id in (self.next_to_restore_partition_id + 1..self.num_partitions).rev() {
            if !self.partition_buffer.is_partition_empty(partition_id)
                && self.partition_buffer.partition_memory_size(partition_id) > spill_unit_size
            {
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    return self
                        .spiller
                        .spill_with_partition(partition_id, DataBlock::concat(&data_blocks)?)
                        .await;
                }
            }
        }

        // If there is no partition with size greater than `spill_unit_size`, then merge partitions to spill.
        let mut accumulated_bytes = 0;
        let mut partitions_to_spill = Vec::new();
        for partition_id in (self.next_to_restore_partition_id + 1..self.num_partitions).rev() {
            if !self.partition_buffer.is_partition_empty(partition_id) {
                let partition_memory_size =
                    self.partition_buffer.partition_memory_size(partition_id);
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    let data_block = DataBlock::concat(&data_blocks)?;
                    partitions_to_spill.push((partition_id, data_block));
                    accumulated_bytes += partition_memory_size;
                }
                if accumulated_bytes >= spill_unit_size {
                    break;
                }
            }
        }

        if accumulated_bytes > 0 {
            let spilled_data = self
                .spiller
                .spill_with_merged_partitions(partitions_to_spill)
                .await?;
            if let SpilledData::MergedPartition {
                location,
                partitions,
            } = spilled_data
            {
                let index = self.spilled_merged_partitions.len();
                for partition in partitions.iter() {
                    self.spilled_small_partitions[partition.0].push(index);
                }
                self.spilled_merged_partitions.push((
                    SpilledData::MergedPartition {
                        location,
                        partitions,
                    },
                    true,
                ));
                return Ok(());
            }
        }

        self.can_spill = false;

        Ok(())
    }

    // Restore data blocks from buffer and spilled files.
    pub async fn restore(&mut self) -> Result<Vec<DataBlock>> {
        while self.next_to_restore_partition_id < self.num_partitions {
            let partition_id = self.next_to_restore_partition_id;
            // Restore large partitions from spilled files.
            let mut result = self.spiller.read_spilled_partition(&partition_id).await?;

            // Restore small merged partitions from spilled files.
            let spilled_small_partitions =
                std::mem::take(&mut self.spilled_small_partitions[partition_id]);
            for index in spilled_small_partitions {
                let out_of_memory_limit = self.out_of_memory_limit();
                let (merged_partitions, valid) = &mut self.spilled_merged_partitions[index];
                if !*valid {
                    continue;
                }
                if let SpilledData::MergedPartition {
                    location,
                    partitions,
                } = merged_partitions
                {
                    if out_of_memory_limit {
                        if let Some(pos) = partitions.iter().position(|p| p.0 == partition_id) {
                            let data_range = &partitions[pos].1;
                            let columns_layout = &partitions[pos].2;
                            let data_block = self
                                .spiller
                                .read_range(location, data_range.clone(), columns_layout)
                                .await?;
                            self.partition_buffer
                                .add_data_block(partition_id, data_block);
                            partitions.remove(pos);
                        }
                    } else {
                        let partitioned_data = self
                            .spiller
                            .read_merged_partitions(merged_partitions)
                            .await?;
                        for (partition_id, data_block) in partitioned_data.into_iter() {
                            self.partition_buffer
                                .add_data_block(partition_id, data_block);
                        }
                        *valid = false;
                    }
                }
            }

            if !self.partition_buffer.is_partition_empty(partition_id) {
                let option = PartitionBufferFetchOption::PickPartitionWithThreshold(0);
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &option)?
                {
                    result.extend(data_blocks);
                }
            }

            self.next_to_restore_partition_id += 1;
            if !result.is_empty() {
                return Ok(result);
            }
        }
        Ok(vec![])
    }

    pub fn is_empty(&self) -> bool {
        self.next_to_restore_partition_id >= self.num_partitions
    }
}

#[derive(Clone, Debug, Default)]
pub struct WindowSpillSettings {
    enable_spill: bool,
    global_memory_threshold: usize,
    processor_memory_threshold: usize,
    spill_unit_size: usize,
}

impl WindowSpillSettings {
    pub fn new(settings: Arc<Settings>, num_threads: usize) -> Result<Self> {
        let global_memory_ratio =
            std::cmp::min(settings.get_window_partition_spilling_memory_ratio()?, 100) as f64
                / 100_f64;

        if global_memory_ratio == 0.0 {
            return Ok(WindowSpillSettings {
                enable_spill: false,
                global_memory_threshold: usize::MAX,
                processor_memory_threshold: usize::MAX,
                spill_unit_size: 0,
            });
        }

        let global_memory_threshold = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => match global_memory_ratio {
                mr if mr == 0_f64 => usize::MAX,
                mr => (max_memory_usage as f64 * mr) as usize,
            },
        };

        let processor_memory_threshold =
            settings.get_window_partition_spilling_bytes_threshold_per_proc()?;
        let processor_memory_threshold = match processor_memory_threshold {
            0 => global_memory_threshold / num_threads,
            bytes => bytes,
        };

        let spill_unit_size = settings.get_window_spill_unit_size_mb()? * 1024 * 1024;

        Ok(WindowSpillSettings {
            enable_spill: true,
            global_memory_threshold,
            processor_memory_threshold,
            spill_unit_size,
        })
    }
}
