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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_sql::plans::JoinType;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::hash_join::spill_common::get_hashes;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;
use crate::spillers::PartitionBuffer;
use crate::spillers::PartitionBufferFetchOption;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

/// The HashJoinSpiller is used to spill/restore data blocks of HashJoin,
/// it is used for both build side and probe side.
pub struct HashJoinSpiller {
    spiller: Spiller,
    partition_buffer: PartitionBuffer,
    partition_threshold: usize,
    join_type: JoinType,
    join_state: Arc<HashJoinState>,
    is_build_side: bool,
    func_ctx: FunctionContext,
    /// Used for partition.
    spill_partition_bits: usize,
    hash_keys: Vec<Expr>,
    hash_method: HashMethodKind,
    /// Next restore file index, only used for cross join.
    next_restore_file: usize,
}

impl HashJoinSpiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        join_state: Arc<HashJoinState>,
        hash_keys: Vec<Expr>,
        hash_method: HashMethodKind,
        spill_partition_bits: usize,
        partition_buffer_threshold: usize,
        is_build_side: bool,
    ) -> Result<Self> {
        // Create a Spiller for spilling build side data.
        let spiller_type = if is_build_side {
            SpillerType::HashJoinBuild
        } else {
            SpillerType::HashJoinProbe
        };

        let location_prefix = ctx.query_id_spill_prefix();

        let spill_config = SpillerConfig {
            spiller_type,
            location_prefix,
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };
        let operator = DataOperator::instance().spill_operator();
        let spiller = Spiller::create(ctx.clone(), operator, spill_config)?;

        let num_partitions = (1 << spill_partition_bits) as usize;
        // The memory threshold of each partition, we will spill the partition data
        // if the partition memory size exceeds the threshold.
        let partition_threshold = partition_buffer_threshold * 1024 * 1024 / num_partitions;

        // Create a PartitionBuffer to buffer data before spilling.
        let partition_buffer = PartitionBuffer::create(num_partitions);

        let join_type = join_state.join_type();
        Ok(Self {
            spiller,
            partition_buffer,
            partition_threshold,
            spill_partition_bits,
            hash_keys,
            hash_method,
            join_type,
            join_state,
            func_ctx: ctx.get_function_context()?,
            is_build_side,
            next_restore_file: 0,
        })
    }

    // Just add datablocks to SpillBuffer without spilling.
    pub(crate) fn buffer(&mut self, data_blocks: &[DataBlock]) -> Result<()> {
        if data_blocks.is_empty() {
            return Ok(());
        }
        let join_type = self.join_type.clone();
        let data_block = DataBlock::concat(data_blocks)?;
        let partition_data_blocks =
            self.partition_data_block(&data_block, &join_type, self.spill_partition_bits)?;
        for (partition_id, data_block) in partition_data_blocks.into_iter().enumerate() {
            if !data_block.is_empty() {
                self.partition_buffer
                    .add_data_block(partition_id, data_block);
            }
        }
        Ok(())
    }

    // Spill data blocks, and return unspilled data blocks.
    pub(crate) async fn spill(
        &mut self,
        data_blocks: &[DataBlock],
        partition_need_to_spill: Option<&HashSet<usize>>,
    ) -> Result<Vec<DataBlock>> {
        let join_type = self.join_type.clone();
        let mut unspilled_data_blocks = vec![];
        let data_block = DataBlock::concat(data_blocks)?;
        let fetch_option =
            PartitionBufferFetchOption::PickPartitionWithThreshold(self.partition_threshold);
        for (partition_id, data_block) in self
            .partition_data_block(&data_block, &join_type, self.spill_partition_bits)?
            .into_iter()
            .enumerate()
        {
            if !data_block.is_empty() {
                if let Some(partition_need_to_spill) = partition_need_to_spill
                    && !partition_need_to_spill.contains(&(partition_id))
                {
                    unspilled_data_blocks.push(data_block);
                    continue;
                }
                self.partition_buffer
                    .add_data_block(partition_id, data_block);
                if let Some(data_blocks) = self
                    .partition_buffer
                    .fetch_data_blocks(partition_id, &fetch_option)?
                {
                    self.spiller
                        .spill_with_partition(partition_id, data_blocks)
                        .await?;
                }
            }
        }
        Ok(unspilled_data_blocks)
    }

    // Restore data blocks from SpillBuffer and spilled files.
    pub(crate) async fn restore(&mut self, partition_id: usize) -> Result<Vec<DataBlock>> {
        let mut data_blocks = vec![];
        // 1. restore data from SpillBuffer.
        let option = if self.can_pick_buffer() {
            PartitionBufferFetchOption::PickPartitionWithThreshold(0)
        } else {
            PartitionBufferFetchOption::ReadPartition
        };
        if self.need_read_buffer()
            && let Some(buffer_blocks) = self
                .partition_buffer
                .fetch_data_blocks(partition_id, &option)?
        {
            data_blocks.extend(buffer_blocks);
        }

        // 2. restore data from spilled files.
        if self.need_read_partition() {
            let partition_data_blocks = self.spiller.read_spilled_partition(&partition_id).await?;
            if !partition_data_blocks.is_empty() {
                let spilled_data = DataBlock::concat(&partition_data_blocks)?;
                if !spilled_data.is_empty() {
                    data_blocks.push(spilled_data);
                }
            }
        } else {
            // Cross join.
            let spilled_files = self.spiller.private_spilled_files();
            let file_index = self.next_restore_file;

            if file_index < spilled_files.len() {
                let spilled_data = self
                    .spiller
                    .read_spilled_file(&spilled_files[file_index])
                    .await?;
                if spilled_data.num_rows() != 0 {
                    data_blocks.push(spilled_data);
                }
                self.next_restore_file += 1;
            }
        }
        Ok(data_blocks)
    }

    fn partition_data_block(
        &mut self,
        data_block: &DataBlock,
        join_type: &JoinType,
        partition_bits: usize,
    ) -> Result<Vec<DataBlock>> {
        if join_type == &JoinType::Cross {
            Ok(vec![data_block.clone()])
        } else {
            let mut hashes = self.get_hashes(data_block, join_type)?;
            for hash in hashes.iter_mut() {
                *hash = Self::get_partition_id(*hash as usize, partition_bits) as u64;
            }
            let partition_blocks = DataBlock::scatter(data_block, &hashes, 1 << partition_bits)?;
            Ok(partition_blocks)
        }
    }

    #[inline(always)]
    fn get_partition_id(hash: usize, bits: usize) -> usize {
        (hash >> (32 - bits)) & ((1 << bits) - 1)
    }

    // Get all hashes for build input data.
    fn get_hashes(&self, data_block: &DataBlock, join_type: &JoinType) -> Result<Vec<u64>> {
        let mut hashes = Vec::with_capacity(data_block.num_rows());
        get_hashes(
            &self.func_ctx,
            data_block,
            &self.hash_keys,
            &self.hash_method,
            join_type,
            self.is_build_side,
            &self.join_state.hash_join_desc.is_null_equal,
            &mut hashes,
        )?;
        Ok(hashes)
    }

    pub(crate) fn spilled_partitions(&self) -> HashSet<usize> {
        let mut partition_ids = self.spiller.spilled_partitions();
        for partition_id in self.partition_buffer.partition_ids() {
            partition_ids.insert(partition_id);
        }
        partition_ids
    }

    pub fn has_next_restore_file(&self) -> bool {
        self.next_restore_file < self.spiller.private_spilled_files().len()
            || (self.next_restore_file == 0 && !self.partition_buffer.is_partition_empty(0))
    }

    pub fn reset_next_restore_file(&mut self) {
        self.next_restore_file = 0;
    }

    pub fn need_read_buffer(&self) -> bool {
        self.join_type != JoinType::Cross || self.next_restore_file == 0
    }

    pub fn need_read_partition(&self) -> bool {
        self.join_type != JoinType::Cross
    }

    pub fn can_pick_buffer(&self) -> bool {
        self.is_build_side || self.join_type != JoinType::Cross
    }
}
