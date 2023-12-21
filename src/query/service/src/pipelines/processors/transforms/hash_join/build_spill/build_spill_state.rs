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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use byte_unit::Byte;
use byte_unit::ByteUnit;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_hashtable::hash2bucket;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_sql::plans::JoinType;
use databend_common_storage::DataOperator;
use log::info;

use crate::pipelines::processors::transforms::hash_join::spill_common::get_hashes;
use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

/// Define some states for hash join build spilling
/// Each processor owns its `BuildSpillState`
pub struct BuildSpillState {
    /// Hash join build state
    pub build_state: Arc<HashJoinBuildState>,
    /// Hash join build spilling coordinator
    pub spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(
        ctx: Arc<QueryContext>,
        spill_coordinator: Arc<BuildSpillCoordinator>,
        build_state: Arc<HashJoinBuildState>,
    ) -> Self {
        let tenant = ctx.get_tenant();
        let spill_config = SpillerConfig::create(query_spill_prefix(&tenant));
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(ctx, operator, spill_config, SpillerType::HashJoinBuild);
        Self {
            build_state,
            spill_coordinator,
            spiller,
        }
    }

    // Get all hashes for build input data.
    pub fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.build_state.ctx.get_function_context()?;
        let keys = &self.build_state.hash_join_state.hash_join_desc.build_keys;
        get_hashes(&func_ctx, block, keys, &self.build_state.method, hashes)
    }

    // Collect all buffered data in `RowSpace` and `Chunks`
    // The method will be executed by only one processor.
    fn collect_rows(&self) -> Result<Vec<DataBlock>> {
        let mut blocks = vec![];
        // Collect rows in `RowSpace`'s buffer
        let mut row_space_buffer = self.build_state.hash_join_state.row_space.buffer.write();
        blocks.extend(row_space_buffer.drain(..));
        self.build_state
            .hash_join_state
            .row_space
            .buffer_row_size
            .store(0, Ordering::Relaxed);
        // Collect rows in `Chunks`
        let chunks = &mut unsafe { &mut *self.build_state.hash_join_state.build_state.get() }
            .generation_state
            .chunks;
        blocks.append(chunks);
        let build_state = unsafe { &mut *self.build_state.hash_join_state.build_state.get() };
        build_state.generation_state.build_num_rows = 0;
        Ok(blocks)
    }

    // Partition input blocks to different partitions.
    // Output is <partition_id, blocks>
    fn partition_input_blocks(
        &self,
        input_blocks: Vec<DataBlock>,
    ) -> Result<HashMap<u8, Vec<DataBlock>>> {
        let mut partition_blocks = HashMap::new();
        for block in input_blocks {
            let mut hashes = Vec::with_capacity(block.num_rows());
            self.get_hashes(&block, &mut hashes)?;
            let mut indices = Vec::with_capacity(hashes.len());
            for hash in hashes {
                indices.push(hash2bucket::<3, false>(hash as usize) as u8);
            }
            let scatter_blocks = DataBlock::scatter(&block, &indices, 1 << 3)?;
            for (p_id, p_block) in scatter_blocks.into_iter().enumerate() {
                partition_blocks
                    .entry(p_id as u8)
                    .and_modify(|v: &mut Vec<DataBlock>| v.push(p_block.clone()))
                    .or_insert(vec![p_block]);
            }
        }
        Ok(partition_blocks)
    }
}

/// Define some spill-related APIs for hash join build
impl BuildSpillState {
    #[async_backtrace::framed]
    // Start to spill, get the processor's spill task from `BuildSpillCoordinator`
    pub(crate) async fn spill(&mut self, p_id: usize) -> Result<()> {
        let spill_partitions = {
            let mut spill_tasks = self.spill_coordinator.spill_tasks.lock();
            spill_tasks.pop_back().unwrap()
        };
        self.spiller.spill(spill_partitions, p_id).await
    }

    // Check if need to spill.
    // Notes: even if the method returns false, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self) -> Result<bool> {
        let settings = self.build_state.ctx.get_settings();
        let spill_threshold = settings.get_join_spilling_threshold()?;
        // If `spill_threshold` is 0, we won't limit memory.
        let enable_spill = spill_threshold != 0
            && self.build_state.hash_join_state.hash_join_desc.join_type == JoinType::Inner;
        if !enable_spill || self.spiller.is_all_spilled() {
            return Ok(false);
        }

        // Check if there are rows in `RowSpace`'s buffer and `Chunks`.
        // If not, directly return false, no need to spill.
        let buffer = self.build_state.hash_join_state.row_space.buffer.read();
        let chunks = &mut unsafe { &mut *self.build_state.hash_join_state.build_state.get() }
            .generation_state
            .chunks;

        if buffer.is_empty() && chunks.is_empty() {
            return Ok(false);
        }

        // Check if global memory usage exceeds the threshold.
        let global_used = GLOBAL_MEM_STAT.get_memory_usage();
        let byte = Byte::from_unit(global_used as f64, ByteUnit::B).unwrap();
        let total_gb = byte.get_appropriate_unit(false).format(3);
        if global_used as usize > spill_threshold {
            info!(
                "need to spill due to global memory usage {:?} is greater than spill threshold",
                total_gb
            );
            return Ok(true);
        }

        let mut total_bytes = 0;
        for block in buffer.iter() {
            total_bytes += block.memory_size();
        }

        for block in chunks.iter() {
            total_bytes += block.memory_size();
        }

        if total_bytes * 3 > spill_threshold {
            return Ok(true);
        }
        Ok(false)
    }

    // Pick partitions which need to spill
    #[allow(unused)]
    fn pick_partitions(&self, partition_blocks: &mut HashMap<u8, Vec<DataBlock>>) -> Result<()> {
        let mut memory_limit = self
            .build_state
            .ctx
            .get_settings()
            .get_join_spilling_threshold()?;
        let global_used = GLOBAL_MEM_STAT.get_memory_usage();
        if global_used as usize > memory_limit {
            return Ok(());
        }
        // Compute each partition's data size
        let mut partition_sizes = partition_blocks
            .iter()
            .map(|(id, blocks)| {
                let size = blocks
                    .iter()
                    .fold(0, |acc, block| acc + block.memory_size());
                (*id, size)
            })
            .collect::<Vec<(u8, usize)>>();
        partition_sizes.sort_by_key(|&(_id, size)| size);

        for (id, size) in partition_sizes.into_iter() {
            if size as f64 <= memory_limit as f64 / 3.0 {
                // Put the partition's data to chunks
                let chunks =
                    &mut unsafe { &mut *self.build_state.hash_join_state.build_state.get() }
                        .generation_state
                        .chunks;
                let blocks = partition_blocks.get_mut(&id).unwrap();
                let rows_num = blocks.iter().fold(0, |acc, block| acc + block.num_rows());
                chunks.append(blocks);
                let build_state =
                    unsafe { &mut *self.build_state.hash_join_state.build_state.get() };
                build_state.generation_state.build_num_rows += rows_num;
                partition_blocks.remove(&id);
                memory_limit -= size;
            } else {
                break;
            }
        }
        Ok(())
    }

    // Tasks will be sent to `BuildSpillCoordinator`.
    pub(crate) fn split_spill_tasks(
        &self,
        active_processors_num: usize,
        spill_tasks: &mut VecDeque<Vec<(u8, DataBlock)>>,
    ) -> Result<()> {
        let blocks = self.collect_rows()?;
        let partition_blocks = self.partition_input_blocks(blocks)?;
        // self.pick_partitions(&mut partition_blocks)?;
        let mut partition_tasks = HashMap::with_capacity(partition_blocks.len());
        // Stat how many rows in each partition, then split it equally.
        for (partition_id, blocks) in partition_blocks.iter() {
            let merged_block = DataBlock::concat(blocks)?;
            let total_rows = merged_block.num_rows();
            // Equally split blocks to `active_processors_num` parts
            let mut start_row;
            let mut end_row = 0;
            for i in 0..active_processors_num {
                start_row = end_row;
                end_row = start_row + total_rows / active_processors_num;
                if i == active_processors_num - 1 {
                    end_row = total_rows;
                }
                let sub_block = merged_block.slice(start_row..end_row);
                partition_tasks
                    .entry(*partition_id)
                    .and_modify(|v: &mut Vec<DataBlock>| v.push(sub_block.clone()))
                    .or_insert(vec![sub_block]);
            }
        }

        let mut task_id: usize = 0;
        while task_id < active_processors_num {
            let mut task = Vec::with_capacity(partition_tasks.len());
            for partition_id in partition_tasks.keys() {
                task.push((
                    *partition_id,
                    partition_tasks.get(partition_id).unwrap()[task_id].clone(),
                ))
            }
            spill_tasks.push_back(task);
            task_id += 1;
        }

        Ok(())
    }
}
