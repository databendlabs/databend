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
use std::sync::Arc;

use byte_unit::Byte;
use byte_unit::ByteUnit;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_storage::DataOperator;
use log::info;

use crate::pipelines::processors::transforms::hash_join::spill_common::get_hashes;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

// Define some states for hash join build spilling
// Each processor owns its `BuildSpillState`
pub struct BuildSpillState {
    // Hash join build state
    pub build_state: Arc<HashJoinBuildState>,
    // Spiller, responsible for specific spill work
    pub spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(ctx: Arc<QueryContext>, build_state: Arc<HashJoinBuildState>) -> Self {
        let tenant = ctx.get_tenant();
        let spill_config = SpillerConfig::create(query_spill_prefix(&tenant));
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(ctx, operator, spill_config, SpillerType::HashJoinBuild);
        Self {
            build_state,
            spiller,
        }
    }

    // Get all hashes for build input data.
    pub fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.build_state.ctx.get_function_context()?;
        let keys = &self.build_state.hash_join_state.hash_join_desc.build_keys;
        get_hashes(&func_ctx, block, keys, &self.build_state.method, hashes)
    }
}

/// Define some spill-related APIs for hash join build
impl BuildSpillState {
    // Check if need to spill.
    pub(crate) fn check_need_spill(&self, pending_spill_data: &[DataBlock]) -> Result<bool> {
        // Check if the pending spill data is bigger than `spilling_threshold_per_proc`
        let pending_spill_data_size = pending_spill_data
            .iter()
            .fold(0, |acc, block| acc + block.memory_size());
        let spill_threshold_per_proc = self.build_state.spilling_threshold_per_proc;
        if pending_spill_data_size > spill_threshold_per_proc {
            info!(
                "pending spill data: {:?} bytes, spilling threshold per processor: {:?} bytes",
                pending_spill_data_size, spill_threshold_per_proc
            );
            return Ok(true);
        }

        // Check if global memory usage exceeds the threshold.
        let mut global_used = GLOBAL_MEM_STAT.get_memory_usage();
        // `global_used` may be negative at the beginning of starting query.
        if global_used < 0 {
            global_used = 0;
        }
        let max_memory_usage = self.build_state.max_memory_usage;
        let byte = Byte::from_unit(global_used as f64, ByteUnit::B).unwrap();
        let total_gb = byte.get_appropriate_unit(false).format(3);
        if global_used as usize > max_memory_usage {
            let spill_threshold_gb = Byte::from_unit(max_memory_usage as f64, ByteUnit::B)
                .unwrap()
                .get_appropriate_unit(false)
                .format(3);
            info!(
                "need to spill due to global memory usage {:?} is greater than spill threshold {:?}",
                total_gb, spill_threshold_gb
            );
            return Ok(true);
        }
        Ok(false)
    }

    // Pick partitions which need to spill
    #[allow(unused)]
    fn pick_partitions(&self, partition_blocks: &mut HashMap<u8, Vec<DataBlock>>) -> Result<()> {
        let mut max_memory_usage = self.build_state.max_memory_usage;
        let global_used = GLOBAL_MEM_STAT.get_memory_usage();
        if global_used as usize > max_memory_usage {
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
            if size as f64 <= max_memory_usage as f64 / 3.0 {
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
                max_memory_usage -= size;
            } else {
                break;
            }
        }
        Ok(())
    }
}
