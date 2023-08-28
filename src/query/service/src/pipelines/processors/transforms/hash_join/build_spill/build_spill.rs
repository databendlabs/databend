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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::HashMethodKind;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::HashMap;
use common_storage::DataOperator;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spiller::Spiller;
use crate::spiller::SpillerConfig;

/// Define some states for hash join build spilling
pub struct BuildSpillState {
    /// Spilling memory threshold
    pub(crate) spill_memory_threshold: usize,
    /// Hash join build spilling coordinator
    pub(crate) spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub(crate) spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(_ctx: Arc<QueryContext>, spill_coordinator: Arc<BuildSpillCoordinator>) -> Self {
        let spill_config = SpillerConfig::create("hash_join_build_spill".to_string());
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(operator, spill_config);
        Self {
            spill_memory_threshold: 1024,
            spill_coordinator,
            spiller,
        }
    }
}

/// Define some spill-related APIs for hash join build
impl HashJoinBuildState {
    // Start to spill `input_data`.
    pub(crate) fn spill(&mut self) -> Result<()> {
        let mut rows_size = 0;
        self.spill_state
            .spiller
            .input_data
            .iter()
            .for_each(|block| {
                rows_size += block.num_rows();
            });
        // Hash values for all rows in input data.
        let mut hashes = Vec::with_capacity(rows_size);
        let block = DataBlock::concat(&self.spill_state.spiller.input_data)?;
        self.get_hashes(&block, &mut hashes)?;
        // Ensure which partition each row belongs to. Currently we limit the maximum number of partitions to 8.
        // We read the first 3 bits of the hash value as the partition id.

        // partition_id -> row indexes
        let mut partition_map = std::collections::HashMap::with_capacity(8);
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = *hash as u8 & 0b0000_0111;
            partition_map
                .entry(partition_id)
                .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                .or_insert(vec![row_idx]);
        }

        // Take row indexes from block and put them to Spiller `partitions`
        for (partition_id, row_indexes) in partition_map.iter() {
            self.spill_state.spiller.partition_set.push(*partition_id);
            let block_row_indexes = row_indexes
                .iter()
                .map(|idx| (0, *idx, 1))
                .collect::<Vec<_>>();
            let partition_block =
                DataBlock::take_blocks(&[&block], &block_row_indexes, row_indexes.len());
            self.spill_state
                .spiller
                .partitions
                .push((*partition_id, partition_block));
        }

        self.spill_state.spiller.spill()
    }

    // Get all hashes for input data.
    fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.ctx.get_function_context()?;
        let mut evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
        let build_key_columns = self
            .hash_join_state
            .hash_join_desc
            .build_keys
            .iter()
            .map(|expr| {
                let return_type = expr.data_type();
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(return_type, block.num_rows())?)
            })
            .collect::<Result<_>>()?;
        // Todo: simplify the following code
        match &*self.method {
            HashMethodKind::Serializer(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::DictionarySerializer(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::SingleString(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU8(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU16(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU32(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU64(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU128(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU256(method) => {
                let rows_iter = method.keys_iter_from_column(build_key_columns)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
        }
        Ok(())
    }

    // Check if need to spill.
    // Notes: even if input can fit into memory, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self, input: &DataBlock) -> Result<bool> {
        todo!()
    }

    // Directly spill input data if all partitions have been spilled.
    pub(crate) fn spill_input(&self, data_block: DataBlock) -> Result<()> {
        todo!()
    }

    // Buffer the input data for the join build processor
    // It will be spilled when the memory usage exceeds threshold.
    // If data isn't in partition set, it'll be used to build hash table.
    pub(crate) fn buffer_data(&mut self, input: DataBlock) {
        let input_data = &mut self.spill_state.spiller.input_data;
        input_data.push(input)
    }
}
