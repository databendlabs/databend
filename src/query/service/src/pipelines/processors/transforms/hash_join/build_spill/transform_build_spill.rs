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
use databend_common_sql::plans::JoinType;
use log::info;

use crate::pipelines::processors::transforms::hash_join::HashJoinBuildStep;
use crate::pipelines::processors::transforms::BuildSpillState;
use crate::pipelines::processors::transforms::HashJoinBuildState;

pub struct BuildSpillHandler {
    // The flag indicates whether data is from spilled data.
    after_spill: bool,
    spill_state: Option<Box<BuildSpillState>>,
    // Data need to spill. If all input data has processed but spilling doesn't happen
    // Add data to row space.
    pending_spill_data: Vec<DataBlock>,
}

impl BuildSpillHandler {
    pub fn create(spill_state: Option<Box<BuildSpillState>>) -> Self {
        Self {
            after_spill: false,
            spill_state,
            pending_spill_data: vec![],
        }
    }

    // If spilling is enabled, return true
    pub(crate) fn enabled_spill(&self) -> bool {
        self.spill_state.is_some()
    }

    // Note: the caller should ensure `spill_state` is Some
    pub(crate) fn spill_state(&self) -> &BuildSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_ref().unwrap()
    }

    pub(crate) fn spill_state_mut(&mut self) -> &mut BuildSpillState {
        debug_assert!(self.spill_state.is_some());
        self.spill_state.as_mut().unwrap()
    }

    pub(crate) fn after_spill(&self) -> bool {
        self.after_spill
    }

    pub(crate) fn set_after_spill(&mut self, val: bool) {
        self.after_spill = val;
    }

    pub(crate) fn add_pending_spill_data(&mut self, data: DataBlock) {
        self.pending_spill_data.push(data);
    }

    pub(crate) fn pending_spill_data(&self) -> &[DataBlock] {
        &self.pending_spill_data
    }

    // Check if need to wait spill
    pub(crate) fn check_need_spill(&mut self, input: &mut Option<DataBlock>) -> Result<bool> {
        if !self.enabled_spill() || self.after_spill() {
            return Ok(false);
        }
        if let Some(input) = input.take() {
            self.add_pending_spill_data(input);
        }
        // If there is no pending spill data, return false
        let rows_num = self
            .pending_spill_data
            .iter()
            .fold(0, |acc, block| acc + block.num_rows());
        if rows_num == 0 {
            return Ok(false);
        }

        self.spill_state()
            .check_need_spill(self.pending_spill_data())
    }

    // Spill pending data block
    pub(crate) async fn spill(&mut self, join_type: &JoinType) -> Result<()> {
        if join_type == &JoinType::Cross {
            return self.spill_cross_join().await;
        }
        if self.pending_spill_data.is_empty() && !self.spill_state().spiller.empty_buffer() {
            // Spill data in spiller buffer
            return self.spill_state_mut().spiller.spill_buffer().await;
        }
        // Concat the data blocks that pending to spill to reduce the spill file number.
        let pending_spill_data = DataBlock::concat(&self.pending_spill_data)?;
        let mut hashes = Vec::with_capacity(pending_spill_data.num_rows());
        let spill_state = self.spill_state_mut();
        spill_state.get_hashes(&pending_spill_data, join_type, &mut hashes)?;
        spill_state
            .spiller
            .spill_input(pending_spill_data.clone(), &hashes, false, None)
            .await?;
        self.pending_spill_data.clear();
        Ok(())
    }

    // The method is dedicated to spill for cross join which doesn't need to consider partition
    async fn spill_cross_join(&mut self) -> Result<()> {
        let data = DataBlock::concat(&self.pending_spill_data)?;
        let spill_state = self.spill_state_mut();
        // Directly spill the data, don't need to calculate hashes.
        spill_state.spiller.spill_block(data).await?;
        // Add a dummy partition id to indicate spilling has happened.
        spill_state.spiller.partition_location.insert(0, vec![]);
        self.pending_spill_data.clear();
        Ok(())
    }

    // Finishing up after spilling
    pub(crate) fn finalize_spill(
        &mut self,
        build_state: &Arc<HashJoinBuildState>,
        processor_id: usize,
    ) -> Result<HashJoinBuildStep> {
        let spilled_partition_set = self.spill_state().spiller.spilled_partitions();
        // For left-related join, will spill all build input blocks which means there isn't first-round hash table.
        // Because first-round hash table will make left join generate wrong results.
        // Todo: make left-related join leverage first-round hash table to reduce I/O.
        if matches!(
            build_state.join_type(),
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) && !spilled_partition_set.is_empty()
            && !self.pending_spill_data().is_empty()
        {
            return Ok(HashJoinBuildStep::Spill);
        }

        // The processor has accepted all data from downstream
        // If there is still pending spill data, add to row space.
        for data in self.pending_spill_data.iter() {
            build_state.build(data.clone())?;
        }
        self.pending_spill_data.clear();
        // Check if there is data in spiller buffer
        if !self.spill_state().spiller.empty_buffer() {
            return Ok(HashJoinBuildStep::Spill);
        }
        if build_state.join_type() != JoinType::Cross {
            let spill_info = self.spill_state().spiller.format_spill_info();
            if !spill_info.is_empty() {
                info!("Processor: {}, spill info: {}", processor_id, spill_info);
            }
        }
        if !spilled_partition_set.is_empty() {
            build_state
                .spilled_partition_set
                .write()
                .extend(spilled_partition_set);
        }
        Ok(HashJoinBuildStep::Running)
    }

    // Restore
    pub(crate) async fn restore(&mut self, partition_id: i8) -> Result<Option<DataBlock>> {
        let spill_state = self.spill_state();
        if spill_state
            .spiller
            .partition_location
            .contains_key(&(partition_id as u8))
        {
            let spilled_data = DataBlock::concat(
                &spill_state
                    .spiller
                    .read_spilled_partition(&(partition_id as u8))
                    .await?,
            )?;
            if !spilled_data.is_empty() {
                return Ok(Some(spilled_data));
            }
        }
        Ok(None)
    }

    // Restore data for cross join
    pub(crate) async fn restore_cross_join(&mut self) -> Result<Option<DataBlock>> {
        // Each round will read one spill file for current processor.
        let spill_state = self.spill_state_mut();
        let spilled_files = spill_state.spiller.spilled_files();
        if !spilled_files.is_empty() {
            let block = spill_state
                .spiller
                .read_spilled_file(&spilled_files[0])
                .await?;
            spill_state.spiller.columns_layout.remove(&spilled_files[0]);
            if block.num_rows() != 0 {
                return Ok(Some(block));
            }
        }
        Ok(None)
    }
}
