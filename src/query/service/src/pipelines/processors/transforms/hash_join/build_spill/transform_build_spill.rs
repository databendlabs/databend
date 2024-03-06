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

use crate::pipelines::processors::transforms::BuildSpillState;
use crate::pipelines::processors::transforms::HashJoinBuildState;

pub struct BuildSpillHandler {
    // The flag indicates whether data is from spilled data.
    after_spill: bool,
    spill_state: Option<Box<BuildSpillState>>,
    // Data need to spill. If all input data has processed but spilling doesn't happen
    // Add data to row space.
    pending_spill_data: Vec<DataBlock>,
    // The flag indicates whether the build side has happened spilling.
    // If any of build processors has happened spilling, the flag is true.
    spilled: bool,
}

impl BuildSpillHandler {
    pub fn create(spill_state: Option<Box<BuildSpillState>>) -> Self {
        Self {
            after_spill: false,
            spill_state,
            pending_spill_data: vec![],
            spilled: false,
        }
    }

    // If spilling is enabled, return true
    pub(crate) fn enabled_spill(&self) -> bool {
        self.spill_state.is_some()
    }

    pub(crate) fn get_spilled(&self) -> bool {
        self.spilled
    }

    pub(crate) fn set_spilled(&mut self, val: bool) {
        self.spilled = val;
    }

    // Note: the caller should ensure `spill_state` is Some
    pub(crate) fn spill_state(&self) -> &BuildSpillState {
        self.spill_state.as_ref().unwrap()
    }

    pub(crate) fn spill_state_mut(&mut self) -> &mut BuildSpillState {
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
        let pending_spill_data = self.pending_spill_data.clone();
        for block in pending_spill_data.iter() {
            let mut hashes = Vec::with_capacity(block.num_rows());
            let spill_state = self.spill_state_mut();
            spill_state.get_hashes(block, join_type, &mut hashes)?;
            spill_state
                .spiller
                .spill_input(block.clone(), &hashes, false, None)
                .await?;
        }
        self.pending_spill_data.clear();
        Ok(())
    }

    // Finishing up after spilling
    pub(crate) fn finalize_spill(
        &mut self,
        build_state: &Arc<HashJoinBuildState>,
        processor_id: usize,
    ) -> Result<()> {
        // Add spilled partition ids to `spill_partitions` of `HashJoinBuildState`
        let spilled_partition_set = self.spill_state().spiller.spilled_partitions();
        info!(
            "build processor-{:?}: spill finished with spilled partitions {:?}",
            processor_id, spilled_partition_set
        );
        if !spilled_partition_set.is_empty() {
            build_state
                .spilled_partition_set
                .write()
                .extend(spilled_partition_set);
        }
        // The processor has accepted all data from downstream
        // If there is still pending spill data, add to row space.
        for data in self.pending_spill_data.iter() {
            build_state.build(data.clone())?;
        }
        self.pending_spill_data.clear();
        Ok(())
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
                    .read_spilled_data(&(partition_id as u8))
                    .await?,
            )?;
            if !spilled_data.is_empty() {
                return Ok(Some(spilled_data));
            }
        }
        Ok(None)
    }
}
