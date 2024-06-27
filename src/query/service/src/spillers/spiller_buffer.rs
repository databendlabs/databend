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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::sessions::QueryContext;

// The spiller buffer will record each partition's unspilled data.
#[derive(Clone)]
pub(crate) struct SpillerBuffer {
    partition_unspilled_data: HashMap<u8, Vec<DataBlock>>,
    buffer_size: HashMap<u8, usize>,
    partition_buffer_threshold: usize,
}

impl SpillerBuffer {
    pub(crate) fn create(ctx: Arc<QueryContext>) -> Result<Self> {
        let partition_number = 1_u32 << ctx.get_settings().get_join_spilling_partition_bits()?;
        let buffer_threshold = ctx
            .get_settings()
            .get_join_spilling_buffer_threshold_per_proc()?;
        let partition_buffer_threshold = buffer_threshold / partition_number as usize;
        Ok(SpillerBuffer {
            partition_unspilled_data: HashMap::new(),
            buffer_size: HashMap::new(),
            partition_buffer_threshold,
        })
    }

    // Add a partition's unspilled data to the buffer
    // The method will check if the partition's buffer is full, if so, it will spill the partition
    // The return value is the partition id and the spilled data
    pub(crate) fn add_partition_unspilled_data(
        &mut self,
        partition_id: u8,
        data: DataBlock,
    ) -> Result<Option<DataBlock>> {
        let data_size = data.memory_size();
        self.buffer_size
            .entry(partition_id)
            .and_modify(|e| *e += data_size)
            .or_insert(data_size);
        self.partition_unspilled_data
            .entry(partition_id)
            .or_default()
            .push(data);
        if *self.buffer_size.get(&partition_id).unwrap()
            >= self.partition_buffer_threshold * 1000 * 1000
        {
            let blocks = self
                .partition_unspilled_data
                .get_mut(&partition_id)
                .unwrap();
            debug_assert!(!blocks.is_empty());
            let merged_block = DataBlock::concat(blocks)?;
            blocks.clear();
            let old_size = self.buffer_size.get_mut(&partition_id).unwrap();
            *old_size = 0;
            return Ok(Some(merged_block));
        }
        Ok(None)
    }

    pub(crate) fn empty(&self) -> bool {
        // Check if each partition's buffer is empty
        self.buffer_size.iter().all(|(_, v)| *v == 0)
    }

    pub(crate) fn partition_unspilled_data(&self) -> HashMap<u8, Vec<DataBlock>> {
        self.partition_unspilled_data.clone()
    }

    pub(crate) fn reset(&mut self) {
        self.partition_unspilled_data.clear();
        self.buffer_size.clear();
    }
}
