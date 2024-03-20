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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

// The spiller buffer will record each partition's unspilled data.
// When the buffer is full(>=8MB), it will pick the partition with the most unspilled data to spill.
#[derive(Clone)]
pub(crate) struct SpillerBuffer {
    partition_unspilled_data: HashMap<u8, Vec<DataBlock>>,
    buffer_size: usize,
}

impl SpillerBuffer {
    pub(crate) fn create() -> Self {
        SpillerBuffer {
            partition_unspilled_data: HashMap::new(),
            buffer_size: 0,
        }
    }

    // Add a partition's unspilled data to the buffer
    // The method will check if the buffer is full, if so, it will spill the partition with the most unspilled data
    // After spilling, the buffer will be clear the spilled partition's unspilled data
    // The return value is the partition id and the spilled data
    pub(crate) fn add_partition_unspilled_data(
        &mut self,
        partition_id: u8,
        data: DataBlock,
    ) -> Result<Option<(u8, DataBlock)>> {
        fn rows(data: &[DataBlock]) -> usize {
            data.iter().map(|block| block.num_rows()).sum()
        }
        let data_size = data.memory_size();
        self.buffer_size += data_size;
        self.partition_unspilled_data
            .entry(partition_id)
            .or_default()
            .push(data);
        if self.buffer_size >= 8 * 1024 * 1024 {
            // Pick the partition with the most unspilled data in `partition_unspilled_data`
            let (partition_id, blocks) = self
                .partition_unspilled_data
                .iter()
                .max_by_key(|(_, v)| rows(v))
                .map(|(k, v)| (*k, v.clone()))
                .ok_or_else(|| ErrorCode::Internal("No unspilled data in the buffer"))?;
            debug_assert!(!blocks.is_empty());
            self.partition_unspilled_data.remove(&partition_id);
            let merged_block = DataBlock::concat(&blocks)?;
            self.buffer_size -= merged_block.memory_size();
            return Ok(Some((partition_id, merged_block)));
        }
        Ok(None)
    }

    pub(crate) fn empty(&self) -> bool {
        self.buffer_size == 0
    }

    pub(crate) fn partition_unspilled_data(&self) -> HashMap<u8, Vec<DataBlock>> {
        self.partition_unspilled_data.clone()
    }

    pub(crate) fn reset(&mut self) {
        self.partition_unspilled_data.clear();
        self.buffer_size = 0;
    }
}
