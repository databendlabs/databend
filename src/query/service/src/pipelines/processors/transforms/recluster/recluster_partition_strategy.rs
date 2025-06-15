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
use databend_common_storages_fuse::io::StreamBlockBuilder;
use databend_common_storages_fuse::io::StreamBlockProperties;

use crate::pipelines::processors::transforms::PartitionProcessStrategy;

/// `ReclusterPartitionStrategy` is used when block stream writing is enabled.
/// It incrementally writes blocks using `StreamBlockBuilder`, which allows
/// partial serialization and flush during reclustering (e.g., Hilbert clustering).
pub struct ReclusterPartitionStrategy {
    properties: Arc<StreamBlockProperties>,
}

impl ReclusterPartitionStrategy {
    pub fn new(properties: Arc<StreamBlockProperties>) -> Self {
        Self { properties }
    }
}

impl PartitionProcessStrategy for ReclusterPartitionStrategy {
    const NAME: &'static str = "Recluster";

    fn calc_partitions(
        &self,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
    ) -> Vec<usize> {
        (0..num_partitions)
            .filter(|&partition| (partition * num_processors) / num_partitions == processor_id)
            .collect()
    }

    /// Stream write each block, and flush it conditionally based on builder status
    /// and input size estimation.
    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let blocks_num = data_blocks.len();
        let mut accumulated_rows = 0;
        let mut accumulated_bytes = 0;
        let mut pending_blocks = Vec::with_capacity(blocks_num);
        let mut staged_blocks = Vec::with_capacity(blocks_num);
        let mut compacted = Vec::with_capacity(blocks_num);
        for block in data_blocks {
            accumulated_rows += block.num_rows();
            accumulated_bytes += block.estimate_block_size();
            pending_blocks.push(block);
            if !self
                .properties
                .check_large_enough(accumulated_rows, accumulated_bytes)
            {
                continue;
            }
            if !staged_blocks.is_empty() {
                compacted.push(std::mem::take(&mut staged_blocks));
            }
            std::mem::swap(&mut staged_blocks, &mut pending_blocks);
            accumulated_rows = 0;
            accumulated_bytes = 0;
        }
        staged_blocks.append(&mut pending_blocks);
        if !staged_blocks.is_empty() {
            compacted.push(std::mem::take(&mut staged_blocks));
        }

        let mut result = Vec::new();
        let mut builder = StreamBlockBuilder::try_new_with_config(self.properties.clone())?;
        for blocks in compacted {
            for block in blocks {
                builder.write(block)?;
            }
            if builder.need_flush() {
                let serialized = builder.finish()?;
                result.push(DataBlock::empty_with_meta(Box::new(serialized)));
                builder = StreamBlockBuilder::try_new_with_config(self.properties.clone())?;
            }
        }
        if !builder.is_empty() {
            let serialized = builder.finish()?;
            result.push(DataBlock::empty_with_meta(Box::new(serialized)));
        }
        Ok(result)
    }
}

/// `CompactPartitionStrategy` is used when stream write is NOT enabled.
/// It uses a traditional "accumulate and concat" strategy to build large blocks
/// once input thresholds (row count or size) are exceeded.
pub struct CompactPartitionStrategy {
    max_bytes_per_block: usize,
    max_rows_per_block: usize,
}

impl CompactPartitionStrategy {
    pub fn new(max_rows_per_block: usize, max_bytes_per_block: usize) -> Self {
        Self {
            max_bytes_per_block,
            max_rows_per_block,
        }
    }

    fn concat_blocks(blocks: Vec<DataBlock>) -> Result<DataBlock> {
        DataBlock::concat(&blocks)
    }

    fn check_large_enough(&self, rows: usize, bytes: usize) -> bool {
        rows >= self.max_rows_per_block || bytes >= self.max_bytes_per_block
    }
}

impl PartitionProcessStrategy for CompactPartitionStrategy {
    const NAME: &'static str = "Compact";

    fn calc_partitions(
        &self,
        processor_id: usize,
        num_processors: usize,
        num_partitions: usize,
    ) -> Vec<usize> {
        (0..num_partitions)
            .filter(|&partition| (partition * num_processors) / num_partitions == processor_id)
            .collect()
    }

    /// Collects blocks into batches and merges them via `concat` when size threshold is reached.
    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let blocks_num = data_blocks.len();
        if blocks_num < 2 {
            return Ok(data_blocks);
        }

        let mut accumulated_rows = 0;
        let mut accumulated_bytes = 0;
        let mut pending_blocks = Vec::with_capacity(blocks_num);
        let mut staged_blocks = Vec::with_capacity(blocks_num);
        let mut result = Vec::with_capacity(blocks_num);
        for block in data_blocks {
            accumulated_rows += block.num_rows();
            accumulated_bytes += block.estimate_block_size();
            pending_blocks.push(block);
            if !self.check_large_enough(accumulated_rows, accumulated_bytes) {
                continue;
            }
            if !staged_blocks.is_empty() {
                result.push(Self::concat_blocks(std::mem::take(&mut staged_blocks))?);
            }
            std::mem::swap(&mut staged_blocks, &mut pending_blocks);
            accumulated_rows = 0;
            accumulated_bytes = 0;
        }

        staged_blocks.append(&mut pending_blocks);
        if !staged_blocks.is_empty() {
            result.push(Self::concat_blocks(std::mem::take(&mut staged_blocks))?);
        }

        Ok(result)
    }
}
