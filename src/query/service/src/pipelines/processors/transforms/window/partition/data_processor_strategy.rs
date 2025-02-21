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

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::memory_size;
use databend_common_pipeline_transforms::sort_merge;
use databend_common_settings::Settings;

pub trait DataProcessorStrategy: Send + Sync + 'static {
    const NAME: &'static str;
    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>>;
}

pub struct CompactStrategy {
    thresholds: BlockThresholds,
}

impl CompactStrategy {
    pub fn new(thresholds: BlockThresholds) -> Self {
        Self { thresholds }
    }

    fn concat_blocks(blocks: Vec<DataBlock>) -> Result<DataBlock> {
        DataBlock::concat(&blocks)
    }
}

impl DataProcessorStrategy for CompactStrategy {
    const NAME: &'static str = "Compact";

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
            accumulated_bytes += memory_size(&block);
            if !self
                .thresholds
                .check_large_enough(accumulated_rows, accumulated_bytes)
            {
                pending_blocks.push(block);
                continue;
            }

            if !staged_blocks.is_empty() {
                result.push(Self::concat_blocks(std::mem::take(&mut staged_blocks))?);
            }

            if pending_blocks.is_empty()
                || self
                    .thresholds
                    .check_for_compact(accumulated_rows, accumulated_bytes)
            {
                std::mem::swap(&mut staged_blocks, &mut pending_blocks);
            } else {
                result.push(Self::concat_blocks(std::mem::take(&mut pending_blocks))?);
            }

            staged_blocks.push(block);
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

pub struct SortStrategy {
    sort_desc: Vec<SortColumnDescription>,
    schema: DataSchemaRef,
    max_block_size: usize,
    sort_spilling_batch_bytes: usize,
    enable_loser_tree: bool,
    have_order_col: bool,
}

impl SortStrategy {
    pub fn try_create(
        settings: &Settings,
        sort_desc: Vec<SortColumnDescription>,
        schema: DataSchemaRef,
        have_order_col: bool,
    ) -> Result<Self> {
        let max_block_size = settings.get_max_block_size()? as usize;
        let enable_loser_tree = settings.get_enable_loser_tree_merge_sort()?;
        let sort_spilling_batch_bytes = settings.get_sort_spilling_batch_bytes()?;
        Ok(Self {
            sort_desc,
            schema,
            max_block_size,
            sort_spilling_batch_bytes,
            enable_loser_tree,
            have_order_col,
        })
    }
}

impl DataProcessorStrategy for SortStrategy {
    const NAME: &'static str = "Sort";

    fn process_data_blocks(&self, data_blocks: Vec<DataBlock>) -> Result<Vec<DataBlock>> {
        let data_blocks = data_blocks
            .into_iter()
            .map(|data_block| DataBlock::sort(&data_block, &self.sort_desc, None))
            .collect::<Result<Vec<_>>>()?;

        sort_merge(
            self.schema.clone(),
            self.max_block_size,
            self.sort_desc.clone(),
            data_blocks,
            self.sort_spilling_batch_bytes,
            self.enable_loser_tree,
            self.have_order_col,
        )
    }
}
