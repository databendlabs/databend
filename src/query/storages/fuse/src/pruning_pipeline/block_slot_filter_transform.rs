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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockSlotDescription;

use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;

/// Transform that filters blocks based on block-level shuffle assignment.
/// This happens early in the pipeline to avoid unnecessary processing of blocks
/// that don't belong to this executor.
pub struct BlockSlotFilterTransform {
    block_slot: Option<BlockSlotDescription>,
}

impl BlockSlotFilterTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_slot: Option<BlockSlotDescription>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            BlockSlotFilterTransform { block_slot },
        )))
    }

    /// Check if a block should be processed by this executor based on block-level shuffle.
    /// For BlockMod shuffle, each executor only processes blocks where block_idx % num_slots == slot.
    fn should_process_block(block_idx: usize, block_slot: &Option<BlockSlotDescription>) -> bool {
        match block_slot {
            Some(BlockSlotDescription { num_slots, slot }) => {
                block_idx % num_slots == *slot as usize
            }
            None => true,
        }
    }
}

impl Transform for BlockSlotFilterTransform {
    const NAME: &'static str = "BlockSlotFilterTransform";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        // If no block slot filtering is needed, pass through unchanged
        if self.block_slot.is_none() {
            return Ok(data);
        }

        // Extract BlockMetasMeta from the data block
        if let Some(block_metas_meta) = data
            .get_meta()
            .and_then(|meta| BlockMetasMeta::downcast_ref_from(meta))
        {
            let original_count = block_metas_meta.block_metas.len();

            let filtered_blocks: Vec<Arc<BlockMeta>> = block_metas_meta
                .block_metas
                .iter()
                .enumerate()
                .filter(|(block_idx, _block_meta)| {
                    // block_idx here is the index within the segment (0, 1, 2, ...)
                    // This matches the block_idx used in BlockPruner and SendPartInfoSink
                    Self::should_process_block(*block_idx, &self.block_slot)
                })
                .map(|(_, block_meta)| block_meta.clone())
                .collect();

            let filtered_count = filtered_blocks.len();

            // Log filtering results for debugging
            if let Some(BlockSlotDescription { num_slots, slot }) = &self.block_slot {
                log::debug!(
                    "BlockSlotFilterTransform: segment_idx={}, filtered {} -> {} blocks (slot={}/{})",
                    block_metas_meta.segment_location.segment_idx,
                    original_count,
                    filtered_count,
                    slot,
                    num_slots
                );
            }

            // If no blocks remain after filtering, return empty block
            if filtered_blocks.is_empty() {
                return Ok(DataBlock::empty());
            }

            // Create new BlockMetasMeta with filtered blocks
            let filtered_meta = BlockMetasMeta::create(
                Arc::new(filtered_blocks),
                block_metas_meta.segment_location.clone(),
            );

            // Return data block with filtered metadata
            return Ok(DataBlock::empty_with_meta(filtered_meta));
        }

        Ok(data)
    }
}
