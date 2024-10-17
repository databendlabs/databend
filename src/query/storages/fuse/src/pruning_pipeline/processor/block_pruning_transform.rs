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

use databend_common_catalog::plan::block_id_in_segment;
use databend_common_expression::DataBlock;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::pruning::PruningContext;
use crate::pruning_pipeline::meta_info::BlockPruningResult;
use crate::pruning_pipeline::meta_info::ExtractSegmentResult;

/// BlockPruningTransform Workflow
/// 1. Internal column pruning at block level
/// 2. Range pruning at block level
/// 3. Page pruning at block level
pub struct BlockPruningTransform {
    pub pruning_ctx: Arc<PruningContext>,
}

impl BlockPruningTransform {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, BlockPruningTransform {
                pruning_ctx,
            }),
        ))
    }
}

impl BlockMetaAccumulatingTransform<ExtractSegmentResult> for BlockPruningTransform {
    const NAME: &'static str = "BlockPruningTransform";

    fn transform(
        &mut self,
        meta: ExtractSegmentResult,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let range_pruner = self.pruning_ctx.range_pruner.clone();
        let page_pruner = self.pruning_ctx.page_pruner.clone();

        let block_meta_indexes = self.internal_column_pruning(&meta.block_metas);
        let mut result = Vec::with_capacity(block_meta_indexes.len());
        let block_num = meta.block_metas.len();
        for (block_idx, block_meta) in block_meta_indexes {
            // check limit speculatively
            if limit_pruner.exceeded() {
                break;
            }
            let row_count = block_meta.row_count;
            if range_pruner.should_keep(&block_meta.col_stats, Some(&block_meta.col_metas))
                && limit_pruner.within_limit(row_count)
            {
                let (keep, range) = page_pruner.should_keep(&block_meta.cluster_stats);
                if keep {
                    result.push((
                        Some(BlockMetaIndex {
                            segment_idx: meta.segment_location.segment_idx,
                            block_idx,
                            range,
                            page_size: block_meta.page_size() as usize,
                            block_id: block_id_in_segment(block_num, block_idx),
                            block_location: block_meta.as_ref().location.0.clone(),
                            segment_location: meta.segment_location.location.0.clone(),
                            snapshot_location: meta.segment_location.snapshot_loc.clone(),
                            matched_rows: None,
                        }),
                        block_meta.clone(),
                    ))
                }
            }
        }
        Ok(Some(DataBlock::empty_with_meta(
            BlockPruningResult::create(result),
        )))
    }
}

impl BlockPruningTransform {
    /// Apply internal column pruning at block level
    fn internal_column_pruning(
        &self,
        block_metas: &[Arc<BlockMeta>],
    ) -> Vec<(usize, Arc<BlockMeta>)> {
        match &self.pruning_ctx.internal_column_pruner {
            Some(pruner) => block_metas
                .iter()
                .enumerate()
                .filter(|(_, block_meta)| {
                    pruner.should_keep(BLOCK_NAME_COL_NAME, &block_meta.location.0)
                })
                .map(|(index, block_meta)| (index, block_meta.clone()))
                .collect(),
            None => block_metas
                .iter()
                .enumerate()
                .map(|(index, block_meta)| (index, block_meta.clone()))
                .collect(),
        }
    }
}
