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
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;

use crate::pruning::BlockPruner;
use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;

pub struct SyncBlockPruneTransform {
    block_pruner: Arc<BlockPruner>,
}

impl SyncBlockPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_pruner: Arc<BlockPruner>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, SyncBlockPruneTransform {
                block_pruner,
            }),
        ))
    }
}

impl BlockMetaAccumulatingTransform<BlockMetasMeta> for SyncBlockPruneTransform {
    const NAME: &'static str = "SyncBlockPruneTransform";

    fn transform(&mut self, data: BlockMetasMeta) -> Result<Option<DataBlock>> {
        let block_meta_indexes = self.block_pruner.internal_column_pruning(&data.block_metas);

        let result = self.block_pruner.block_pruning_sync(
            data.segment_location,
            data.block_metas,
            block_meta_indexes,
        )?;
        if result.is_empty() {
            return Ok(None);
        }

        Ok(Some(DataBlock::empty_with_meta(BlockPruneResult::create(
            result,
        ))))
    }
}
