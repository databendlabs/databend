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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::pruning::BlockPruner;
use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;

pub struct AsyncBlockPruneTransform {
    pub block_pruner: Arc<BlockPruner>,
}

impl AsyncBlockPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_pruner: Arc<BlockPruner>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            AsyncBlockPruneTransform { block_pruner },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for AsyncBlockPruneTransform {
    const NAME: &'static str = "AsyncBlockPruneTransform";

    async fn transform(&mut self, mut data: DataBlock) -> Result<Option<DataBlock>> {
        if let Some(ptr) = data.take_meta() {
            if let Some(meta) = BlockMetasMeta::downcast_from(ptr) {
                let block_meta_indexes =
                    self.block_pruner.internal_column_pruning(&meta.block_metas);

                let result: Vec<(BlockMetaIndex, Arc<BlockMeta>)> = self
                    .block_pruner
                    .block_pruning(meta.segment_location, meta.block_metas, block_meta_indexes)
                    .await?;

                if result.is_empty() {
                    return Ok(None);
                }

                return Ok(Some(DataBlock::empty_with_meta(BlockPruneResult::create(
                    result,
                ))));
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast meta to BlockMetasMeta",
        ))
    }
}
