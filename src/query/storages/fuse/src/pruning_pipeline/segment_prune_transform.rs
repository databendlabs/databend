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
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;

use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;
use crate::pruning_pipeline::pruned_segment_meta::PrunedSegmentMeta;
use crate::pruning_pipeline::LazySegmentMeta;

pub struct SegmentPruneTransform {
    pub segment_pruner: Arc<SegmentPruner>,
    pub pruning_ctx: Arc<PruningContext>,
}

impl SegmentPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        segment_pruner: Arc<SegmentPruner>,
        pruning_context: Arc<PruningContext>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            SegmentPruneTransform {
                segment_pruner,
                pruning_ctx: pruning_context,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for SegmentPruneTransform {
    const NAME: &'static str = "SegmentPruneTransform";

    async fn transform(&mut self, mut data: DataBlock) -> Result<Option<DataBlock>> {
        if let Some(ptr) = data.take_meta() {
            if let Some(meta) = LazySegmentMeta::downcast_from(ptr) {
                let location = meta.segment_location;
                if let Some(pruner) = &self.pruning_ctx.internal_column_pruner {
                    if !pruner.should_keep(SEGMENT_NAME_COL_NAME, &location.location.0) {
                        return Ok(None);
                    }
                }
                let mut pruned_segments = self.segment_pruner.pruning(vec![location]).await?;

                if pruned_segments.is_empty() {
                    return Ok(None);
                }

                debug_assert!(pruned_segments.len() == 1);

                return Ok(Some(DataBlock::empty_with_meta(PrunedSegmentMeta::create(
                    pruned_segments.pop().unwrap(),
                ))));
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast meta to LazySegmentMeta",
        ))
    }
}
