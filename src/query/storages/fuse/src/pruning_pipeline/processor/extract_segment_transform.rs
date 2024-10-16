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
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;

use crate::pruning_pipeline::meta_info::CompactSegmentMeta;
use crate::pruning_pipeline::meta_info::ExtractSegmentResult;

/// ExtractSegmentTransform Workflow:
/// 1. Extract the pruned segment to blocks
pub struct ExtractSegmentTransform {}

impl ExtractSegmentTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, ExtractSegmentTransform {}),
        ))
    }
}

impl BlockMetaAccumulatingTransform<CompactSegmentMeta> for ExtractSegmentTransform {
    const NAME: &'static str = "ExtractSegmentTransform";

    fn transform(
        &mut self,
        data: CompactSegmentMeta,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        let populate_cache = true;
        let block_metas = if let Some(cache) = CacheManager::instance().get_block_meta_cache() {
            if let Some(metas) = cache.get(data.location.location.0.clone()) {
                Ok::<_, ErrorCode>(metas)
            } else {
                match populate_cache {
                    true => Ok(cache.insert(
                        data.location.location.0.to_string(),
                        data.compact_segment.block_metas()?,
                    )),
                    false => Ok(Arc::new(data.compact_segment.block_metas()?)),
                }
            }
        } else {
            Ok(Arc::new(data.compact_segment.block_metas()?))
        }?;
        Ok(Some(DataBlock::empty_with_meta(
            ExtractSegmentResult::create(block_metas, data.location.clone()),
        )))
    }
}
