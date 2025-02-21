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

use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;

use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;
use crate::pruning_pipeline::pruned_segment_meta::PrunedCompactSegmentMeta;

pub struct ExtractSegmentTransform {
    populate_cache: bool,
}

impl ExtractSegmentTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        populate_cache: bool,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, ExtractSegmentTransform {
                populate_cache,
            }),
        ))
    }
}

impl BlockMetaAccumulatingTransform<PrunedCompactSegmentMeta> for ExtractSegmentTransform {
    const NAME: &'static str = "ExtractSegmentTransform";

    fn transform(
        &mut self,
        data: PrunedCompactSegmentMeta,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        let (segment_location, info) = data.segments;

        let block_metas =
            Self::extract_block_metas(&segment_location.location.0, &info, self.populate_cache)?;

        if block_metas.is_empty() {
            return Ok(None);
        };

        Ok(Some(DataBlock::empty_with_meta(BlockMetasMeta::create(
            block_metas,
            segment_location,
        ))))
    }
}

impl ExtractSegmentTransform {
    fn extract_block_metas(
        segment_path: &str,
        segment: &CompactSegmentInfo,
        populate_cache: bool,
    ) -> databend_common_exception::Result<Arc<Vec<Arc<BlockMeta>>>> {
        if let Some(cache) = CacheManager::instance().get_segment_block_metas_cache() {
            if let Some(metas) = cache.get(segment_path) {
                Ok(metas)
            } else {
                match populate_cache {
                    true => Ok(cache.insert(segment_path.to_string(), segment.block_metas()?)),
                    false => Ok(Arc::new(segment.block_metas()?)),
                }
            }
        } else {
            Ok(Arc::new(segment.block_metas()?))
        }
    }
}
