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

use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_cache::CountableMeter;
use common_catalog::plan::block_id_in_segment;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_expression::BLOCK_NAME_COL_NAME;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use common_pipeline_transforms::processors::transforms::AccumulatingTransformer;
use common_pipeline_transforms::processors::transforms::AsyncTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransform;
use common_pipeline_transforms::processors::transforms::BlockMetaAccumulatingTransformer;
use common_pipeline_transforms::processors::transforms::Transform;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_pruner::BlockMetaIndex;
use storages_common_pruner::Limiter;
use storages_common_pruner::PagePruner;
use storages_common_pruner::RangePruner;

use crate::index_analyzer::block_filter_meta::BlockFilterMeta;
use crate::index_analyzer::segment_info_meta::CompactSegmentInfoMeta;
use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::index_analyzer::segment_result_meta::SegmentFilterResult;
use crate::io::MetaReaders;
use crate::metrics::metrics_inc_bytes_segment_range_pruning_after;
use crate::metrics::metrics_inc_bytes_segment_range_pruning_before;
use crate::metrics::metrics_inc_segments_range_pruning_after;
use crate::metrics::metrics_inc_segments_range_pruning_before;
use crate::pruning::BlockMetaPruner;
use crate::pruning::FusePruningStatistics;
use crate::pruning::InternalBlockMetaPruner;
use crate::pruning::LimitBlockMetaPruner;
use crate::pruning::PruningContext;
use crate::pruning::RangeBlockMetaPruner;

pub struct BlockRangeFilterTransform {
    pruner: Arc<dyn BlockMetaPruner>,
    page_pruner: Arc<dyn PagePruner + Send + Sync>,
}

impl BlockRangeFilterTransform {
    pub fn create(
        ctx: Arc<PruningContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let block_meta_pruner =
            LimitBlockMetaPruner::create(&ctx, RangeBlockMetaPruner::create(&ctx));

        match &ctx.internal_column_pruner {
            None => Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                input,
                output,
                BlockRangeFilterTransform {
                    pruner: Arc::new(block_meta_pruner),
                    page_pruner: ctx.page_pruner.clone(),
                },
            ))),
            Some(internal_column_pruner) => {
                let block_meta_pruner = InternalBlockMetaPruner::create(
                    block_meta_pruner,
                    internal_column_pruner.clone(),
                );

                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    input,
                    output,
                    BlockRangeFilterTransform {
                        pruner: Arc::new(block_meta_pruner),
                        page_pruner: ctx.page_pruner.clone(),
                    },
                )))
            }
        }
    }
}

impl AccumulatingTransform for BlockRangeFilterTransform {
    const NAME: &'static str = "BlockRangeFilterTransform";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(meta) = SegmentFilterResult::downcast_from(data.take_meta().unwrap()) {
            let mut filter_mask = vec![true; meta.metas.len()];
            self.pruner.pruning(&meta.metas, &mut filter_mask)?;

            let size = filter_mask.len();
            let mut new_blocks = Vec::with_capacity(meta.metas.len());
            for (idx, mask) in filter_mask.into_iter().enumerate() {
                if !mask {
                    continue;
                }

                let block_meta = &meta.metas[idx];
                let block_stats = &block_meta.cluster_stats;
                let location = Arc::new(meta.segment_location.clone());
                if let (true, range) = self.page_pruner.should_keep(block_stats) {
                    new_blocks.push(DataBlock::empty_with_meta(BlockFilterMeta::create(
                        location,
                        block_meta.clone(),
                        BlockMetaIndex {
                            range,
                            block_idx: idx,
                            segment_idx: meta.segment_location.segment_idx,
                            page_size: block_meta.page_size() as usize,
                            block_id: block_id_in_segment(size, idx),
                            block_location: block_meta.as_ref().location.0.clone(),
                            segment_location: meta.segment_location.location.0.clone(),
                            snapshot_location: meta.segment_location.snapshot_loc.clone(),
                        },
                    )));
                }
            }

            return Ok(new_blocks);
        }

        Err(ErrorCode::Internal(
            "BlockRangeFilterTransform only recv SegmentFilterResult",
        ))
    }
}
