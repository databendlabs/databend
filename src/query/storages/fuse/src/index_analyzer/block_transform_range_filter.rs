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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::{BLOCK_NAME_COL_NAME, BlockMetaInfoDowncast};
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::{AsyncTransform, BlockMetaAccumulatingTransform, Transform};
use opendal::Operator;
use common_cache::CountableMeter;
use common_catalog::plan::block_id_in_segment;
use storages_common_cache::LoadParams;
use storages_common_pruner::{BlockMetaIndex, Limiter, PagePruner, RangePruner};
use crate::index_analyzer::block_filter_meta::{BlockFilterMeta, BlocksFilterMeta};
use crate::index_analyzer::segment_result_meta::SegmentFilterResult;

use crate::index_analyzer::segment_info_meta::CompactSegmentInfoMeta;
use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::io::MetaReaders;
use crate::metrics::{metrics_inc_bytes_segment_range_pruning_after, metrics_inc_bytes_segment_range_pruning_before, metrics_inc_segments_range_pruning_after, metrics_inc_segments_range_pruning_before};
use crate::pruning::{BlockMetaPruner, FusePruningStatistics, InternalBlockMetaPruner, LimitBlockMetaPruner, RangeBlockMetaPruner};

pub struct BlockRangeFilterTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    pruner: Arc<dyn BlockMetaPruner>,
    page_pruner: Arc<dyn PagePruner + Send + Sync>,
}

impl BlockMetaAccumulatingTransform<SegmentFilterResult> for BlockRangeFilterTransform {
    const NAME: &'static str = "BlocksFilterTransform";

    fn transform(&mut self, meta: SegmentFilterResult) -> Result<Option<DataBlock>> {
        let mut filter_mask = vec![true; meta.metas.len()];
        self.pruner.pruning(&meta.metas, &mut filter_mask)?;

        let mut new_metas = Vec::with_capacity(meta.metas.len());
        for (idx, mask) in filter_mask.into_iter().enumerate() {
            if !mask {
                continue;
            }

            let block_meta = &meta.metas[idx];
            let block_stats = &block_meta.cluster_stats;
            if let (true, range) = self.page_pruner.should_keep(block_stats) {
                new_metas.push(BlockFilterMeta {
                    block_meta: block_meta.clone(),
                    meta_index: BlockMetaIndex {
                        range,
                        block_idx: idx,
                        segment_idx: meta.segment_location.segment_idx,
                        page_size: block_meta.page_size() as usize,
                        block_id: block_id_in_segment(filter_mask.len(), idx),
                        block_location: block_meta.as_ref().location.0.clone(),
                        segment_location: meta.segment_location.location.0.clone(),
                        snapshot_location: meta.segment_location.snapshot_loc.clone(),
                    },
                });
            }
        }

        match new_metas.is_empty() {
            true => Ok(None),
            false => Ok(Some(DataBlock::empty_with_meta(BlocksFilterMeta::create(
                meta.segment_location.clone(),
                new_metas,
            ))))
        }
    }
}
