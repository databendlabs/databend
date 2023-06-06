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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::{AsyncTransform, BlockMetaAccumulatingTransform, Transform};
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_pruner::RangePruner;
use crate::index_analyzer::segment_result_meta::SegmentFilterResult;

use crate::index_analyzer::segment_info_meta::CompactSegmentInfoMeta;
use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::io::MetaReaders;
use crate::metrics::{metrics_inc_bytes_segment_range_pruning_after, metrics_inc_bytes_segment_range_pruning_before, metrics_inc_segments_range_pruning_after, metrics_inc_segments_range_pruning_before};
use crate::pruning::FusePruningStatistics;

pub struct SegmentFilterTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    pruning_stats: Arc<FusePruningStatistics>,
    range_pruner: Arc<dyn RangePruner + Sync + Send>,
}

impl BlockMetaAccumulatingTransform<CompactSegmentInfoMeta> for SegmentFilterTransform {
    const NAME: &'static str = "SegmentFilterTransform";

    fn transform(&mut self, meta: CompactSegmentInfoMeta) -> Result<Option<DataBlock>> {
        let total_bytes = meta.info.summary.uncompressed_byte_size;
        // Perf.
        {
            metrics_inc_segments_range_pruning_before(1);
            metrics_inc_bytes_segment_range_pruning_before(total_bytes);

            self.pruning_stats.set_segments_range_pruning_before(1);
        }

        if self.range_pruner.should_keep(&meta.info.summary.col_stats, None) {
            // Perf.
            {
                metrics_inc_segments_range_pruning_after(1);
                metrics_inc_bytes_segment_range_pruning_after(total_bytes);

                self.pruning_stats.set_segments_range_pruning_after(1);
            }

            let blocks_meta = meta.info.block_metas()?;
            return Ok(Some(DataBlock::empty_with_meta(SegmentFilterResult::create(
                meta.segment_location,
                blocks_meta
            ))));
        }

        Ok(None)
    }
}
