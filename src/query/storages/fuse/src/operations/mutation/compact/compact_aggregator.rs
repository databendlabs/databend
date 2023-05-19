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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRefExt;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use tracing::info;

use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::merge_into::mutation_meta::CommitMeta;
use crate::operations::mutation::compact::CompactSourceMeta;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::BlockCompactMutator;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

pub struct CompactAggregator {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    // locations all the merged segments.
    merged_segments: BTreeMap<usize, Location>,
    // summarised statistics of all the merged segments
    merged_statistics: Statistics,
    // locations all the merged blocks.
    merge_blocks: HashMap<usize, BTreeMap<usize, Arc<BlockMeta>>>,
    thresholds: BlockThresholds,
    abort_operation: AbortOperation,

    start_time: Instant,
    total_tasks: usize,
}

impl CompactAggregator {
    pub fn new(
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        mutator: BlockCompactMutator,
    ) -> Self {
        Self {
            ctx: mutator.ctx.clone(),
            dal,
            location_gen,
            merged_segments: mutator.unchanged_segments_map,
            merged_statistics: mutator.unchanged_segment_statistics,
            merge_blocks: mutator.unchanged_blocks_map,
            thresholds: mutator.thresholds,
            abort_operation: AbortOperation::default(),
            start_time: Instant::now(),
            total_tasks: mutator.compact_tasks.len(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for CompactAggregator {
    const NAME: &'static str = "CompactAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        // gather the input data.
        if let Some(meta) = data
            .get_meta()
            .and_then(CompactSourceMeta::downcast_ref_from)
        {
            self.abort_operation.add_block(&meta.block);
            self.merge_blocks
                .entry(meta.index.segment_idx)
                .and_modify(|v| {
                    v.insert(meta.index.block_idx, meta.block.clone());
                })
                .or_insert(BTreeMap::from([(meta.index.block_idx, meta.block.clone())]));

            // Refresh status
            {
                let status = format!(
                    "compact: run compact tasks:{}/{}, cost:{} sec",
                    self.abort_operation.blocks.len(),
                    self.total_tasks,
                    self.start_time.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
                info!(status);
            }
        }
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mut serialized_segments = Vec::with_capacity(self.merge_blocks.len());
        for (segment_idx, block_map) in std::mem::take(&mut self.merge_blocks) {
            // generate the new segment.
            let blocks: Vec<_> = block_map.into_values().collect();
            let new_summary = reduce_block_metas(&blocks, self.thresholds)?;
            merge_statistics_mut(&mut self.merged_statistics, &new_summary)?;
            let new_segment = SegmentInfo::new(blocks, new_summary);
            let location = self.location_gen.gen_segment_info_location();
            self.abort_operation.add_segment(location.clone());
            self.merged_segments
                .insert(segment_idx, (location.clone(), SegmentInfo::VERSION));
            serialized_segments.push(SerializedSegment {
                path: location,
                segment: Arc::new(new_segment),
            });
        }

        let start = Instant::now();
        // Refresh status
        {
            let status = format!(
                "compact: begin to write new segments:{}",
                serialized_segments.len()
            );
            self.ctx.set_status_info(&status);
            info!(status);
        }
        // write segments, schema in segments_io is useless here.
        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.dal.clone(),
            TableSchemaRefExt::create(vec![]),
        );
        segments_io.write_segments(serialized_segments).await?;

        // Refresh status
        {
            let status = format!(
                "compact: end to write new segments, cost:{} sec",
                start.elapsed().as_secs()
            );
            self.ctx.set_status_info(&status);
            info!(status);
        }
        // gather the all segments.
        let merged_segments = std::mem::take(&mut self.merged_segments)
            .into_values()
            .collect();
        let meta = CommitMeta::new(
            merged_segments,
            std::mem::take(&mut self.merged_statistics),
            std::mem::take(&mut self.abort_operation),
            true,
        );
        Ok(Some(DataBlock::empty_with_meta(Box::new(meta))))
    }
}
