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

use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::CompactSegmentInfo;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use tracing::info;
use tracing::Instrument;

use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::compact::CompactSourceMeta;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::MutationSinkMeta;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

#[derive(Clone)]
struct SerializedSegment {
    location: String,
    segment: Arc<SegmentInfo>,
}

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

    #[async_backtrace::framed]
    async fn write_segment(dal: Operator, serialized_segment: SerializedSegment) -> Result<()> {
        assert_eq!(
            serialized_segment.segment.format_version,
            SegmentInfo::VERSION
        );
        let raw_bytes = serialized_segment.segment.to_bytes()?;
        let compact_segment_info = CompactSegmentInfo::from_slice(&raw_bytes)?;
        dal.write(&serialized_segment.location, raw_bytes).await?;
        if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
            segment_cache.put(serialized_segment.location, Arc::new(compact_segment_info));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn write_segments(&self, segments: Vec<SerializedSegment>) -> Result<()> {
        let mut iter = segments.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|segment| {
                Self::write_segment(self.dal.clone(), segment.clone())
                    .instrument(tracing::debug_span!("write_segment"))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "compact-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
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
                location,
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
        // write segments.
        self.write_segments(serialized_segments).await?;

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
        let meta = MutationSinkMeta::create(
            merged_segments,
            std::mem::take(&mut self.merged_statistics),
            std::mem::take(&mut self.abort_operation),
        );
        Ok(Some(DataBlock::empty_with_meta(meta)))
    }
}
