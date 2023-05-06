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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
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

use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationSinkMeta;
use crate::operations::mutation::MutationTransformMeta;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

type MutationMap = HashMap<usize, (Vec<(usize, Arc<BlockMeta>)>, Vec<usize>)>;

struct SerializedData {
    data: Vec<u8>,
    location: String,
    segment: CompactSegmentInfo,
}

pub struct MutationAggregator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    base_segments: Vec<Location>,
    thresholds: BlockThresholds,
    abort_operation: AbortOperation,

    input_metas: MutationMap,

    start_time: Instant,
    total_tasks: usize,
    finished_tasks: usize,
}

impl MutationAggregator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        base_segments: Vec<Location>,
        thresholds: BlockThresholds,
        total_tasks: usize,
    ) -> Self {
        Self {
            ctx,
            schema,
            dal,
            location_gen,
            base_segments,
            thresholds,
            abort_operation: AbortOperation::default(),
            input_metas: HashMap::new(),
            start_time: Instant::now(),
            total_tasks,
            finished_tasks: 0,
        }
    }

    #[async_backtrace::framed]
    async fn write_segments(&self, serialized_data: Vec<SerializedData>) -> Result<()> {
        let mut tasks = Vec::with_capacity(serialized_data.len());
        for serialized in serialized_data {
            let op = self.dal.clone();
            tasks.push(async move {
                op.write(&serialized.location, serialized.data).await?;
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    segment_cache.put(serialized.location.clone(), Arc::new(serialized.segment));
                }
                Ok::<_, ErrorCode>(())
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "mutation-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for MutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        // gather the input data.
        if let Some(meta) = data
            .get_meta()
            .and_then(MutationTransformMeta::downcast_ref_from)
        {
            self.finished_tasks += 1;
            match &meta.op {
                Mutation::Replaced(block_meta) => {
                    self.input_metas
                        .entry(meta.index.segment_idx)
                        .and_modify(|v| v.0.push((meta.index.block_idx, block_meta.clone())))
                        .or_insert((vec![(meta.index.block_idx, block_meta.clone())], vec![]));
                    self.abort_operation.add_block(block_meta);
                }
                Mutation::Deleted => {
                    self.input_metas
                        .entry(meta.index.segment_idx)
                        .and_modify(|v| v.1.push(meta.index.block_idx))
                        .or_insert((vec![], vec![meta.index.block_idx]));
                }
                Mutation::DoNothing => (),
            }

            // Refresh status
            {
                let status = format!(
                    "mutation: run tasks:{}/{}, cost:{} sec",
                    self.finished_tasks,
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
        // Read all segments information in parallel.
        let segments_io =
            SegmentsIO::create(self.ctx.clone(), self.dal.clone(), self.schema.clone());
        let segment_locations = self.base_segments.as_slice();
        let segment_infos = segments_io
            .read_segments(segment_locations, true)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        let segments = self.base_segments.clone();
        let mut summary = Statistics::default();
        let mut serialized_data = Vec::with_capacity(self.input_metas.len());
        let mut segments_editor = BTreeMap::<_, _>::from_iter(segments.into_iter().enumerate());
        for (seg_idx, seg_info) in segment_infos.iter().enumerate() {
            if let Some((replaced, deleted)) = self.input_metas.get(&seg_idx) {
                // prepare the new segment
                let mut new_segment =
                    SegmentInfo::new(seg_info.blocks.clone(), seg_info.summary.clone());
                // take away the blocks, they are being mutated
                let mut block_editor = BTreeMap::<_, _>::from_iter(
                    std::mem::take(&mut new_segment.blocks)
                        .into_iter()
                        .enumerate(),
                );

                for (idx, new_meta) in replaced {
                    block_editor.insert(*idx, new_meta.clone());
                }
                for idx in deleted {
                    block_editor.remove(idx);
                }
                // assign back the mutated blocks to segment
                new_segment.blocks = block_editor.into_values().collect();
                if new_segment.blocks.is_empty() {
                    segments_editor.remove(&seg_idx);
                } else {
                    // re-calculate the segment statistics
                    let new_summary = reduce_block_metas(&new_segment.blocks, self.thresholds)?;
                    merge_statistics_mut(&mut summary, &new_summary)?;
                    new_segment.summary = new_summary;

                    let location = self.location_gen.gen_segment_info_location();
                    self.abort_operation.add_segment(location.clone());
                    segments_editor.insert(seg_idx, (location.clone(), SegmentInfo::VERSION));
                    serialized_data.push(SerializedData {
                        data: new_segment.to_bytes()?,
                        location,
                        segment: Arc::new(new_segment),
                    });
                }
            } else {
                merge_statistics_mut(&mut summary, &seg_info.summary)?;
            }
        }

        // assign back the mutated segments to snapshot
        let segments = segments_editor.into_values().collect();
        self.write_segments(serialized_data).await?;
        let meta =
            MutationSinkMeta::create(segments, summary, std::mem::take(&mut self.abort_operation));

        Ok(Some(DataBlock::empty_with_meta(meta)))
    }
}
