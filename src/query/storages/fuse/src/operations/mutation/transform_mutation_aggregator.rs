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
use common_expression::TableSchemaRef;
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
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationSinkMeta;
use crate::operations::mutation::MutationTransformMeta;
use crate::statistics::reducers::deduct_statistics_mut;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

type MutationMap = HashMap<usize, (Vec<(usize, Arc<BlockMeta>)>, Vec<usize>)>;

struct SegmentLite {
    // segment index.
    index: usize,
    // new segment location and summary.
    new_segment_info: Option<(String, Statistics)>,
    // origin segment summary.
    origin_summary: Statistics,
}

pub struct MutationAggregator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    base_segments: Vec<Location>,
    merged_statistics: Statistics,
    thresholds: BlockThresholds,
    abort_operation: AbortOperation,

    input_metas: MutationMap,

    start_time: Instant,
    total_tasks: usize,
    finished_tasks: usize,
}

impl MutationAggregator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        merged_statistics: Statistics,
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
            merged_statistics,
            thresholds,
            abort_operation: AbortOperation::default(),
            input_metas: HashMap::new(),
            start_time: Instant::now(),
            total_tasks,
            finished_tasks: 0,
        }
    }

    // read the segment info, replace the blocks, and write the new segment info.
    #[async_backtrace::framed]
    async fn generate_segments(&mut self, segment_indices: Vec<usize>) -> Result<Vec<SegmentLite>> {
        let thresholds = self.thresholds;
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let (replaced, deleted) = self.input_metas.remove(&index).unwrap();
            let location = self.base_segments[index].clone();
            let op = self.dal.clone();
            let schema = self.schema.clone();
            let location_gen = self.location_gen.clone();

            tasks.push(async move {
                // read the old segment
                let segment_info =
                    SegmentsIO::read_segment(op.clone(), location, schema, false).await?;
                // prepare the new segment
                let mut new_segment =
                    SegmentInfo::new(segment_info.blocks.clone(), segment_info.summary.clone());
                // take away the blocks, they are being mutated
                let mut block_editor = BTreeMap::<_, _>::from_iter(
                    std::mem::take(&mut new_segment.blocks)
                        .into_iter()
                        .enumerate(),
                );

                for (idx, new_meta) in replaced {
                    block_editor.insert(idx, new_meta);
                }
                for idx in deleted {
                    block_editor.remove(&idx);
                }

                // assign back the mutated blocks to segment
                new_segment.blocks = block_editor.into_values().collect();
                if !new_segment.blocks.is_empty() {
                    // re-calculate the segment statistics
                    let new_summary = reduce_block_metas(&new_segment.blocks, thresholds)?;
                    new_segment.summary = new_summary.clone();

                    // write the segment info.
                    let location = location_gen.gen_segment_info_location();
                    let serialized_segment = SerializedSegment {
                        path: location.clone(),
                        segment: Arc::new(new_segment),
                    };
                    SegmentsIO::write_segment(op, serialized_segment).await?;

                    Ok(SegmentLite {
                        index,
                        new_segment_info: Some((location, new_summary)),
                        origin_summary: segment_info.summary.clone(),
                    })
                } else {
                    Ok(SegmentLite {
                        index,
                        new_segment_info: None,
                        origin_summary: segment_info.summary.clone(),
                    })
                }
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
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
        let mut recalc_stats = false;
        if self.input_metas.len() == self.base_segments.len() {
            self.merged_statistics = Statistics::default();
            recalc_stats = true;
        }

        let start = Instant::now();
        let mut count = 0;

        let segment_locations = self.base_segments.clone();
        let mut segments_editor =
            BTreeMap::<_, _>::from_iter(segment_locations.into_iter().enumerate());

        let chunk_size = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let segment_indices = self.input_metas.keys().cloned().collect::<Vec<_>>();
        for chunk in segment_indices.chunks(chunk_size) {
            let results = self.generate_segments(chunk.to_vec()).await?;
            for result in results {
                if let Some((location, summary)) = result.new_segment_info {
                    // replace the old segment location with the new one.
                    self.abort_operation.add_segment(location.clone());
                    segments_editor.insert(result.index, (location.clone(), SegmentInfo::VERSION));
                    merge_statistics_mut(&mut self.merged_statistics, &summary)?;
                } else {
                    // remove the old segment location.
                    segments_editor.remove(&result.index);
                }

                if !recalc_stats {
                    // deduct the old segment summary from the merged summary.
                    deduct_statistics_mut(&mut self.merged_statistics, &result.origin_summary);
                }
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "mutation: generate new segment files:{}/{}, cost:{} sec",
                    count,
                    segment_indices.len(),
                    start.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
                info!(status);
            }
        }

        // assign back the mutated segments to snapshot
        let segments = segments_editor.into_values().collect::<Vec<_>>();
        if segments.is_empty() {
            self.merged_statistics = Statistics::default();
        }
        let meta = MutationSinkMeta::create(
            segments,
            self.merged_statistics.clone(),
            self.abort_operation.clone(),
        );

        Ok(Some(DataBlock::empty_with_meta(meta)))
    }
}
