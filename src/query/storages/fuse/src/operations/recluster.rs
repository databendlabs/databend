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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_inc_recluster_build_task_milliseconds;
use databend_common_metrics::storage::metrics_inc_recluster_segment_nums_scheduled;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ClusterType;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use log::warn;
use opendal::Operator;

use crate::FuseTable;
use crate::SegmentLocation;
use crate::operations::ReclusterMutator;
use crate::operations::SelectedReclusterSegment;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;
use crate::pruning::create_segment_location_vector;

impl FuseTable {
    #[async_backtrace::framed]
    pub(crate) async fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let start = Instant::now();

        // Status.
        {
            let status = "[FUSE-RECLUSTER] Starting recluster operation";
            ctx.set_status_info(status);
        }

        let cluster_type = self.cluster_type();
        if cluster_type.is_none_or(|v| v != ClusterType::Linear) {
            return Ok(None);
        }

        let Some(snapshot) = self.read_table_snapshot().await? else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let mutator = Arc::new(ReclusterMutator::try_create(
            self,
            ctx.clone(),
            snapshot.as_ref(),
        )?);

        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let segment_limit = limit.unwrap_or(1000);
        // The default limit might be too small, which makes
        // the scanning of recluster candidates slow.
        let chunk_size = segment_limit.max(max_threads * 4);
        // The max number of segments to be reclustered.
        let max_seg_num = segment_limit.min(max_threads * 2);

        let mut recluster_seg_num = 0;
        let mut recluster_blocks_count = 0;
        let mut parts = ReclusterParts::default();

        let number_segments = segment_locations.len();
        let mut segment_idx = 0;
        for chunk in segment_locations.chunks(chunk_size) {
            let mut selected_seg_num = 0;
            // read segments.
            let compact_segments = Self::segment_pruning(
                &ctx,
                self.schema_with_stream(),
                self.get_operator(),
                &push_downs,
                chunk.to_vec(),
            )
            .await?;

            // Status.
            {
                segment_idx += chunk.len();
                let status = format!(
                    "[FUSE-RECLUSTER] Read segment files: {}/{}, elapsed: {:?}",
                    segment_idx,
                    number_segments,
                    start.elapsed()
                );
                ctx.set_status_info(&status);
            }

            if compact_segments.is_empty() {
                continue;
            }

            // select the segments with the highest depth.
            let selected_segs = mutator.select_segments(&compact_segments, max_seg_num)?;
            // select the blocks with the highest depth.
            if selected_segs.is_empty() {
                let result =
                    Self::generate_recluster_parts(mutator.clone(), compact_segments).await?;
                if let Some((seg_num, block_num, recluster_parts)) = result {
                    selected_seg_num = seg_num;
                    recluster_blocks_count = block_num;
                    parts = recluster_parts;
                }
            } else {
                selected_seg_num = selected_segs.len() as u64;
                (recluster_blocks_count, parts) = mutator.target_select(selected_segs).await?;
            }

            if !parts.is_empty() || limit.is_some() {
                recluster_seg_num = selected_seg_num;
                break;
            }
        }

        {
            let elapsed_time = start.elapsed();
            ctx.set_status_info(&format!(
                "[FUSE-RECLUSTER] Built recluster tasks - segments: {}, blocks: {}, elapsed: {:?}",
                recluster_seg_num, recluster_blocks_count, elapsed_time,
            ));
            metrics_inc_recluster_build_task_milliseconds(elapsed_time.as_millis() as u64);
            metrics_inc_recluster_segment_nums_scheduled(recluster_seg_num);
        }

        Ok(Some((parts, snapshot)))
    }

    pub async fn generate_recluster_parts(
        mutator: Arc<ReclusterMutator>,
        compact_segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    ) -> Result<Option<(u64, u64, ReclusterParts)>> {
        let mut selected_segs = vec![];
        let mut block_count = 0;

        let max_threads = mutator.ctx.get_settings().get_max_threads()? as usize;
        let mut segment_batches = Vec::new();

        let latest = compact_segments.len() - 1;
        for (idx, compact_segment) in compact_segments.into_iter().enumerate() {
            let segment =
                SelectedReclusterSegment::create(&mutator, compact_segment.0, compact_segment.1);
            if !(segment.stats.level >= 0
                || (segment.info.summary.block_count as usize)
                    < mutator.block_thresholds.block_per_segment)
            {
                continue;
            }

            block_count += segment.info.summary.block_count as usize;
            selected_segs.push(segment);
            if block_count >= mutator.block_thresholds.block_per_segment || idx == latest {
                segment_batches.push(std::mem::take(&mut selected_segs));
                block_count = 0;
            }
        }

        if segment_batches.is_empty() {
            return Ok(None);
        }

        let evaluate_batch = |selected_segs: Vec<SelectedReclusterSegment>| {
            let mutator = mutator.clone();
            async move {
                let seg_num = selected_segs.len() as u64;
                let (block_num, parts) = mutator.target_select(selected_segs).await?;
                Ok::<_, databend_common_exception::ErrorCode>((seg_num, block_num, parts))
            }
        };

        if segment_batches.len() == 1 {
            let selected_segs = segment_batches.pop().unwrap();
            let (seg_num, block_num, parts) = evaluate_batch(selected_segs).await?;
            return Ok((!parts.is_empty()).then_some((seg_num, block_num, parts)));
        }

        let concurrency = max_threads.min(segment_batches.len());
        let mut batches = segment_batches.into_iter();
        let mut pending = FuturesUnordered::new();

        for _ in 0..concurrency {
            if let Some(selected_segs) = batches.next() {
                pending.push(evaluate_batch(selected_segs));
            }
        }

        while let Some(result) = pending.next().await {
            let (seg_num, block_num, parts) = result?;
            if !parts.is_empty() {
                return Ok(Some((seg_num, block_num, parts)));
            }

            if let Some(selected_segs) = batches.next() {
                pending.push(evaluate_batch(selected_segs));
            }
        }

        Ok(None)
    }

    pub async fn segment_pruning(
        ctx: &Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        mut segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        let max_concurrency = {
            let max_threads = ctx.get_settings().get_max_threads()? as usize;
            let v = std::cmp::max(max_threads, 10);
            if v > max_threads {
                warn!(
                    "[FUSE-RECLUSTER] max_threads setting too low {}, increased to {}",
                    max_threads, v
                )
            }
            v
        };

        // during re-cluster, we do not rebuild missing bloom index
        let bloom_index_builder = None;
        // Only use push_down here.
        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            schema.clone(),
            push_down,
            None,
            vec![],
            BloomIndexColumns::None,
            vec![],
            HashSet::new(),
            max_concurrency,
            bloom_index_builder,
        )?;

        let segment_pruner =
            SegmentPruner::create(pruning_ctx.clone(), schema, Default::default())?;
        let mut remain = segment_locs.len() % max_concurrency;
        let batch_size = segment_locs.len() / max_concurrency;
        let mut works = Vec::with_capacity(max_concurrency);

        while !segment_locs.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
            works.push(pruning_ctx.pruning_runtime.spawn({
                let segment_pruner = segment_pruner.clone();

                async move {
                    let pruned_segments = segment_pruner.pruning(batch).await?;
                    Result::<_>::Ok(pruned_segments)
                }
            }));
        }

        let mut metas = vec![];
        let workers = futures::future::try_join_all(works).await?;
        for worker in workers {
            let res = worker?;
            metas.extend(res);
        }

        Ok(metas)
    }
}
