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
use log::debug;
use log::warn;
use opendal::Operator;

use crate::FuseTable;
use crate::SegmentLocation;
use crate::operations::ReclusterMutator;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;
use crate::pruning::create_segment_location_vector;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclusterMode {
    Normal,
    Final,
}

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
        mode: ReclusterMode,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let start = Instant::now();

        ctx.set_status_info("[FUSE-RECLUSTER] Starting recluster operation");

        if self.cluster_type().is_none_or(|v| v != ClusterType::Linear) {
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

        let segment_locations = create_segment_location_vector(snapshot.segments.clone(), None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let segment_limit = limit.unwrap_or(1000);
        let number_segments = segment_locations.len();
        // The default limit might be too small, which makes
        // the scanning of recluster candidates slow.
        let chunk_size = segment_limit.max(max_threads * 4);
        // The max number of segments to be reclustered.
        let max_seg_num = segment_limit.min(max_threads * 2);

        let mut recluster_seg_num = 0;
        let mut recluster_blocks_count = 0;
        let mut parts = ReclusterParts::default();

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

            debug!(
                "recluster: scanned segment chunk chunk_segments={} compact_segments={} segment_progress={}/{}",
                chunk.len(),
                compact_segments.len(),
                segment_idx + chunk.len(),
                number_segments,
            );

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
                debug!(
                    "recluster: build tasks skipped chunk_segments={} skip_reason=empty_compact_segments",
                    chunk.len(),
                );
                continue;
            }

            // Select segment windows with the highest depth.
            let segment_windows = mutator.select_segments(&compact_segments, max_seg_num)?;
            debug!(
                "recluster: selected segment windows compact_segments={} window_count={} max_segments={}",
                compact_segments.len(),
                segment_windows.len(),
                max_seg_num,
            );
            // Select the blocks with the highest depth.
            for selected_segs in segment_windows {
                let candidate_seg_num = selected_segs.len();
                let (block_num, recluster_parts) =
                    mutator.target_select(selected_segs, mode).await?;
                let seg_num = recluster_parts.removed_segment_indexes.len() as u64;
                if !recluster_parts.is_empty() {
                    debug!(
                        "recluster: built parts candidate_segments={} selected_segments={} blocks={} tasks={}",
                        candidate_seg_num,
                        seg_num,
                        block_num,
                        recluster_parts.tasks.len(),
                    );
                    selected_seg_num = seg_num;
                    recluster_blocks_count = block_num;
                    parts = recluster_parts;
                    break;
                }
            }

            if !parts.is_empty() || limit.is_some() {
                recluster_seg_num = selected_seg_num;
                break;
            }
        }

        {
            let elapsed_time = start.elapsed();
            ctx.set_status_info(&format!(
                "[FUSE-RECLUSTER] Built recluster tasks: segments={} blocks={} elapsed={:?}",
                recluster_seg_num, recluster_blocks_count, elapsed_time,
            ));
            metrics_inc_recluster_build_task_milliseconds(elapsed_time.as_millis() as u64);
            metrics_inc_recluster_segment_nums_scheduled(recluster_seg_num);
        }

        Ok(Some((parts, snapshot)))
    }

    pub async fn segment_pruning(
        ctx: &Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        mut segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrency = std::cmp::max(max_threads, 10);
        if max_concurrency > max_threads {
            warn!(
                "recluster: max_threads setting too low {}, increased to {}",
                max_threads, max_concurrency
            );
        }

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
