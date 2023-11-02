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
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use std::vec;

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::ColumnId;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::CompactSegmentInfo;
use storages_common_table_meta::meta::Statistics;

use crate::io::SegmentsIO;
use crate::metrics::metrics_inc_compact_block_build_task_milliseconds;
use crate::operations::acquire_task_permit;
use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::compact::compact_part::CompactExtraInfo;
use crate::operations::mutation::compact::compact_part::CompactLazyPartInfo;
use crate::operations::mutation::compact::compact_part::CompactPartInfo;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::CompactTaskInfo;
use crate::operations::mutation::SegmentIndex;
use crate::operations::mutation::MAX_BLOCK_COUNT;
use crate::operations::CompactOptions;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;
use crate::TableContext;

#[derive(Clone)]
pub struct BlockCompactMutator {
    pub ctx: Arc<dyn TableContext>,
    pub operator: Operator,

    pub thresholds: BlockThresholds,
    pub compact_params: CompactOptions,
    pub cluster_key_id: Option<u32>,
}

impl BlockCompactMutator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        thresholds: BlockThresholds,
        compact_params: CompactOptions,
        operator: Operator,
        cluster_key_id: Option<u32>,
    ) -> Self {
        Self {
            ctx,
            operator,
            thresholds,
            compact_params,
            cluster_key_id,
        }
    }

    #[async_backtrace::framed]
    pub async fn target_select(&mut self) -> Result<Partitions> {
        let start = Instant::now();
        let snapshot = self.compact_params.base_snapshot.clone();
        let segment_locations = &snapshot.segments;
        let number_segments = segment_locations.len();
        let limit = self.compact_params.limit.unwrap_or(number_segments);

        // Status.
        self.ctx
            .set_status_info("compact: begin to build compact tasks");

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.operator.clone(),
            Arc::new(self.compact_params.base_snapshot.schema.clone()),
        );
        let mut checker = SegmentCompactChecker::new(
            self.compact_params.block_per_seg as u64,
            self.cluster_key_id,
        );

        let mut segment_idx = 0;
        let mut is_end = false;
        let mut parts = Vec::new();
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        for chunk in segment_locations.chunks(chunk_size) {
            // Read the segments information in parallel.
            let mut segment_infos = segments_io
                .read_segments::<Arc<CompactSegmentInfo>>(chunk, false)
                .await?
                .into_iter()
                .map(|sg| {
                    sg.map(|v| {
                        let idx = segment_idx;
                        segment_idx += 1;
                        (idx, v)
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            if let Some(default_cluster_key) = self.cluster_key_id {
                // sort descending.
                segment_infos.sort_by(|a, b| {
                    sort_by_cluster_stats(
                        &b.1.summary.cluster_stats,
                        &a.1.summary.cluster_stats,
                        default_cluster_key,
                    )
                });
            }

            // Check the segment to be compacted.
            // Size of compacted segment should be in range R == [threshold, 2 * threshold)
            for (segment_idx, compact_segment) in segment_infos.into_iter() {
                let segments_vec = checker.add(segment_idx, compact_segment);
                for segments in segments_vec {
                    self.generate_part(segments, &mut parts, &mut checker);
                }

                if checker.compacted_segment_cnt + checker.segments.len() >= limit
                    || checker.compacted_block_cnt >= MAX_BLOCK_COUNT as u64
                {
                    is_end = true;
                    break;
                }
            }

            // Status.
            {
                let status = format!(
                    "compact: read segment files:{}/{}, cost:{} sec",
                    segment_idx,
                    number_segments,
                    start.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
            }

            if is_end {
                break;
            }
        }

        // finalize the compaction.
        self.generate_part(
            std::mem::take(&mut checker.segments),
            &mut parts,
            &mut checker,
        );

        // Status.
        self.ctx.set_status_info(&format!(
            "compact: end to build lazy compact parts:{}, segments to be compacted:{}, cost:{} sec",
            parts.len(),
            checker.compacted_segment_cnt,
            start.elapsed().as_secs()
        ));

        let cluster = self.ctx.get_cluster();
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let partitions = if cluster.is_empty() || parts.len() < cluster.nodes.len() * max_threads {
            let column_ids = self
                .compact_params
                .base_snapshot
                .schema
                .to_leaf_column_id_set();
            let lazy_parts = parts
                .into_iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();
            Partitions::create_nolazy(
                PartitionsShuffleKind::Mod,
                BlockCompactMutator::build_compact_tasks(
                    self.ctx.clone(),
                    column_ids,
                    self.cluster_key_id,
                    self.thresholds,
                    lazy_parts,
                )
                .await?,
            )
        } else {
            Partitions::create(PartitionsShuffleKind::Mod, parts, true)
        };
        Ok(partitions)
    }

    #[async_backtrace::framed]
    pub async fn build_compact_tasks(
        ctx: Arc<dyn TableContext>,
        column_ids: HashSet<ColumnId>,
        cluster_key_id: Option<u32>,
        thresholds: BlockThresholds,
        mut lazy_parts: Vec<CompactLazyPartInfo>,
    ) -> Result<Vec<PartInfoPtr>> {
        let start = Instant::now();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrency = std::cmp::max(max_threads * 2, 10);
        let semaphore: Arc<Semaphore> = Arc::new(Semaphore::new(max_concurrency));

        let mut remain = lazy_parts.len() % max_threads;
        let batch_size = lazy_parts.len() / max_threads;
        let mut works = Vec::with_capacity(max_threads);
        while !lazy_parts.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let column_ids = column_ids.clone();
            let semaphore = semaphore.clone();

            let batch = lazy_parts.drain(0..batch_size).collect::<Vec<_>>();
            works.push(async move {
                let mut res = vec![];
                for lazy_part in batch {
                    let mut builder =
                        CompactTaskBuilder::new(column_ids.clone(), cluster_key_id, thresholds);
                    let parts = builder
                        .build_tasks(
                            lazy_part.segment_indices,
                            lazy_part.compact_segments,
                            semaphore.clone(),
                        )
                        .await?;
                    res.extend(parts);
                }
                Ok::<_, ErrorCode>(res)
            });
        }

        match futures::future::try_join_all(works).await {
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "build compact tasks failure, {}",
                e
            ))),
            Ok(res) => {
                let parts = res.into_iter().flatten().collect::<Vec<_>>();
                // Status.
                {
                    let elapsed_time = start.elapsed().as_millis() as u64;
                    ctx.set_status_info(&format!(
                        "compact: end to build compact parts:{}, cost:{} ms",
                        parts.len(),
                        elapsed_time,
                    ));
                    metrics_inc_compact_block_build_task_milliseconds(elapsed_time);
                }
                Ok(parts)
            }
        }
    }

    fn generate_part(
        &mut self,
        segments: Vec<(SegmentIndex, Arc<CompactSegmentInfo>)>,
        parts: &mut Vec<PartInfoPtr>,
        checker: &mut SegmentCompactChecker,
    ) {
        if !segments.is_empty() && checker.check_for_compact(&segments) {
            let mut segment_indices = Vec::with_capacity(segments.len());
            let mut compact_segments = Vec::with_capacity(segments.len());
            for (idx, segment) in segments.into_iter() {
                segment_indices.push(idx);
                compact_segments.push(segment);
            }

            let lazy_part = CompactLazyPartInfo::create(segment_indices, compact_segments);
            parts.push(lazy_part);
        }
    }
}

struct SegmentCompactChecker {
    segments: Vec<(SegmentIndex, Arc<CompactSegmentInfo>)>,
    total_block_count: u64,
    block_threshold: u64,
    cluster_key_id: Option<u32>,

    compacted_segment_cnt: usize,
    compacted_block_cnt: u64,
}

impl SegmentCompactChecker {
    fn new(block_threshold: u64, cluster_key_id: Option<u32>) -> Self {
        Self {
            segments: vec![],
            total_block_count: 0,
            block_threshold,
            cluster_key_id,
            compacted_block_cnt: 0,
            compacted_segment_cnt: 0,
        }
    }

    fn check_for_compact(
        &mut self,
        segments: &Vec<(SegmentIndex, Arc<CompactSegmentInfo>)>,
    ) -> bool {
        if segments.is_empty() {
            return false;
        }

        if segments.len() == 1 {
            let summary = &segments[0].1.summary;
            if (summary.block_count == 1 || summary.perfect_block_count == summary.block_count)
                && (self.cluster_key_id.is_none()
                    || self.cluster_key_id
                        == summary.cluster_stats.as_ref().map(|v| v.cluster_key_id))
            {
                return false;
            }
        }

        self.compacted_segment_cnt += segments.len();
        self.compacted_block_cnt += segments
            .iter()
            .fold(0, |acc, x| acc + x.1.summary.block_count);
        true
    }

    fn add(
        &mut self,
        idx: SegmentIndex,
        segment: Arc<CompactSegmentInfo>,
    ) -> Vec<Vec<(SegmentIndex, Arc<CompactSegmentInfo>)>> {
        self.total_block_count += segment.summary.block_count;
        if self.total_block_count < self.block_threshold {
            self.segments.push((idx, segment));
            return vec![];
        }

        if self.total_block_count > 2 * self.block_threshold {
            self.total_block_count = 0;
            let trivial = vec![(idx, segment)];
            if self.segments.is_empty() {
                return vec![trivial];
            } else {
                return vec![std::mem::take(&mut self.segments), trivial];
            }
        }

        self.total_block_count = 0;
        self.segments.push((idx, segment));
        vec![std::mem::take(&mut self.segments)]
    }
}

struct CompactTaskBuilder {
    column_ids: HashSet<ColumnId>,
    cluster_key_id: Option<u32>,
    thresholds: BlockThresholds,

    blocks: Vec<Arc<BlockMeta>>,
    total_rows: usize,
    total_size: usize,
}

impl CompactTaskBuilder {
    fn new(
        column_ids: HashSet<ColumnId>,
        cluster_key_id: Option<u32>,
        thresholds: BlockThresholds,
    ) -> Self {
        Self {
            column_ids,
            cluster_key_id,
            thresholds,
            blocks: vec![],
            total_rows: 0,
            total_size: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    fn take_blocks(&mut self) -> Vec<Arc<BlockMeta>> {
        self.total_rows = 0;
        self.total_size = 0;
        std::mem::take(&mut self.blocks)
    }

    fn add(&mut self, block: &Arc<BlockMeta>, thresholds: BlockThresholds) -> (bool, bool) {
        if let Some(default_cluster_key) = self.cluster_key_id {
            if block
                .cluster_stats
                .as_ref()
                .is_some_and(|v| v.level != 0 && v.cluster_key_id == default_cluster_key)
            {
                return (true, !self.blocks.is_empty());
            }
        }

        let total_rows = self.total_rows + block.row_count as usize;
        let total_size = self.total_size + block.block_size as usize;
        if !thresholds.check_large_enough(total_rows, total_size) {
            // blocks < N
            self.blocks.push(block.clone());
            self.total_rows = total_rows;
            self.total_size = total_size;
            (false, false)
        } else if thresholds.check_for_compact(total_rows, total_size) {
            // N <= blocks < 2N
            self.blocks.push(block.clone());
            (false, true)
        } else {
            // blocks > 2N
            (true, !self.blocks.is_empty())
        }
    }

    fn build_task(
        &self,
        tasks: &mut VecDeque<(usize, Vec<Arc<BlockMeta>>)>,
        unchanged_blocks: &mut Vec<(BlockIndex, Arc<BlockMeta>)>,
        block_idx: BlockIndex,
        blocks: Vec<Arc<BlockMeta>>,
    ) -> bool {
        let mut flag = false;
        if blocks.len() == 1 && !self.check_compact(&blocks[0]) {
            unchanged_blocks.push((block_idx, blocks[0].clone()));
            flag = true;
        } else {
            tasks.push_back((block_idx, blocks));
        }
        flag
    }

    fn check_compact(&self, block: &Arc<BlockMeta>) -> bool {
        let column_ids: HashSet<ColumnId> = block.col_metas.keys().cloned().collect();
        if self.column_ids == column_ids {
            // Check if the block needs to be resort.
            self.cluster_key_id.is_some_and(|key| {
                block
                    .cluster_stats
                    .as_ref()
                    .map_or(true, |v| v.cluster_key_id != key)
            })
        } else {
            true
        }
    }

    // Select the row_count >= min_rows_per_block or block_size >= max_bytes_per_block
    // as the perfect_block condition(N for short). Gets a set of segments, iterates
    // through the blocks, and finds the blocks >= N and blocks < 2N as a task.
    async fn build_tasks(
        &mut self,
        segment_indices: Vec<usize>,
        compact_segments: Vec<Arc<CompactSegmentInfo>>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<PartInfoPtr>> {
        let mut block_idx = 0;
        // Used to identify whether the latest block is unchanged or needs to be compacted.
        let mut latest_flag = true;
        let mut unchanged_blocks = Vec::new();
        let mut removed_segment_summary = Statistics::default();

        let runtime = GlobalIORuntime::instance();
        let mut handlers = Vec::with_capacity(compact_segments.len());
        for segment in compact_segments.into_iter().rev() {
            let permit = acquire_task_permit(semaphore.clone()).await?;
            let handler = runtime.spawn(async_backtrace::location!().frame({
                async move {
                    let blocks = segment.block_metas()?;
                    drop(permit);
                    Ok::<_, ErrorCode>((blocks, segment.summary.clone()))
                }
            }));
            handlers.push(handler);
        }

        let joint = futures::future::try_join_all(handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("deserialize failure, {}", e)))?;

        let mut blocks = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flat_map(|(blocks, summary)| {
                merge_statistics_mut(&mut removed_segment_summary, &summary, self.cluster_key_id);
                blocks
            })
            .collect::<Vec<_>>();

        if let Some(default_cluster_key) = self.cluster_key_id {
            // sort ascending.
            blocks.sort_by(|a, b| {
                sort_by_cluster_stats(&a.cluster_stats, &b.cluster_stats, default_cluster_key)
            });
        }

        let mut tasks = VecDeque::new();
        for block in blocks.iter() {
            let (unchanged, need_take) = self.add(block, self.thresholds);
            if need_take {
                let blocks = self.take_blocks();
                latest_flag = self.build_task(&mut tasks, &mut unchanged_blocks, block_idx, blocks);
                block_idx += 1;
            }
            if unchanged {
                let blocks = vec![block.clone()];
                latest_flag = self.build_task(&mut tasks, &mut unchanged_blocks, block_idx, blocks);
                block_idx += 1;
            }
        }

        if !self.is_empty() {
            let tail = self.take_blocks();
            if self.cluster_key_id.is_some() && latest_flag {
                // The clustering table cannot compact different level blocks.
                self.build_task(&mut tasks, &mut unchanged_blocks, block_idx, tail);
            } else {
                let mut blocks = if latest_flag {
                    unchanged_blocks.pop().map_or(vec![], |(_, v)| vec![v])
                } else {
                    tasks.pop_back().map_or(vec![], |(_, v)| v)
                };

                let (total_rows, total_size) =
                    blocks.iter().chain(tail.iter()).fold((0, 0), |mut acc, x| {
                        acc.0 += x.row_count as usize;
                        acc.1 += x.block_size as usize;
                        acc
                    });
                if self.thresholds.check_for_compact(total_rows, total_size) {
                    blocks.extend(tail);
                    self.build_task(&mut tasks, &mut unchanged_blocks, block_idx, blocks);
                } else {
                    // blocks > 2N
                    self.build_task(&mut tasks, &mut unchanged_blocks, block_idx, blocks);
                    self.build_task(&mut tasks, &mut unchanged_blocks, block_idx + 1, tail);
                }
            }
        }

        let mut removed_segment_indexes = segment_indices;
        let segment_idx = removed_segment_indexes.pop().unwrap();
        let mut partitions: Vec<PartInfoPtr> = Vec::with_capacity(tasks.len() + 1);
        for (block_idx, blocks) in tasks.into_iter() {
            partitions.push(Arc::new(Box::new(CompactPartInfo::CompactTaskInfo(
                CompactTaskInfo::create(blocks, BlockMetaIndex {
                    segment_idx,
                    block_idx,
                }),
            ))));
        }

        partitions.push(Arc::new(Box::new(CompactPartInfo::CompactExtraInfo(
            CompactExtraInfo::create(
                segment_idx,
                unchanged_blocks,
                removed_segment_indexes,
                removed_segment_summary,
            ),
        ))));
        Ok(partitions)
    }
}
