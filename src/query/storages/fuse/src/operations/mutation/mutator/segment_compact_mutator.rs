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

//! `COMPACT SEGMENT` is a pipeline over the selected segments:
//!
//! 1. `read_segments_oldest_first` reads segments concurrently, in order.
//! 2. `SegmentCompactor` plans merge groups in snapshot order, from summaries.
//! 3. `MergeTasks` runs planned groups concurrently (`merge_group`); their
//!    results are applied in plan order.
//! 4. On selection/merge failure or cancellation, newly written outputs are
//!    removed on a best-effort basis. Files orphaned by commit failure rely on vacuum.
//!
//! `max_threads` bounds concurrent merge groups; `max_storage_io_requests`
//! bounds all storage requests (reads and writes) of one compaction.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_set_compact_segments_select_duration_second;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::column_oriented_segment::VirtualBlockInput;
use futures::Stream;
use futures::StreamExt;
use log::info;
use opendal::Operator;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;
use tokio::task::JoinHandle;

use crate::TableContext;
use crate::io::CachedMetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::io::build_virtual_segment_schema;
use crate::io::read_segment_stats;
use crate::operations::CompactOptions;
use crate::statistics::reducers::generate_virtual_column_statistics;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::same_partition;

// A segment taking part in compaction: base snapshot index, summary, location.
type SegmentEntry = (usize, Arc<CompactSegmentInfo>, Location);

#[derive(Default)]
pub struct SegmentCompactionState {
    // Newly written segment paths, used to report successful merges. Commit
    // failures do not use these paths for synchronous rollback.
    pub new_segment_paths: Vec<String>,
    // base segment indexes to be replaced by compacted segments.
    pub replaced_segments: HashMap<usize, Location>,
    // base segment indexes removed by segment compaction.
    pub removed_segment_indexes: Vec<usize>,
    // number of fragmented segments compacted
    pub num_fragments_compacted: usize,
    // statistics of segments that were consumed by compaction
    pub removed_statistics: Statistics,
}

pub struct SegmentCompactMutator {
    ctx: Arc<dyn TableContext>,
    compact_params: CompactOptions,
    data_accessor: Operator,
    location_generator: TableMetaLocationGenerator,
    compaction: SegmentCompactionState,
    cluster_key_info: Option<ClusterKeyInfo>,
    pub(crate) partition_key_count: usize,
    table_meta_timestamps: TableMetaTimestamps,
}

impl SegmentCompactMutator {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        compact_params: CompactOptions,
        location_generator: TableMetaLocationGenerator,
        operator: Operator,
        cluster_key_info: Option<ClusterKeyInfo>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            compact_params,
            data_accessor: operator,
            location_generator,
            compaction: Default::default(),
            cluster_key_info,
            partition_key_count: 0,
            table_meta_timestamps,
        })
    }

    fn has_compaction(&self) -> bool {
        !self.compaction.new_segment_paths.is_empty()
    }

    pub fn into_compaction_state(self) -> SegmentCompactionState {
        self.compaction
    }

    pub fn base_snapshot(&self) -> &Arc<TableSnapshot> {
        &self.compact_params.base_snapshot
    }

    #[async_backtrace::framed]
    pub async fn target_select(&mut self) -> Result<bool> {
        let select_begin = Instant::now();

        let base_segments = &self.compact_params.base_snapshot.segments;
        if base_segments.len() <= 1 {
            return Ok(false);
        }

        // prepare compactor
        let schema = Arc::new(self.compact_params.base_snapshot.schema.clone());
        let settings = self.ctx.get_settings();
        let mut compactor = SegmentCompactor::new(
            self.compact_params.block_per_seg as u64,
            self.cluster_key_info.clone(),
            settings.get_max_threads()? as usize,
            settings.get_max_storage_io_requests()? as usize,
            schema,
            &self.data_accessor,
            &self.location_generator,
            self.table_meta_timestamps,
        );
        compactor.partition_key_count = self.partition_key_count;

        self.compaction = compactor
            .compact(
                base_segments,
                self.compact_params.num_segment_limit,
                |status| {
                    self.ctx.set_status_info(&status);
                },
            )
            .await?;

        metrics_set_compact_segments_select_duration_second(select_begin.elapsed());

        Ok(self.has_compaction())
    }
}

// Segments compactor that preserves the order of ingestion.
//
// Since the order of segments( and the order of blocks as well) should be preserved,
// if only segments of size "threshold" are allowed to be generated during compaction,
// there might be cases that to compact one fragmented segment, a large amount of
// non-fragmented segments have to be split into pieces and re-compacted.
//
// To avoid this "ripple effects", consecutive segments are allowed to be compacted into
// a new segment, if the size of compacted segment is lesser than 2 * threshold (exclusive).

pub struct SegmentCompactor<'a> {
    // Size of compacted segment should be in range R == [threshold, 2 * threshold)
    // within R, smaller one is preferred
    threshold: u64,
    cluster_key_info: Option<ClusterKeyInfo>,
    partition_key_count: usize,
    // fragmented segment collected so far, it will be reset to empty if compaction occurs
    fragmented_segments: Vec<SegmentEntry>,
    // state which keep the number of blocks of all the fragmented segment collected so far,
    // it will be reset to 0 if compaction occurs
    accumulated_num_blocks: u64,
    // number of segments planned so far, for progress reporting
    num_planned: usize,
    // Storage requests in flight for this compaction: segment and stats reads
    // plus merge output writes.
    max_io_requests: usize,
    // Schema used to decode the segments being compacted.
    schema: TableSchemaRef,
    // Runs planned merge groups concurrently and cleans up on failure.
    merge_tasks: MergeTasks,
    operator: &'a Operator,
    location_generator: &'a TableMetaLocationGenerator,
    // accumulated compaction state
    compacted_state: SegmentCompactionState,
    table_meta_timestamps: TableMetaTimestamps,
}

impl<'a> SegmentCompactor<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        threshold: u64,
        cluster_key_info: Option<ClusterKeyInfo>,
        max_threads: usize,
        max_io_requests: usize,
        schema: TableSchemaRef,
        operator: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        let merge_tasks = MergeTasks::new(
            operator.clone(),
            cluster_key_info.clone(),
            max_threads,
            max_io_requests,
        );
        Self {
            threshold,
            cluster_key_info,
            partition_key_count: 0,
            accumulated_num_blocks: 0,
            num_planned: 0,
            fragmented_segments: vec![],
            max_io_requests,
            schema,
            merge_tasks,
            operator,
            location_generator,
            compacted_state: Default::default(),
            table_meta_timestamps,
        }
    }

    /// `segments` are in snapshot order (newest first). LIMIT selects the newest
    /// locations before reading; the selected window is compacted oldest first.
    #[async_backtrace::framed]
    pub async fn compact<T>(
        mut self,
        segments: &[Location],
        limit: Option<usize>,
        status_callback: T,
    ) -> Result<SegmentCompactionState>
    where
        T: Fn(String),
    {
        let selected = limit
            .map(|n| n.max(2).min(segments.len()))
            .unwrap_or(segments.len());
        let mut reads = read_segments_oldest_first(
            self.operator.clone(),
            self.schema.clone(),
            self.merge_tasks.io_permits.clone(),
            self.max_io_requests,
            &segments[..selected],
        );
        let result = self
            .plan_and_merge(&mut reads, selected, &status_callback)
            .await;
        // Abort outstanding reads before cleaning up merge outputs.
        drop(reads);
        if let Err(err) = result {
            log::warn!(
                "compact segment failed: selected:{selected}, processed:{}, merged_groups:{}, error:{err}",
                self.num_planned,
                self.compacted_state.new_segment_paths.len(),
            );
            self.merge_tasks.abort().await;
            return Err(err);
        }
        info!(
            "compact segment: selected:{selected}, merged_groups:{}, fragments_compacted:{}",
            self.compacted_state.new_segment_paths.len(),
            self.compacted_state.num_fragments_compacted,
        );
        Ok(self.compacted_state)
    }

    // Plan merge groups in snapshot order while earlier groups merge
    // concurrently, then wait for the rest and apply them in plan order.
    #[async_backtrace::framed]
    async fn plan_and_merge<S, T>(
        &mut self,
        reads: &mut S,
        selected: usize,
        status_callback: &T,
    ) -> Result<()>
    where
        S: Stream<Item = Result<SegmentEntry>> + Unpin,
        T: Fn(String),
    {
        while let Some(result) = reads.next().await {
            let (idx, segment, location) = result?;
            self.add(idx, segment, location).await?;
            self.num_planned += 1;
            if self.num_planned.is_multiple_of(self.max_io_requests) || self.num_planned == selected
            {
                status_callback(format!(
                    "compact segment: processed segments:{}/{selected}",
                    self.num_planned
                ));
            }
        }
        self.compact_fragments().await?;
        while let Some(result) = self.merge_tasks.next().await? {
            self.apply_merge_result(result);
        }
        self.merge_tasks.finish();
        Ok(())
    }

    // accumulate one segment
    #[async_backtrace::framed]
    async fn add(
        &mut self,
        segment_idx: usize,
        segment_info: Arc<CompactSegmentInfo>,
        location: Location,
    ) -> Result<()> {
        let num_blocks_current_segment = segment_info.summary.block_count;

        if num_blocks_current_segment == 0 {
            // Removing this segment is destructive: verify that the summary does
            // not hide any blocks before dropping its location from the snapshot.
            let blocks = segment_info.block_metas()?;
            if !blocks.is_empty() {
                return Err(ErrorCode::StorageOther(format!(
                    "segment {} has zero blocks in its summary but {} blocks in its metadata",
                    location.0,
                    blocks.len()
                )));
            }
            self.compacted_state
                .removed_segment_indexes
                .push(segment_idx);
            return Ok(());
        }

        if let Some((_, previous, _)) = self.fragmented_segments.last()
            && !same_partition(
                previous.summary.partition_stats.as_ref(),
                segment_info.summary.partition_stats.as_ref(),
                self.partition_key_count,
            )
        {
            self.compact_fragments().await?;
        }

        let s = self.accumulated_num_blocks + num_blocks_current_segment;

        if s < self.threshold {
            // not enough blocks yet, just keep this segment for later compaction
            self.accumulated_num_blocks = s;
            self.fragmented_segments
                .push((segment_idx, segment_info, location));
        } else if s >= self.threshold && s < 2 * self.threshold {
            // compact the fragmented segments
            self.fragmented_segments
                .push((segment_idx, segment_info, location));
            self.compact_fragments().await?;
        } else {
            // JackTan25: I think this won't happen, right? so need to remove this branch??
            // no choice but to compact the fragmented segments collected so far.
            // in this situation, after compaction, the size of compacted segments may be
            // lesser than threshold. this happens if the size of segment BEFORE compaction
            // is already larger than threshold.
            self.compact_fragments().await?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn compact_fragments(&mut self) -> Result<()> {
        if self.fragmented_segments.is_empty() {
            return Ok(());
        }

        let fragments = std::mem::take(&mut self.fragmented_segments);
        self.accumulated_num_blocks = 0;

        // A single fragment stays unchanged in the base snapshot.
        if fragments.len() == 1 {
            return Ok(());
        }

        self.compacted_state.num_fragments_compacted += fragments.len();
        let location = self
            .location_generator
            .gen_segment_info_location(self.table_meta_timestamps, false);
        if let Some(result) = self.merge_tasks.submit(fragments, location).await? {
            self.apply_merge_result(result);
        }
        Ok(())
    }

    fn apply_merge_result(&mut self, result: MergeResult) {
        merge_statistics_mut(
            &mut self.compacted_state.removed_statistics,
            &result.statistics,
            self.cluster_key_info.as_ref(),
        );
        let replace_idx = *result.indexes.iter().min().expect("nonempty merge group");
        self.compacted_state
            .replaced_segments
            .insert(replace_idx, (result.location.clone(), SegmentInfo::VERSION));
        self.compacted_state
            .removed_segment_indexes
            .extend(result.indexes.into_iter().filter(|idx| *idx != replace_idx));
        self.compacted_state.new_segment_paths.push(result.location);
    }
}

// Dropping a JoinHandle detaches its task. Wrap compaction's segment reads,
// merge groups and HLL reads so failure or cancellation aborts them.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map_err(|e| ErrorCode::Internal(format!("compact segment task failed: {e}")))
    }
}

// Keep planned groups in submission order. A failed/cancelled compaction
// must abort its writers before deleting their output paths.
struct MergeTasks {
    operator: Operator,
    // Bounds all storage requests of one compaction: segment reads, stats
    // reads and merge output writes.
    io_permits: Arc<Semaphore>,
    cluster_key_info: Option<ClusterKeyInfo>,
    // Merge groups executed concurrently.
    capacity: usize,
    // Stats reads queued by one merge group.
    stats_read_window: usize,
    in_flight: VecDeque<AbortOnDrop<Result<MergeResult>>>,
    paths: Vec<String>,
    finished: bool,
}

struct MergeResult {
    indexes: Vec<usize>,
    location: String,
    statistics: Statistics,
}

impl MergeTasks {
    fn new(
        operator: Operator,
        cluster_key_info: Option<ClusterKeyInfo>,
        max_threads: usize,
        max_io_requests: usize,
    ) -> Self {
        Self {
            operator,
            io_permits: Arc::new(Semaphore::new(max_io_requests)),
            cluster_key_info,
            capacity: max_threads,
            // Stats reads are IO bound: queue as many as one compaction may
            // run. The shared semaphore bounds the requests actually in flight.
            stats_read_window: max_io_requests,
            in_flight: VecDeque::new(),
            paths: Vec::new(),
            finished: false,
        }
    }

    /// Submits a merge group. When every slot is busy, first waits for the
    /// oldest group and returns its result, so results stay in plan order.
    async fn submit(
        &mut self,
        fragments: Vec<SegmentEntry>,
        location: String,
    ) -> Result<Option<MergeResult>> {
        let completed = if self.in_flight.len() >= self.capacity {
            self.next().await?
        } else {
            None
        };
        self.paths.push(location.clone());
        let task = merge_group(
            self.operator.clone(),
            fragments,
            location,
            self.cluster_key_info.clone(),
            self.io_permits.clone(),
            self.stats_read_window,
        );
        self.in_flight
            .push_back(AbortOnDrop(GlobalIORuntime::instance().spawn(task)));
        Ok(completed)
    }

    async fn next(&mut self) -> Result<Option<MergeResult>> {
        let Some(task) = self.in_flight.front_mut() else {
            return Ok(None);
        };
        // Keep the handle in the queue across await so cancellation can
        // abort and await it before cleaning up its output files.
        let outcome = task.await;
        self.in_flight.pop_front();
        Ok(Some(outcome??))
    }

    fn finish(&mut self) {
        debug_assert!(self.in_flight.is_empty());
        self.finished = true;
    }

    /// Moves in-flight groups and their output paths into a cleanup task on
    /// the shared runtime. The task keeps running if its JoinHandle is
    /// dropped, so cleanup also survives cancellation of the query.
    fn spawn_cleanup(&mut self) -> JoinHandle<()> {
        self.finished = true;
        let tasks = std::mem::take(&mut self.in_flight);
        let paths = std::mem::take(&mut self.paths);
        let operator = self.operator.clone();
        GlobalIORuntime::instance().spawn(cleanup_merges(operator, tasks, paths))
    }

    async fn abort(&mut self) {
        let _ = self.spawn_cleanup().await;
    }
}

impl Drop for MergeTasks {
    fn drop(&mut self) {
        // Reached without finish()/abort() only when the query is cancelled.
        if !self.finished && !self.paths.is_empty() {
            drop(self.spawn_cleanup());
        }
    }
}

// The semaphore is never closed, so acquiring only waits for a free slot.
async fn acquire_io(permits: &Semaphore) -> Result<SemaphorePermit<'_>> {
    permits
        .acquire()
        .await
        .map_err(|e| ErrorCode::Internal(format!("compact segment io permits closed: {e}")))
}

async fn cleanup_merges(
    operator: Operator,
    tasks: VecDeque<AbortOnDrop<Result<MergeResult>>>,
    paths: Vec<String>,
) {
    for task in &tasks {
        task.0.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
    for path in paths {
        let stats_path =
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(&path);
        for output in [&path, &stats_path] {
            if let Err(err) = operator.delete(output).await {
                log::warn!("unable to remove uncommitted compact output {output}: {err}");
            }
        }
    }
}

struct DecodedGroup {
    blocks: Vec<Arc<BlockMeta>>,
    statistics: Statistics,
    // Stats files of all sources, or None if any source has none.
    stats_locations: Option<Vec<Location>>,
}

// Runs on the blocking pool: decoding block metas is CPU bound.
fn decode_fragments(
    fragments: Vec<SegmentEntry>,
    cluster_key_info: Option<&ClusterKeyInfo>,
) -> Result<DecodedGroup> {
    // Each fragment in a merge group contains at least one block. Reserve
    // this lower bound without trusting an unvalidated summary block count.
    let mut blocks = Vec::with_capacity(fragments.len());
    let mut virtual_inputs = Vec::with_capacity(fragments.len());
    let mut statistics = Statistics::default();
    // A merged stats file is only valid if every source has stats. Avoid
    // allocating or cloning further locations after the first missing one.
    let mut stats_locations = Some(Vec::new());
    for (_, segment, fragment_location) in fragments {
        merge_statistics_mut(&mut statistics, &segment.summary, cluster_key_info);
        let virtual_schema = segment.summary.virtual_segment_schema.clone().map(Arc::new);
        let segment_blocks = segment.block_metas()?;
        if segment.summary.block_count != segment_blocks.len() as u64 {
            return Err(ErrorCode::StorageOther(format!(
                "segment {} has {} blocks in its summary but {} blocks in its metadata",
                fragment_location.0,
                segment.summary.block_count,
                segment_blocks.len()
            )));
        }
        virtual_inputs.extend(
            (0..segment_blocks.len()).map(|_| VirtualBlockInput::Existing {
                schema: virtual_schema.clone(),
            }),
        );
        blocks.extend(segment_blocks);
        if let Some(locations) = &mut stats_locations {
            if let Some(meta) = segment.summary.additional_stats_meta.as_ref() {
                locations.push(meta.location.clone());
            } else {
                stats_locations = None;
            }
        }
    }
    statistics.virtual_segment_schema =
        build_virtual_segment_schema(&mut blocks, &mut virtual_inputs)?;
    statistics.virtual_col_stats = if blocks.iter().all(|b| b.virtual_block_meta.is_some()) {
        Some(generate_virtual_column_statistics(
            &blocks
                .iter()
                .map(|b| &b.virtual_block_meta.as_ref().unwrap().virtual_column_metas)
                .collect::<Vec<_>>(),
        ))
    } else {
        None
    };
    Ok(DecodedGroup {
        blocks,
        statistics,
        stats_locations,
    })
}

async fn merge_group(
    operator: Operator,
    fragments: Vec<SegmentEntry>,
    location: String,
    cluster_key_info: Option<ClusterKeyInfo>,
    io_permits: Arc<Semaphore>,
    stats_read_window: usize,
) -> Result<MergeResult> {
    let indexes = fragments.iter().map(|(idx, _, _)| *idx).collect::<Vec<_>>();
    let DecodedGroup {
        blocks,
        mut statistics,
        stats_locations,
    } = databend_common_base::runtime::spawn_blocking(move || {
        decode_fragments(fragments, cluster_key_info.as_ref())
    })
    .await
    .map_err(|err| ErrorCode::Internal(format!("compact block decode failed: {err}")))??;
    let mut stats_output = None;
    if let Some(stats_locations) = stats_locations {
        let stats = read_merged_stats(
            &operator,
            &io_permits,
            stats_locations,
            stats_read_window,
            blocks.len(),
        )
        .await?;
        let stats_data = stats.to_bytes()?;
        let stats_path =
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(&location);
        statistics.additional_stats_meta = Some(AdditionalStatsMeta {
            size: stats_data.len() as u64,
            location: (stats_path.clone(), SegmentStatistics::VERSION),
            ..Default::default()
        });
        stats_output = Some((stats_path, stats_data));
    }
    let new_segment = SegmentInfo::new(blocks, statistics);
    write_outputs(
        &operator,
        &io_permits,
        &new_segment,
        &location,
        stats_output,
    )
    .await?;
    Ok(MergeResult {
        indexes,
        location,
        statistics: new_segment.summary,
    })
}

// Write the merged segment and its stats file concurrently. Both writes run to
// completion even if one fails, so cleanup cannot race an unfinished write.
async fn write_outputs(
    operator: &Operator,
    io_permits: &Semaphore,
    segment: &SegmentInfo,
    location: &str,
    stats_output: Option<(String, Vec<u8>)>,
) -> Result<()> {
    let write_stats = async {
        if let Some((path, data)) = stats_output {
            let _permit = acquire_io(io_permits).await?;
            operator.write(&path, data).await?;
        }
        Ok::<_, ErrorCode>(())
    };
    let write_segment = async {
        let _permit = acquire_io(io_permits).await?;
        segment.write_meta_through_cache(operator, location).await?;
        Ok::<_, ErrorCode>(())
    };
    let (stats_result, segment_result) = tokio::join!(write_stats, write_segment);
    stats_result?;
    segment_result
}

// Concatenate the sources' per-block HLL and Top-N stats in block order.
// The window bounds one group's queued reads; all groups share `io_permits`
// with segment reads and writes, which bounds their active requests.
async fn read_merged_stats(
    operator: &Operator,
    io_permits: &Arc<Semaphore>,
    stats_locations: Vec<Location>,
    stats_read_window: usize,
    num_blocks: usize,
) -> Result<SegmentStatistics> {
    let mut block_hlls = Vec::with_capacity(num_blocks);
    let mut block_top_ns = Vec::with_capacity(num_blocks);
    let runtime = GlobalIORuntime::instance();
    let mut reads = futures::stream::iter(stats_locations)
        .map(|location| {
            let dal = operator.clone();
            let permits = io_permits.clone();
            AbortOnDrop(runtime.spawn(async move {
                let _permit = acquire_io(&permits).await?;
                read_segment_stats(dal, location).await
            }))
        })
        .buffered(stats_read_window);
    while let Some(result) = reads.next().await {
        let stats = result??;
        block_hlls.extend(stats.block_hlls.iter().cloned());
        block_top_ns.extend(stats.block_top_ns.iter().cloned());
    }
    Ok(SegmentStatistics::new(block_hlls, block_top_ns))
}

// Read `segments` (snapshot order) oldest first. Reads are spawned tasks, so
// they keep running while the planner waits for a merge slot; the window is
// twice the IO limit so one slow GET at the head does not leave storage idle.
fn read_segments_oldest_first(
    operator: Operator,
    schema: TableSchemaRef,
    io_permits: Arc<Semaphore>,
    max_io_requests: usize,
    segments: &[Location],
) -> impl Stream<Item = Result<SegmentEntry>> + Unpin + '_ {
    let runtime = GlobalIORuntime::instance();
    futures::stream::iter(segments.iter().cloned().enumerate().rev())
        .map(move |(idx, location)| {
            let dal = operator.clone();
            let schema = schema.clone();
            let permits = io_permits.clone();
            AbortOnDrop(runtime.spawn(async move {
                let _permit = acquire_io(&permits).await?;
                let segment =
                    SegmentsIO::read_compact_segment(dal, location.clone(), schema, false).await?;
                Ok::<_, ErrorCode>((idx, segment, location))
            }))
        })
        .buffered(max_io_requests * 2)
        .map(|result| result.and_then(|inner| inner))
}
