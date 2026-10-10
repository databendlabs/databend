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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_set_compact_segments_select_duration_second;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
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
use futures::StreamExt;
use log::info;
use opendal::Operator;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;
use tokio::task::JoinHandle;

use crate::TableContext;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::io::build_virtual_segment_schema;
use crate::io::read_segment_stats;
use crate::operations::CompactOptions;
use crate::statistics::reducers::generate_virtual_column_statistics;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::same_partition;
use crate::statistics::sort_by_cluster_stats;

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

        Ok(!self.compaction.new_segment_paths.is_empty())
    }
}

// Metadata-only compaction preserves ingestion order for unclustered tables;
// clustered tables retain the original chunk-local cluster-stat ordering.
// Allow merged segments below 2 * threshold instead of splitting existing
// segments just to reach threshold, avoiding cascading rewrites.

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
        let merge_tasks = MergeTasks {
            operator: operator.clone(),
            io_permits: Arc::new(Semaphore::new(max_io_requests)),
            decode_permits: Arc::new(Semaphore::new(max_threads)),
            cluster_key_info: cluster_key_info.clone(),
            capacity: max_threads,
            stats_read_window: max_io_requests,
            in_flight: VecDeque::new(),
            paths: Vec::new(),
            finished: false,
        };
        Self {
            threshold,
            cluster_key_info,
            partition_key_count: 0,
            accumulated_num_blocks: 0,
            num_planned: 0,
            fragmented_segments: vec![],
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
        let result = self
            .plan_and_merge(&segments[..selected], &status_callback)
            .await;
        if let Err(err) = result {
            log::warn!(
                "compact segment failed: selected:{selected}, processed:{}, merged_groups:{}, error:{err}",
                self.num_planned,
                self.compacted_state.new_segment_paths.len(),
            );
            let _ = self.merge_tasks.spawn_cleanup().await;
            return Err(err);
        }
        info!(
            "compact segment: selected:{selected}, merged_groups:{}, fragments_compacted:{}",
            self.compacted_state.new_segment_paths.len(),
            self.compacted_state.num_fragments_compacted,
        );
        Ok(self.compacted_state)
    }

    #[async_backtrace::framed]
    async fn plan_and_merge<T>(
        &mut self,
        segments: &[Location],
        status_callback: &T,
    ) -> Result<()>
    where
        T: Fn(String),
    {
        let selected = segments.len();
        // Plan oldest-first in bounded chunks. Fragments may span chunks;
        // merge groups execute concurrently and results stay in plan order.
        let chunk_size = self.merge_tasks.capacity * 4;
        let key_id = self
            .cluster_key_info
            .as_ref()
            .map(|key| key.cluster_key_id());
        let mut chunk_end = selected;
        for locations in segments.rchunks(chunk_size) {
            let chunk_start = chunk_end - locations.len();
            chunk_end = chunk_start;
            let mut chunk = SegmentsIO::read_segments_with_semaphore::<Arc<CompactSegmentInfo>>(
                self.operator.clone(),
                self.schema.clone(),
                locations,
                false,
                self.merge_tasks.io_permits.clone(),
            )
            .await?
            .into_iter()
            .enumerate()
            .rev()
            .map(|(idx, segment)| segment.map(|segment| (chunk_start + idx, segment)))
            .collect::<Result<Vec<_>>>()?;
            if let Some(key_id) = key_id {
                chunk.sort_by(|a, b| {
                    sort_by_cluster_stats(
                        a.1.summary.cluster_stats.as_ref(),
                        b.1.summary.cluster_stats.as_ref(),
                        key_id,
                    )
                });
            }
            for (idx, segment) in chunk {
                self.add(idx, segment, &segments[idx]).await?;
                self.num_planned += 1;
            }
            status_callback(format!(
                "compact segment: processed segments:{}/{selected}",
                self.num_planned
            ));
        }
        self.compact_fragments().await?;
        while let Some(result) = self.merge_tasks.next().await? {
            self.apply_merge_result(result);
        }
        debug_assert!(self.merge_tasks.in_flight.is_empty());
        self.merge_tasks.finished = true;
        Ok(())
    }

    // accumulate one segment
    #[async_backtrace::framed]
    async fn add(
        &mut self,
        segment_idx: usize,
        segment_info: Arc<CompactSegmentInfo>,
        location: &Location,
    ) -> Result<()> {
        let num_blocks_current_segment = segment_info.summary.block_count;
        if num_blocks_current_segment == 0 {
            return Err(ErrorCode::StorageOther(format!(
                "segment {} has zero blocks in its summary",
                location.0
            )));
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

        let total_blocks = self.accumulated_num_blocks + num_blocks_current_segment;
        if total_blocks >= 2 * self.threshold {
            // Adding this segment would exceed the target range. Flush the
            // pending fragments and leave this segment unchanged.
            return self.compact_fragments().await;
        }

        self.fragmented_segments
            .push((segment_idx, segment_info, location.clone()));
        if total_blocks < self.threshold {
            self.accumulated_num_blocks = total_blocks;
        } else {
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

// Keep planned groups in submission order. A failed/cancelled compaction
// must drain its writers before deleting their output paths.
struct MergeTasks {
    operator: Operator,
    // Bounds all storage requests of one compaction: segment reads, stats
    // reads and merge output writes.
    io_permits: Arc<Semaphore>,
    // Shared by block metadata decoding, assembly, statistics concatenation
    // and output serialization. The common stats reader still decompresses
    // and deserializes input statistics on the IO runtime, outside this budget.
    decode_permits: Arc<Semaphore>,
    cluster_key_info: Option<ClusterKeyInfo>,
    // Merge groups executed concurrently.
    capacity: usize,
    // Stats reads queued by one merge group.
    stats_read_window: usize,
    in_flight: VecDeque<JoinHandle<Result<MergeResult>>>,
    paths: Vec<String>,
    finished: bool,
}

struct DecodedGroup {
    blocks: Vec<Arc<BlockMeta>>,
    statistics: Statistics,
    // All source statistics locations, or None if any source has no file.
    stats_locations: Option<Vec<(Location, usize)>>,
}

struct MergeResult {
    indexes: Vec<usize>,
    location: String,
    statistics: Statistics,
}

impl MergeTasks {
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
        let task = Self::merge_group(
            self.operator.clone(),
            fragments,
            location,
            self.cluster_key_info.clone(),
            self.io_permits.clone(),
            self.stats_read_window,
            self.decode_permits.clone(),
        );
        self.in_flight
            .push_back(GlobalIORuntime::instance().spawn(task));
        Ok(completed)
    }

    async fn decode_fragments(
        fragments: Vec<SegmentEntry>,
        cluster_key_info: Option<ClusterKeyInfo>,
        decode_permits: Arc<Semaphore>,
    ) -> Result<DecodedGroup> {
        // Decode in parallel. Acquire before spawning to bound blocking work.
        let mut tasks = Vec::with_capacity(fragments.len());
        for (_, segment, location) in fragments {
            let permit = decode_permits.clone().acquire_owned().await.map_err(|e| {
                ErrorCode::Internal(format!("compact segment decode permits closed: {e}"))
            })?;
            tasks.push(databend_common_base::runtime::spawn_blocking(move || {
                // Keep the permit until decoding finishes, even if the caller
                // stops waiting for this task.
                let _permit = permit;
                let blocks = segment.block_metas()?;
                if segment.summary.block_count != blocks.len() as u64 {
                    return Err(ErrorCode::StorageOther(format!(
                        "segment {} has {} blocks in its summary but {} blocks in its metadata",
                        location.0,
                        segment.summary.block_count,
                        blocks.len()
                    )));
                }
                Ok::<_, ErrorCode>((segment, blocks))
            }));
        }
        let decoded = futures::future::try_join_all(tasks)
            .await
            .map_err(|e| ErrorCode::Internal(format!("compact segment decode task failed: {e}")))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        // All decode permits have been released before assembly acquires one;
        // this also works with a single CPU permit.
        let (blocks, statistics, stats_locations) = run_cpu(decode_permits, move || {
            let block_count = decoded.iter().map(|(_, blocks)| blocks.len()).sum();
            let mut blocks = Vec::with_capacity(block_count);
            let mut virtual_inputs = Vec::with_capacity(block_count);
            let mut statistics = Statistics::default();
            // A merged stats file is only valid if every source has stats.
            let mut stats_locations = Some(Vec::new());
            // try_join_all preserves source order, independently of decode completion.
            for (segment, segment_blocks) in decoded {
                merge_statistics_mut(&mut statistics, &segment.summary, cluster_key_info.as_ref());
                let virtual_schema = segment.summary.virtual_segment_schema.clone().map(Arc::new);
                virtual_inputs.extend((0..segment_blocks.len()).map(|_| {
                    VirtualBlockInput::Existing {
                        schema: virtual_schema.clone(),
                    }
                }));
                blocks.extend(segment_blocks);
                if let Some(locations) = &mut stats_locations {
                    if let Some(meta) = segment.summary.additional_stats_meta.as_ref() {
                        locations
                            .push((meta.location.clone(), segment.summary.block_count as usize));
                    } else {
                        stats_locations = None;
                    }
                }
            }
            statistics.virtual_segment_schema =
                build_virtual_segment_schema(&mut blocks, &mut virtual_inputs)?;
            statistics.virtual_col_stats = if blocks.iter().all(|b| b.virtual_block_meta.is_some())
            {
                Some(generate_virtual_column_statistics(
                    &blocks
                        .iter()
                        .map(|b| &b.virtual_block_meta.as_ref().unwrap().virtual_column_metas)
                        .collect::<Vec<_>>(),
                ))
            } else {
                None
            };
            Ok((blocks, statistics, stats_locations))
        })
        .await?;
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
        decode_permits: Arc<Semaphore>,
    ) -> Result<MergeResult> {
        let indexes = fragments.iter().map(|(idx, _, _)| *idx).collect::<Vec<_>>();
        let DecodedGroup {
            blocks,
            mut statistics,
            stats_locations,
        } = Self::decode_fragments(fragments, cluster_key_info, decode_permits.clone()).await?;
        let stats = if let Some(stats_locations) = stats_locations {
            Some(
                Self::read_source_stats(&operator, &io_permits, stats_locations, stats_read_window)
                    .await?,
            )
        } else {
            None
        };
        let output_location = location.clone();
        let (segment_data, cached_segment, stats_output, statistics) =
            run_cpu(decode_permits, move || {
                let mut stats_output = None;
                if let Some(sources) = stats {
                    let mut block_hlls = Vec::with_capacity(blocks.len());
                    let mut block_top_ns = Vec::with_capacity(blocks.len());
                    for (stats, block_count) in sources {
                        let mut stats = Arc::unwrap_or_clone(stats);
                        stats.align_to_blocks(block_count)?;
                        block_hlls.extend(stats.block_hlls);
                        block_top_ns.extend(stats.block_top_ns);
                    }
                    let stats_data = SegmentStatistics::new(block_hlls, block_top_ns).to_bytes()?;
                    let stats_path = TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(&output_location);
                    statistics.additional_stats_meta = Some(AdditionalStatsMeta {
                        size: stats_data.len() as u64,
                        location: (stats_path.clone(), SegmentStatistics::VERSION),
                        ..Default::default()
                    });
                    stats_output = Some((stats_path, stats_data));
                }
                let new_segment = SegmentInfo::new(blocks, statistics);
                let segment_data = new_segment.to_bytes()?;
                // Reuse the encoded blocks instead of encoding and compressing them again.
                let cached_segment = SegmentInfo::cache()
                    .map(|_| CompactSegmentInfo::from_reader(segment_data.as_slice()))
                    .transpose()?;
                Ok((segment_data, cached_segment, stats_output, new_segment.summary))
            }).await?;
        Self::write_outputs(
            &operator,
            &io_permits,
            (segment_data, cached_segment),
            &location,
            stats_output,
        )
        .await?;
        Ok(MergeResult {
            indexes,
            location,
            statistics,
        })
    }

    // Read in source order. The window bounds queued reads; the shared IO
    // semaphore bounds active requests across all groups.
    async fn read_source_stats(
        operator: &Operator,
        io_permits: &Arc<Semaphore>,
        stats_locations: Vec<(Location, usize)>,
        stats_read_window: usize,
    ) -> Result<Vec<(Arc<SegmentStatistics>, usize)>> {
        let mut sources = Vec::with_capacity(stats_locations.len());
        let runtime = GlobalIORuntime::instance();
        let mut reads = futures::stream::iter(stats_locations)
            .map(|(location, block_count)| {
                let dal = operator.clone();
                let permits = io_permits.clone();
                runtime.spawn(async move {
                    let _permit = acquire_io(&permits).await?;
                    Ok::<_, ErrorCode>((read_segment_stats(dal, location).await?, block_count))
                })
            })
            .buffered(stats_read_window);
        while let Some(result) = reads.next().await {
            sources.push(result.map_err(|e| {
                ErrorCode::Internal(format!("compact segment statistics read task failed: {e}"))
            })??);
        }
        Ok(sources)
    }

    // Drive both writes to completion even if one fails, before cleanup.
    async fn write_outputs(
        operator: &Operator,
        io_permits: &Semaphore,
        segment: (Vec<u8>, Option<CompactSegmentInfo>),
        location: &str,
        stats_output: Option<(String, Vec<u8>)>,
    ) -> Result<()> {
        let (segment_data, cached_segment) = segment;
        let write_stats = async {
            if let Some((path, data)) = stats_output {
                let _permit = acquire_io(io_permits).await?;
                operator.write(&path, data).await?;
            }
            Ok::<_, ErrorCode>(())
        };
        let write_segment = async {
            let _permit = acquire_io(io_permits).await?;
            // Same write-then-cache ordering as write_meta_through_cache; CPU
            // preparation stays local to compaction and outside the IO permit.
            operator.write(location, segment_data).await?;
            if let Some(cached) = cached_segment
                && let Some(cache) = SegmentInfo::cache()
            {
                cache.insert(location.to_owned(), cached);
            }
            Ok::<_, ErrorCode>(())
        };
        let (stats_result, segment_result) = tokio::join!(write_stats, write_segment);
        stats_result?;
        segment_result
    }

    async fn next(&mut self) -> Result<Option<MergeResult>> {
        let Some(task) = self.in_flight.front_mut() else {
            return Ok(None);
        };
        // Keep the handle in the queue across await so cancellation can
        // drain it before cleaning up its output files.
        let outcome = task
            .await
            .map_err(|e| ErrorCode::Internal(format!("compact segment merge task failed: {e}")));
        self.in_flight.pop_front();
        Ok(Some(outcome??))
    }

    /// Moves in-flight groups and their output paths into a cleanup task on
    /// the shared runtime. The task keeps running if its JoinHandle is
    /// dropped, so cleanup also survives cancellation of the query.
    fn spawn_cleanup(&mut self) -> JoinHandle<()> {
        self.finished = true;
        let tasks = std::mem::take(&mut self.in_flight);
        let paths = std::mem::take(&mut self.paths);
        let operator = self.operator.clone();
        GlobalIORuntime::instance().spawn(async move {
            // Do not abort storage futures: a backend write may outlive a dropped
            // future. Drain every submitted group before deleting its outputs.
            for task in tasks {
                let _ = task.await;
            }
            for path in paths {
                let stats_path =
                    TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                        &path,
                    );
                for output in [&path, &stats_path] {
                    if let Err(err) = operator.delete(output).await {
                        log::warn!("unable to remove uncommitted compact output {output}: {err}");
                    }
                }
            }
        })
    }
}

impl Drop for MergeTasks {
    fn drop(&mut self) {
        // An unfinished compaction still owns outputs when the query is cancelled.
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

async fn run_cpu<T, F>(permits: Arc<Semaphore>, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    let permit = permits
        .acquire_owned()
        .await
        .map_err(|e| ErrorCode::Internal(format!("compact segment CPU permits closed: {e}")))?;
    databend_common_base::runtime::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await?
}
