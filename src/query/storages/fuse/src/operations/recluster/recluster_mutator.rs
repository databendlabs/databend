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

use std::cmp;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::parse_cluster_keys;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::PartitionStatistics;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ClusterType;
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use log::debug;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::DEFAULT_RECLUSTER_DEPTH;
use crate::FUSE_OPT_KEY_RECLUSTER_DEPTH;
use crate::FuseTable;
use crate::MAX_RECLUSTER_DEPTH;
use crate::MIN_RECLUSTER_DEPTH;
use crate::SegmentLocation;
use crate::io::MetaReaders;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterBlockStats;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::task_candidate;
use crate::statistics::VectorClusterInfo;
use crate::statistics::partition_values;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;

/// Maximum recluster depth allowed when only two blocks remain.
/// For two-block layouts, repeated reclustering beyond this level
/// rarely improves data locality and may cause task churn.
const MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS: i32 = 2;
/// Maximum recluster level allowed for candidate selection.
/// Blocks that reach this level have already been rewritten many times, so
/// keep them out of future recluster tasks to avoid unbounded level growth.
const MAX_RECLUSTER_LEVEL: i32 = 32;
const SMALL_TABLE_RECLUSTER_BLOCK_COUNT: u64 = 1000;
/// Minimum depth threshold enforced for Hilbert clustering. Hilbert overlap is measured on
/// per-dimension MBRs, which report false overlaps even when blocks do not actually intersect,
/// so the measured depth never settles at 1. Clamping the threshold up to this floor keeps
/// RECLUSTER FINAL from selecting tasks forever on already well-clustered data.
const HILBERT_MIN_RECLUSTER_DEPTH: f64 = 8.0;
/// Bound the synchronous block-metadata work performed by one candidate probe.
const MAX_RECLUSTER_WINDOW_SEGMENTS: usize = 128;

/// A partition-local segment window selected for candidate probing.
#[derive(Clone)]
pub struct SelectedReclusterWindow {
    pub segments: Vec<SelectedReclusterSegment>,
    pub partition_stats: Option<PartitionStatistics>,
}

/// Candidate tasks plus cached segment metadata for one scanned window.
#[derive(Clone, Default)]
pub struct ReclusterCandidateWindow {
    // Window locations plus cached SegmentInfo for positions touched by candidates.
    pub(crate) segments: Vec<(Location, Option<Arc<SegmentInfo>>)>,
    pub(crate) tasks: Vec<ReclusterTaskCandidate>,
    // Exact PARTITION BY values shared by all segments in this window.
    partition_stats: Option<PartitionStatistics>,
}

impl ReclusterCandidateWindow {
    /// Number of task candidates in this window.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Whether this window contains any rewrite or repack task.
    pub(crate) fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Score of one task candidate by index.
    pub fn task_score(&self, task_idx: usize) -> CandidateScore {
        self.tasks[task_idx].score
    }
}

/// Carry-over state reused across rounds of one `RECLUSTER FINAL` statement.
#[derive(Clone, Default)]
pub struct ReclusterFinalCarry {
    // State reused across rounds of one RECLUSTER FINAL statement.
    pub(crate) pending: Vec<ReclusterCandidateWindow>,
    // Next scan-range start in the latest snapshot. A successful task resets this to the current
    // range start so the next round can continue reclustering replacement segments.
    pub(crate) scan_cursor: usize,
    // Cached candidates must match this cluster key.
    pub(crate) cluster_key_id: u32,
    // Reuse block-meta decode workers across rounds of one FINAL statement.
    pub(crate) decode_runtime: Option<Arc<Runtime>>,
}

impl ReclusterFinalCarry {
    /// Lazily create or reuse the block-meta decode runtime for FINAL recluster.
    pub(crate) fn decode_runtime(&mut self, workers: usize) -> Result<Arc<Runtime>> {
        if self.decode_runtime.is_none() {
            self.decode_runtime = Some(Arc::new(Runtime::with_worker_threads(
                workers,
                Some("recluster-block-meta-worker".to_owned()),
            )?));
        }

        Ok(self.decode_runtime.as_ref().unwrap().clone())
    }
}

/// Builds recluster candidate windows and materializes selected tasks.
#[derive(Clone)]
pub struct ReclusterMutator {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) operator: Operator,
    pub(crate) schema: TableSchemaRef,
    pub(crate) max_tasks: usize,
    pub(crate) properties: ReclusterProperties,
    strategy: Arc<dyn ReclusterStrategy>,
}

impl ReclusterMutator {
    /// Build a recluster mutator from table metadata and current snapshot state.
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
        mode: ReclusterMode,
    ) -> Result<Self> {
        let schema = table.schema_with_stream();
        let Some(cluster_key_id) = table.cluster_key_id() else {
            return Err(ErrorCode::Internal("recluster requires cluster key id"));
        };
        let block_thresholds = table.get_block_thresholds();

        let mut depth_threshold = table
            .get_table_info()
            .options()
            .get(FUSE_OPT_KEY_RECLUSTER_DEPTH)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or({
                if snapshot.summary.block_count <= SMALL_TABLE_RECLUSTER_BLOCK_COUNT {
                    MIN_RECLUSTER_DEPTH
                } else {
                    DEFAULT_RECLUSTER_DEPTH
                }
            }) as f64;
        // Hilbert overlap is measured on per-dimension MBRs, which report false overlaps even when
        // blocks do not actually intersect, so the measured depth never settles at 1. Clamp the
        // threshold up to a Hilbert-specific floor so RECLUSTER FINAL cannot keep selecting tasks
        // forever (this also overrides an explicit low `recluster_depth` for Hilbert tables).
        if matches!(table.cluster_type(), Some(ClusterType::Hilbert)) {
            depth_threshold = depth_threshold.max(HILBERT_MIN_RECLUSTER_DEPTH);
        }

        let settings = ctx.get_settings();
        let recluster_block_size = settings.get_recluster_block_size()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        let memory_threshold = if max_memory_usage == 0 {
            recluster_block_size
        } else {
            let memory_usage = ctx.get_nodes_memory_usage();
            let memory_budget = max_memory_usage.saturating_sub(memory_usage) * 30 / 100;
            if memory_budget == 0 {
                return Err(ErrorCode::MemoryExceedsLimit(format!(
                    "Not enough memory for recluster: max_memory_usage = {}, used = {}.",
                    max_memory_usage, memory_usage
                )));
            }
            // Actual block sizes are checked during task selection.
            recluster_block_size.min(memory_budget)
        };
        let mut max_tasks = 1;
        let cluster = ctx.get_cluster();
        if !cluster.is_empty() && settings.get_enable_distributed_recluster()? {
            max_tasks = cluster.nodes.len();
        }

        let Some(cluster_keys) = table.resolve_cluster_keys() else {
            return Err(ErrorCode::Internal(
                "recluster requires cluster key expressions",
            ));
        };
        let parsed_cluster_keys =
            parse_cluster_keys(ctx.clone(), Arc::new(table.clone()), cluster_keys)?;
        let (properties, strategy) = ReclusterProperties::try_create(
            table,
            &schema,
            parsed_cluster_keys,
            mode,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            memory_threshold,
        )?;

        Ok(Self {
            ctx,
            operator: table.get_operator(),
            schema,
            max_tasks,
            properties,
            strategy,
        })
    }

    /// Build a recluster mutator directly for tests.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        schema: TableSchemaRef,
        cluster_key_exprs: Vec<Expr<usize>>,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_id: u32,
        partition_key_count: usize,
        max_tasks: usize,
        mode: ReclusterMode,
        vector_cluster_info: Option<VectorClusterInfo>,
    ) -> Self {
        let memory_threshold = ctx
            .get_settings()
            .get_recluster_block_size()
            .expect("get recluster_block_size setting for recluster mutator")
            as usize;
        let (properties, strategy) = ReclusterProperties::for_test(
            &schema,
            cluster_key_exprs,
            mode,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            partition_key_count,
            memory_threshold,
            vector_cluster_info,
        );
        Self {
            ctx,
            operator,
            schema,
            max_tasks,
            properties,
            strategy,
        }
    }

    /// Decode one selected segment window and build candidate tasks from it.
    #[async_backtrace::framed]
    pub async fn probe_candidate_window(
        &self,
        selected_window: SelectedReclusterWindow,
        task_budget: usize,
        decode_runtime: Arc<Runtime>,
        decode_semaphore: Arc<Semaphore>,
    ) -> Result<ReclusterCandidateWindow> {
        debug_assert!(task_budget > 0);
        let SelectedReclusterWindow {
            segments: compact_segments,
            partition_stats,
        } = selected_window;
        let mut window_segments = Vec::with_capacity(compact_segments.len());
        let mut window_segment_infos = Vec::with_capacity(compact_segments.len());
        let mut selected_segments = Vec::with_capacity(compact_segments.len());
        let mut total_block_count = 0usize;
        for (window_pos, segment) in compact_segments.into_iter().enumerate() {
            total_block_count += segment.info.summary.block_count as usize;
            selected_segments.push((
                window_pos,
                segment.loc.location.clone(),
                segment.info.clone(),
            ));
            window_segments.push((segment.loc.location, None));
            window_segment_infos.push(segment.info);
        }

        // Read blocks once; materialization reuses cached selected SegmentInfo.
        let mut blocks_by_segment = self
            .gather_blocks(selected_segments, decode_runtime, decode_semaphore)
            .await?;
        let mut blocks = Vec::with_capacity(total_block_count);
        for segment_blocks in &blocks_by_segment {
            blocks.extend(segment_blocks.iter());
        }

        let mut candidate_window = ReclusterCandidateWindow {
            segments: window_segments,
            tasks: Vec::new(),
            partition_stats,
        };
        let mut selected_window_positions = vec![false; window_segment_infos.len()];

        let tasks = self.build_tasks(&blocks, task_budget)?;

        for candidate in &tasks {
            for (window_pos, _) in &candidate.selected_blocks {
                selected_window_positions[*window_pos] = true;
            }
        }
        candidate_window.tasks = tasks;

        if candidate_window.tasks.is_empty() {
            let all_original_stats = blocks
                .iter()
                .all(|block| matches!(block.stats, ReclusterBlockStats::Original));
            let unordered = || {
                blocks.windows(2).any(|window| {
                    sort_by_cluster_stats(
                        Some(window[0].stats()),
                        Some(window[1].stats()),
                        self.properties.cluster_key_id,
                    ) == cmp::Ordering::Greater
                })
            };
            let selected_segment_count = window_segment_infos.len();
            let target_segment_count =
                total_block_count.div_ceil(self.properties.block_thresholds.block_per_segment);
            let compactable_repack = total_block_count > 0
                && selected_segment_count > 1
                && target_segment_count < selected_segment_count;
            let unordered_repack = self.properties.mode == ReclusterMode::Conservative
                && all_original_stats
                && unordered();
            if compactable_repack || unordered_repack {
                // Repack-only candidate removes segments without rewrite tasks.
                selected_window_positions.fill(true);
                candidate_window.tasks.push(ReclusterTaskCandidate {
                    score: CandidateScore {
                        selected_total_bytes: 0,
                        max_depth: 0,
                        average_depth: 0.0,
                    },
                    selected_blocks: Vec::new(),
                    output_level: 0,
                    all_ordered: false,
                });
            } else {
                return Ok(candidate_window);
            }
        }

        for (window_pos, selected) in selected_window_positions.into_iter().enumerate() {
            if !selected {
                continue;
            }
            let info = &window_segment_infos[window_pos];
            let blocks = blocks_by_segment[window_pos]
                .drain(..)
                .map(|block| block.meta)
                .collect::<Vec<_>>();
            candidate_window.segments[window_pos].1 = Some(Arc::new(SegmentInfo {
                format_version: info.format_version,
                blocks,
                summary: info.summary.clone(),
            }));
        }

        Ok(candidate_window)
    }

    /// Bin block indices into recluster groups and build rewrite-task
    /// candidates. This reuses the already decoded block metas in this window;
    /// it only builds in-memory groups and runs candidate selection, without
    /// extra pruning or IO.
    fn build_tasks(
        &self,
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        let mut blocks_map: BTreeMap<(ReclusterGroup, Vec<Scalar>), Vec<usize>> = BTreeMap::new();
        for (idx, block) in blocks.iter().enumerate() {
            let level = block.stats().level;
            if level < 0 {
                continue;
            }
            if level >= MAX_RECLUSTER_LEVEL {
                // Terminal-level blocks are excluded from further rewrite tasks.
                continue;
            }
            let partition = if self.properties.partition_key_count == 0 {
                Vec::new()
            } else if let Some(partition) = partition_values(
                block.meta.partition_stats.as_ref(),
                self.properties.partition_key_count,
            ) {
                partition.to_vec()
            } else {
                // Never rewrite a block with missing or non-constant partition metadata
                // together with another partition.
                continue;
            };
            blocks_map
                .entry((
                    ReclusterGroup::assign(level, self.properties.mode),
                    partition,
                ))
                .or_default()
                .push(idx);
        }

        let mut tasks: Vec<ReclusterTaskCandidate> = Vec::new();
        let mut deferred_candidates = Vec::new();
        let large_task_bytes_threshold = self.large_task_bytes_threshold();
        for ((group, _), indices) in blocks_map {
            if tasks.len() >= task_budget {
                break;
            }
            let remaining_task_budget = task_budget - tasks.len();
            let candidates = self.build_recluster_task_candidates_for_indices(
                group,
                indices,
                blocks,
                remaining_task_budget,
            )?;

            for candidate in candidates {
                let defer = candidate.score.selected_total_bytes < large_task_bytes_threshold
                    && (candidate.score.max_depth as f64) < 4.0 * self.properties.depth_threshold;
                if defer {
                    debug!(
                        "recluster: defer candidate group={} selected_bytes={} max_depth={} depth_threshold={} skip_reason=deferred_small_shallow_task",
                        group,
                        candidate.score.selected_total_bytes,
                        candidate.score.max_depth,
                        self.properties.depth_threshold,
                    );
                    deferred_candidates.push(candidate);
                } else {
                    tasks.push(candidate);
                    if tasks.len() >= task_budget {
                        break;
                    }
                }
            }
        }

        if tasks.len() < task_budget {
            deferred_candidates.sort_by(|left, right| right.score.cmp_desc(&left.score));
            let remaining_task_budget = task_budget - tasks.len();
            for candidate in deferred_candidates.into_iter().take(remaining_task_budget) {
                debug!("recluster: backfill deferred candidate {}", candidate);
                tasks.push(candidate);
            }
        }

        Ok(tasks)
    }

    /// Convert selected in-memory candidates into physical recluster parts.
    pub async fn materialize_task_candidates(
        &self,
        live_segments: &HashMap<&Location, usize>,
        selected: Vec<(ReclusterCandidateWindow, Vec<usize>)>,
    ) -> Result<(u64, ReclusterParts)> {
        if selected.is_empty() {
            return Ok((0, ReclusterParts::default()));
        }

        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));
        let mut selected_block_keys = HashSet::new();
        let mut removed_segment_infos: HashMap<usize, Arc<SegmentInfo>> = HashMap::new();
        let mut tasks = Vec::new();
        let mut selected_block_count = 0u64;

        for (window, task_indices) in selected {
            for task_idx in task_indices {
                let candidate = &window.tasks[task_idx];

                if candidate.selected_blocks.is_empty() {
                    // Repack-only path: remove cached segments and keep their blocks.
                    for (location, segment_info) in &window.segments {
                        let Some(segment_info) = segment_info else {
                            continue;
                        };
                        let current_segment_idx = live_segments[location];
                        selected_block_count += segment_info.blocks.len() as u64;
                        removed_segment_infos
                            .entry(current_segment_idx)
                            .or_insert_with(|| segment_info.clone());
                    }
                    continue;
                }

                let mut block_metas = Vec::with_capacity(candidate.selected_block_count());
                let mut total_rows = 0usize;
                let mut total_bytes = 0usize;
                let mut total_compressed = 0usize;

                for (window_pos, block_indices) in &candidate.selected_blocks {
                    let (location, segment_info) = &window.segments[*window_pos];
                    let current_segment_idx = live_segments[location];
                    let segment_info = segment_info
                        .as_ref()
                        .expect("selected segment should be cached");
                    removed_segment_infos
                        .entry(current_segment_idx)
                        .or_insert_with(|| segment_info.clone());
                    for &block_idx in block_indices {
                        let block_meta = segment_info.blocks[block_idx].clone();
                        selected_block_keys.insert((current_segment_idx, block_idx));
                        total_rows += block_meta.row_count as usize;
                        total_bytes += block_meta.block_size as usize;
                        total_compressed += block_meta.file_size as usize;
                        block_metas.push((None, block_meta));
                    }
                }

                let (stats, parts) = FuseTable::to_partitions(
                    Some(&self.schema),
                    &block_metas,
                    &column_nodes,
                    None,
                    None,
                );
                tasks.push(ReclusterTask {
                    parts,
                    stats,
                    total_rows,
                    total_bytes,
                    total_compressed,
                    level: candidate.output_level,
                    all_ordered: candidate.all_ordered,
                    partition_stats: window.partition_stats.clone(),
                });
                selected_block_count += block_metas.len() as u64;
            }
        }

        if removed_segment_infos.is_empty() {
            return Ok((0, ReclusterParts::default()));
        }

        let mut removed_segment_indexes = removed_segment_infos.keys().copied().collect::<Vec<_>>();
        removed_segment_indexes.sort_unstable_by(|a, b| b.cmp(a));

        let cluster_key_info = Some((self.properties.cluster_key_id, self.properties.cluster_type));
        let mut removed_segment_summary = Statistics::default();
        for segment_info in removed_segment_infos.values() {
            // Summary still comes from SegmentInfo, not normalized cached blocks.
            merge_statistics_mut(
                &mut removed_segment_summary,
                &segment_info.summary,
                cluster_key_info,
            );
        }

        let mut hll_requests = Vec::new();
        let mut remained_blocks = Vec::new();
        for (&segment_idx, segment_info) in &removed_segment_infos {
            let mut hll_block_indices = Vec::new();
            for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
                if selected_block_keys.contains(&(segment_idx, block_idx)) {
                    continue;
                }
                // HLL is needed only when a removed segment leaves remained blocks.
                hll_block_indices.push(block_idx);
                remained_blocks.push((
                    BlockIndex {
                        segment_idx,
                        block_idx,
                    },
                    block_meta.clone(),
                ));
            }
            if let Some(stats_meta) = segment_info.summary.additional_stats_meta.as_ref() {
                if !hll_block_indices.is_empty() {
                    hll_requests.push((
                        segment_idx,
                        stats_meta.location.clone(),
                        hll_block_indices,
                    ));
                }
            }
        }

        let mut hlls = self.gather_hlls(hll_requests).await?;
        let remained_blocks = remained_blocks
            .into_iter()
            .map(|(block_index, block_meta)| {
                let hll = hlls.remove(&block_index);
                (block_meta, hll)
            })
            .collect();

        Ok((selected_block_count, ReclusterParts {
            tasks,
            remained_blocks,
            removed_segment_indexes,
            removed_segment_summary,
        }))
    }

    fn build_recluster_task_candidates_for_indices(
        &self,
        group: ReclusterGroup,
        indices: Vec<usize>,
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        debug_assert!(task_budget > 0);
        let group_start = Instant::now();
        let block_count = indices.len();
        if block_count < 2 {
            return Ok(Vec::new());
        }
        if block_count == 2
            && indices
                .iter()
                .all(|idx| blocks[*idx].stats().level >= MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS)
        {
            debug!(
                "recluster: candidate selection group={} block_count={} skip_reason=high_level_two_blocks",
                group, block_count,
            );
            return Ok(Vec::new());
        }

        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        for &idx in &indices {
            total_rows += blocks[idx].meta.row_count;
            total_bytes += blocks[idx].meta.block_size;
        }
        // Physical small-block compaction intentionally takes precedence over strategy-specific
        // overlap depth gates. A compactable group is rewritten even when its Hilbert/vector depth
        // is below the recluster threshold, so RECLUSTER also converges fragmented layouts.
        if self
            .properties
            .block_thresholds
            .check_for_compact(total_rows as usize, total_bytes as usize)
            && total_bytes as usize <= self.properties.memory_threshold
        {
            let score = CandidateScore {
                selected_total_bytes: total_bytes as usize,
                max_depth: block_count,
                average_depth: block_count as f64,
            };
            return Ok(vec![task_candidate(group, score, &indices, blocks)]);
        }

        let candidates = self.strategy.fetch_task_candidates(
            &self.properties,
            group,
            &indices,
            blocks,
            task_budget,
        )?;

        debug!(
            "recluster: candidate selection group={} block_count={} task_count={} elapsed={:?}",
            group,
            block_count,
            candidates.len(),
            group_start.elapsed(),
        );

        Ok(candidates)
    }

    /// Fast-path acceptance for very deep, sufficiently large candidates.
    pub(crate) fn passes_early_accept(&self, candidate: &ReclusterTaskCandidate) -> bool {
        let mature_gate = (2.0 * self.properties.depth_threshold).min(MAX_RECLUSTER_DEPTH as f64);
        let early_accept_depth = (mature_gate + 1.0).max(self.properties.depth_threshold * 4.0);
        let min_task_bytes = self.large_task_bytes_threshold();
        candidate.score.max_depth as f64 >= early_accept_depth
            && candidate.score.selected_total_bytes >= min_task_bytes
    }

    fn large_task_bytes_threshold(&self) -> usize {
        self.properties.memory_threshold.saturating_mul(3) / 4
    }

    /// Group segments by partition before selecting strategy-specific windows, then split oversized
    /// windows evenly to bound the metadata decoded by each candidate probe.
    pub fn select_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<SelectedReclusterWindow>> {
        let windows = if self.properties.partition_key_count == 0 {
            self.strategy
                .select_segments(&self.properties, compact_segments, window_len)?
                .into_iter()
                .map(|segments| (None, segments))
                .collect::<Vec<_>>()
        } else {
            let mut segments_by_partition = BTreeMap::new();
            for (location, segment) in compact_segments {
                let Some(partition) = partition_values(
                    segment.summary.partition_stats.as_ref(),
                    self.properties.partition_key_count,
                ) else {
                    // Legacy segments without exact partition metadata cannot safely
                    // participate in a partition-local rewrite window.
                    continue;
                };
                segments_by_partition
                    .entry(partition.to_vec())
                    .or_insert_with(Vec::new)
                    .push((location.clone(), segment.clone()));
            }

            let mut windows = Vec::new();
            for (partition, segments) in segments_by_partition {
                let partition_stats = Some(PartitionStatistics::new(partition));
                windows.extend(
                    self.strategy
                        .select_segments(&self.properties, &segments, window_len)?
                        .into_iter()
                        .map(|segments| (partition_stats.clone(), segments)),
                );
            }
            windows
        };

        let mut split_windows = Vec::with_capacity(windows.len());
        for (partition_stats, window) in windows {
            let segment_count = window.len();
            let window_count = segment_count.div_ceil(MAX_RECLUSTER_WINDOW_SEGMENTS);
            if window_count <= 1 {
                split_windows.push(SelectedReclusterWindow {
                    segments: window,
                    partition_stats,
                });
                continue;
            }

            let base_window_len = segment_count / window_count;
            let larger_windows = segment_count % window_count;
            let mut segments = window.into_iter();
            for window_idx in 0..window_count {
                let window_len = base_window_len + usize::from(window_idx < larger_windows);
                split_windows.push(SelectedReclusterWindow {
                    segments: segments.by_ref().take(window_len).collect(),
                    partition_stats: partition_stats.clone(),
                });
            }
            debug!(
                "recluster: split oversized candidate window segments={} windows={} max_window_segments={}",
                segment_count, window_count, MAX_RECLUSTER_WINDOW_SEGMENTS,
            );
        }
        Ok(split_windows)
    }

    #[async_backtrace::framed]
    async fn gather_blocks(
        &self,
        compact_segments: Vec<(usize, Location, Arc<CompactSegmentInfo>)>,
        decode_runtime: Arc<Runtime>,
        decode_semaphore: Arc<Semaphore>,
    ) -> Result<Vec<Vec<ReclusterBlock>>> {
        let segment_count = compact_segments.len();
        let block_metas_cache = CacheManager::instance().get_segment_block_metas_cache();
        let tasks = compact_segments
            .into_iter()
            .map(|(segment_idx, location, v)| {
                let block_metas_cache = block_metas_cache.clone();
                move |permit| {
                    async move {
                        let _permit = permit;
                        // Reuse immutable block metas decoded from the same segment path
                        // across probes and queries.
                        let block_metas = match block_metas_cache
                            .as_ref()
                            .and_then(|cache| cache.get(location.0.as_str()))
                        {
                            Some(block_metas) => block_metas,
                            None => match block_metas_cache.as_ref() {
                                Some(cache) => cache.insert(location.0.clone(), v.block_metas()?),
                                None => Arc::new(v.block_metas()?),
                            },
                        };
                        Ok::<_, ErrorCode>((segment_idx, block_metas))
                    }
                    .in_span(Span::enter_with_local_parent(func_path!()))
                }
            });

        let joint = decode_runtime
            .try_spawn_batch_with_owned_semaphore(decode_semaphore, tasks)
            .await?;
        let segment_block_metas = futures::future::try_join_all(joint)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let mut blocks_by_segment = std::iter::repeat_with(Vec::new)
            .take(segment_count)
            .collect::<Vec<_>>();
        for (segment_idx, block_metas) in segment_block_metas {
            let mut segment_blocks = Vec::with_capacity(block_metas.len());
            for (block_idx, block_meta) in block_metas.iter().enumerate() {
                let stats = if block_meta.cluster_stats.as_ref().is_some_and(|stats| {
                    self.strategy
                        .can_reuse_cluster_stats(&self.properties, stats)
                }) {
                    ReclusterBlockStats::Original
                } else {
                    ReclusterBlockStats::Normalized(self.strategy.build_cluster_stats(
                        &self.properties,
                        block_meta.cluster_stats.as_ref(),
                        &block_meta.col_stats,
                    ))
                };
                segment_blocks.push(ReclusterBlock {
                    index: BlockIndex {
                        segment_idx,
                        block_idx,
                    },
                    meta: block_meta.clone(),
                    stats,
                });
            }
            blocks_by_segment[segment_idx] = segment_blocks;
        }
        Ok(blocks_by_segment)
    }

    #[async_backtrace::framed]
    async fn gather_hlls(
        &self,
        hlls: Vec<(usize, Location, Vec<usize>)>,
    ) -> Result<HashMap<BlockIndex, RawBlockHLL>> {
        if hlls.is_empty() {
            return Ok(HashMap::new());
        }

        let tasks = hlls.into_iter().map(|(segment_idx, (loc, ver), blocks)| {
            let dal = self.operator.clone();
            async move {
                let reader = MetaReaders::segment_stats_reader(dal);
                let load_params = LoadParams {
                    location: loc,
                    len_hint: None,
                    ver,
                    put_cache: true,
                };
                let stats = reader.read(&load_params).await?;
                Ok(blocks
                    .into_iter()
                    .filter_map(|block_idx| {
                        let hll = stats.block_hlls.get(block_idx)?;
                        let block_index = BlockIndex {
                            segment_idx,
                            block_idx,
                        };
                        Some((block_index, hll.clone()))
                    })
                    .collect::<Vec<_>>())
            }
            .in_span(Span::enter_with_local_parent(func_path!()))
        });

        let thread_nums = self.ctx.get_settings().get_max_threads()? as usize;

        let joint = execute_futures_in_parallel(
            tasks,
            thread_nums,
            thread_nums * 2,
            "convert-segments-worker".to_owned(),
        )
        .await?;
        Ok(joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect())
    }
}
