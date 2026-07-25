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
use std::fmt;
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
use databend_common_expression::ColumnRef;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_common_sql::parse_cluster_keys;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use indexmap::IndexSet;
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
use crate::operations::ReclusterMode;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::statistics::PreparedClusterKeyExpr;
use crate::statistics::RangeMaxTree;
use crate::statistics::VectorClusterInfo;
use crate::statistics::get_min_max_stats;
use crate::statistics::prepare_cluster_key_exprs;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;
use crate::statistics::vector_cluster_info_from_column;

/// Maximum recluster depth allowed when only two blocks remain.
/// For two-block layouts, repeated reclustering beyond this level
/// rarely improves data locality and may cause task churn.
const MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS: i32 = 2;
/// Maximum recluster level allowed for candidate selection.
/// Blocks that reach this level have already been rewritten many times, so
/// keep them out of future recluster tasks to avoid unbounded level growth.
const MAX_RECLUSTER_LEVEL: i32 = 32;
const SMALL_TABLE_RECLUSTER_BLOCK_COUNT: u64 = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ReclusterGroup {
    /// A single level forms its own group.
    Level(i32),
    /// Aggressive mode: a fixed maturity bin identified by its lower bound `lo`.
    Range(i32),
}

impl ReclusterGroup {
    /// Assign a block's recluster group for the given mode.
    fn assign(level: i32, mode: ReclusterMode) -> ReclusterGroup {
        match mode {
            ReclusterMode::Conservative => ReclusterGroup::Level(level),
            ReclusterMode::Aggressive if level == 0 => ReclusterGroup::Level(level),
            ReclusterMode::Aggressive => {
                // Aggressive recluster packs blocks into fixed maturity bins so each
                // round can pick tasks across a wider level span, letting high-overlap
                // blocks at high levels land in the same candidate group instead of
                // being split across narrow windows:
                //   - {1..=3}: young-ish blocks, room for early recluster.
                //   - {4..=8}: mature blocks.
                //   - {9..}  : high-maturity blocks (upper-bounded by MAX_RECLUSTER_LEVEL).
                debug_assert!(level > 0);
                let lo = match level {
                    1..=3 => 1,
                    4..=8 => 4,
                    _ => 9,
                };
                ReclusterGroup::Range(lo)
            }
        }
    }

    /// Output level for a selected task.
    ///
    /// NORMAL keeps the single level. FINAL takes the majority level of the
    /// selected blocks (ties pick the smaller level), representing the maturity
    /// of most blocks in the task. Rewrite still advances this by one level.
    fn output_level(self, task_indices: &[usize], blocks: &[&ReclusterBlock]) -> i32 {
        match self {
            ReclusterGroup::Level(level) => level,
            ReclusterGroup::Range(lo) => {
                let mut counts: BTreeMap<i32, usize> = BTreeMap::new();
                for &idx in task_indices {
                    let level = blocks[idx].stats().level;
                    *counts.entry(level).or_default() += 1;
                }
                let mut best = (lo, 0usize);
                for (level, count) in counts {
                    if count > best.1 {
                        best = (level, count);
                    }
                }
                best.0
            }
        }
    }
}

impl fmt::Display for ReclusterGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReclusterGroup::Level(level) => write!(f, "{}", level),
            ReclusterGroup::Range(1) => write!(f, "1-3"),
            ReclusterGroup::Range(4) => write!(f, "4-8"),
            ReclusterGroup::Range(9) => write!(f, "9+"),
            ReclusterGroup::Range(lo) => unreachable!("unexpected FINAL bin lower bound: {lo}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateScore {
    pub selected_total_bytes: usize,
    pub max_depth: usize,
    pub average_depth: f64,
}

impl CandidateScore {
    pub fn cmp_desc(&self, other: &Self) -> cmp::Ordering {
        // Score order: max depth, average depth, then bytes.
        self.max_depth
            .cmp(&other.max_depth)
            .then_with(|| {
                self.average_depth
                    .partial_cmp(&other.average_depth)
                    .unwrap_or(cmp::Ordering::Equal)
            })
            .then_with(|| self.selected_total_bytes.cmp(&other.selected_total_bytes))
    }
}

#[derive(Clone)]
pub(crate) struct ReclusterTaskCandidate {
    pub(crate) score: CandidateScore,
    // Empty means a rebuild-only repack candidate.
    selected_blocks: Vec<(usize, Vec<usize>)>,
    output_level: i32,
    all_ordered: bool,
}

impl ReclusterTaskCandidate {
    fn selected_block_count(&self) -> usize {
        self.selected_blocks
            .iter()
            .map(|(_, block_indices)| block_indices.len())
            .sum()
    }

    pub(crate) fn is_repack_only(&self) -> bool {
        self.selected_blocks.is_empty()
    }
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "output_level={} max_depth={} avg_depth={} selected_count={} bytes={}",
            self.output_level,
            self.score.max_depth,
            self.score.average_depth,
            self.selected_block_count(),
            self.score.selected_total_bytes,
        )
    }
}

#[derive(Clone, Default)]
pub struct ReclusterCandidateWindow {
    // Window locations plus cached SegmentInfo for positions touched by candidates.
    pub(crate) segments: Vec<(Location, Option<Arc<SegmentInfo>>)>,
    pub(crate) tasks: Vec<ReclusterTaskCandidate>,
}

impl ReclusterCandidateWindow {
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub fn task_score(&self, task_idx: usize) -> CandidateScore {
        self.tasks[task_idx].score
    }
}

#[derive(Clone, Default)]
pub struct ReclusterFinalCarry {
    // State reused across rounds of one RECLUSTER FINAL statement.
    pub(crate) pending: Vec<ReclusterCandidateWindow>,
    // Next fixed scan-range start.
    pub(crate) scan_cursor: usize,
    // Cached candidates must match this cluster key.
    pub(crate) cluster_key_id: u32,
    // Reuse block-meta decode workers across rounds of one FINAL statement.
    pub(crate) decode_runtime: Option<Arc<Runtime>>,
}

impl ReclusterFinalCarry {
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

/// Cluster statistics for a candidate block.
///
/// `Original` means the block already carries cluster statistics matching the
/// current cluster key, so selection can borrow them directly and write-back can
/// reuse the original `Arc<BlockMeta>` without cloning the (potentially large)
/// `ClusterStatistics`. `Normalized` holds statistics recomputed for a block
/// whose cached cluster key differs from the current one. Normalized stats are
/// only a selection-time view; they must not be persisted into unchanged block
/// metas because they do not prove that the physical block is ordered.
enum ReclusterBlockStats {
    Original,
    Normalized(ClusterStatistics),
}

struct ReclusterBlock {
    index: BlockIndex,
    meta: Arc<BlockMeta>,
    stats: ReclusterBlockStats,
}

impl ReclusterBlock {
    /// Cluster statistics used during candidate selection (read-only).
    fn stats(&self) -> &ClusterStatistics {
        match &self.stats {
            ReclusterBlockStats::Original => self
                .meta
                .cluster_stats
                .as_ref()
                .expect("Original implies matched cluster_stats"),
            ReclusterBlockStats::Normalized(stats) => stats,
        }
    }
}

#[derive(Clone)]
pub struct SelectedReclusterSegment {
    pub loc: SegmentLocation,
    pub info: Arc<CompactSegmentInfo>,
}

struct VectorReclusterSegment {
    segment: SelectedReclusterSegment,
    block_metas: Vec<Arc<BlockMeta>>,
    stats: ClusterStatistics,
}

#[derive(Clone)]
pub struct ReclusterMutator {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) operator: Operator,
    pub(crate) mode: ReclusterMode,
    pub(crate) depth_threshold: f64,
    pub(crate) block_thresholds: BlockThresholds,
    pub(crate) cluster_key_id: u32,
    pub(crate) schema: TableSchemaRef,
    pub(crate) max_tasks: usize,
    pub(crate) memory_threshold: usize,
    pub(crate) prepared_cluster_key_exprs: Vec<PreparedClusterKeyExpr>,
    pub(crate) cluster_key_types: Vec<DataType>,
    pub(crate) vector_cluster_info: Option<VectorClusterInfo>,
}

impl ReclusterMutator {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
        mode: ReclusterMode,
    ) -> Result<Self> {
        let schema = table.schema_with_stream();
        let cluster_key_id = table.cluster_key_id().unwrap();
        let block_thresholds = table.get_block_thresholds();

        let depth_threshold = table
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

        let settings = ctx.get_settings();
        let memory_threshold = Self::recluster_memory_threshold(ctx.as_ref())?;
        let mut max_tasks = 1;
        let cluster = ctx.get_cluster();
        if !cluster.is_empty() && settings.get_enable_distributed_recluster()? {
            max_tasks = cluster.nodes.len();
        }

        // safe to unwrap
        let cluster_keys = table.resolve_cluster_keys().unwrap();
        let full_cluster_key_exprs =
            parse_cluster_keys(ctx.clone(), Arc::new(table.clone()), cluster_keys)?;
        let vector_cluster_info = vector_cluster_info_from_exprs(table, &full_cluster_key_exprs)?;
        let cluster_key_exprs = scalar_cluster_key_exprs(full_cluster_key_exprs);
        if cluster_key_exprs.is_empty() && vector_cluster_info.is_none() {
            return Err(ErrorCode::Internal(
                "recluster requires non-empty cluster key expressions",
            ));
        }
        let cluster_key_types = cluster_key_exprs
            .iter()
            .map(|v| v.data_type().clone())
            .collect::<Vec<_>>();
        let prepared_cluster_key_exprs =
            prepare_cluster_key_exprs(&cluster_key_exprs, schema.as_ref());

        Ok(Self {
            ctx,
            operator: table.get_operator(),
            mode,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            memory_threshold,
            prepared_cluster_key_exprs,
            cluster_key_types,
            vector_cluster_info,
        })
    }

    /// Used for tests.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        schema: TableSchemaRef,
        cluster_key_exprs: Vec<Expr<usize>>,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_id: u32,
        max_tasks: usize,
        mode: ReclusterMode,
        vector_cluster_info: Option<VectorClusterInfo>,
    ) -> Self {
        let cluster_key_exprs = scalar_cluster_key_exprs(cluster_key_exprs);
        assert!(
            !cluster_key_exprs.is_empty() || vector_cluster_info.is_some(),
            "recluster requires non-empty cluster key expressions"
        );
        let cluster_key_types = cluster_key_exprs
            .iter()
            .map(|expr| expr.data_type().clone())
            .collect();
        let memory_threshold = ctx
            .get_settings()
            .get_recluster_block_size()
            .expect("get recluster_block_size setting for recluster mutator")
            as usize;
        let prepared_cluster_key_exprs =
            prepare_cluster_key_exprs(&cluster_key_exprs, schema.as_ref());
        Self {
            ctx,
            operator,
            mode,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            memory_threshold,
            prepared_cluster_key_exprs,
            cluster_key_types,
            vector_cluster_info,
        }
    }

    #[async_backtrace::framed]
    pub async fn probe_candidate_window(
        &self,
        compact_segments: Vec<SelectedReclusterSegment>,
        task_budget: usize,
        decode_runtime: Arc<Runtime>,
        decode_semaphore: Arc<Semaphore>,
    ) -> Result<ReclusterCandidateWindow> {
        debug_assert!(task_budget > 0);
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
                        self.cluster_key_id,
                    ) == cmp::Ordering::Greater
                })
            };
            let selected_segment_count = window_segment_infos.len();
            let target_segment_count =
                total_block_count.div_ceil(self.block_thresholds.block_per_segment);
            let compactable_repack = total_block_count > 0
                && selected_segment_count > 1
                && target_segment_count < selected_segment_count;
            let unordered_repack =
                self.mode == ReclusterMode::Conservative && all_original_stats && unordered();
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
        let mut blocks_map: BTreeMap<ReclusterGroup, Vec<usize>> = BTreeMap::new();
        for (idx, block) in blocks.iter().enumerate() {
            let level = block.stats().level;
            if level < 0 {
                continue;
            }
            if level >= MAX_RECLUSTER_LEVEL {
                // Terminal-level blocks are excluded from further rewrite tasks.
                continue;
            }
            blocks_map
                .entry(ReclusterGroup::assign(level, self.mode))
                .or_default()
                .push(idx);
        }

        let mut tasks: Vec<ReclusterTaskCandidate> = Vec::new();
        let mut deferred_group_candidates = None;
        let large_task_bytes_threshold = self.large_task_bytes_threshold();
        for (group, indices) in blocks_map {
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
            if candidates.is_empty() {
                continue;
            }

            if tasks.is_empty() && deferred_group_candidates.is_none() {
                // When the first constructible group is too small, defer it once so
                // later groups get a chance to consume this window's budget first.
                let selected_total_bytes = candidates
                    .iter()
                    .map(|candidate| candidate.score.selected_total_bytes)
                    .sum::<usize>();
                if selected_total_bytes < large_task_bytes_threshold {
                    debug!(
                        "recluster: defer low-level group={} selected_bytes={} task_count={} skip_reason=deferred_low_level_batch",
                        group,
                        selected_total_bytes,
                        candidates.len(),
                    );
                    deferred_group_candidates = Some(candidates);
                    continue;
                }
            }

            tasks.extend(candidates.into_iter().take(remaining_task_budget));
        }

        if tasks.len() < task_budget {
            if let Some(candidates) = deferred_group_candidates {
                let remaining_task_budget = task_budget - tasks.len();
                for candidate in candidates.into_iter().take(remaining_task_budget) {
                    debug!("recluster: backfill deferred candidate {}", candidate);
                    tasks.push(candidate);
                }
            }
        }

        Ok(tasks)
    }

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
                });
                selected_block_count += block_metas.len() as u64;
            }
        }

        if removed_segment_infos.is_empty() {
            return Ok((0, ReclusterParts::default()));
        }

        let mut removed_segment_indexes = removed_segment_infos.keys().copied().collect::<Vec<_>>();
        removed_segment_indexes.sort_unstable_by(|a, b| b.cmp(a));

        let default_cluster_key_id = Some(self.cluster_key_id);
        let mut removed_segment_summary = Statistics::default();
        for segment_info in removed_segment_infos.values() {
            // Summary still comes from SegmentInfo, not normalized cached blocks.
            merge_statistics_mut(
                &mut removed_segment_summary,
                &segment_info.summary,
                default_cluster_key_id,
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

    fn recluster_memory_threshold(ctx: &dyn TableContext) -> Result<usize> {
        let settings = ctx.get_settings();
        let recluster_block_size = settings.get_recluster_block_size()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()? as usize;
        if max_memory_usage == 0 {
            return Ok(recluster_block_size);
        }
        let memory_usage = ctx.get_nodes_memory_usage();
        let memory_budget = max_memory_usage.saturating_sub(memory_usage) * 30 / 100;
        // No memory budget left: fail with a clear reason.
        if memory_budget == 0 {
            return Err(ErrorCode::MemoryExceedsLimit(format!(
                "Not enough memory for recluster: max_memory_usage = {}, used = {}.",
                max_memory_usage, memory_usage
            )));
        }
        // Whether a task actually fits is checked during task selection using
        // real block sizes, so small-block tables are not rejected here under low memory.
        Ok(recluster_block_size.min(memory_budget))
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

        let mut total_rows = 0;
        let mut total_bytes = 0;

        for &i in &indices {
            let block = &blocks[i];
            total_rows += block.meta.row_count;
            total_bytes += block.meta.block_size;
        }

        if self
            .block_thresholds
            .check_for_compact(total_rows as usize, total_bytes as usize)
            && total_bytes as usize <= self.memory_threshold
        {
            // Small compactable groups are treated as one dense overlap.
            let score = CandidateScore {
                selected_total_bytes: total_bytes as usize,
                max_depth: block_count,
                average_depth: block_count as f64,
            };
            return Ok(vec![Self::task_candidate(group, score, &indices, blocks)]);
        }

        let candidates = if let Some(vector_cluster_info) = self.vector_cluster_key() {
            self.fetch_vector_task_candidates(
                group,
                &indices,
                blocks,
                vector_cluster_info,
                task_budget,
            )?
        } else {
            let mut points_map = HashMap::new();
            for (local_idx, &i) in indices.iter().enumerate() {
                // Use a group-local block index (0..block_count) as the point key so
                // dense lookup vectors are sized by the group block count, not the
                // window-global block index range. `indices` maps each local index
                // back to its `blocks` index.
                let stats = blocks[i].stats();
                let point: &mut (Vec<usize>, Vec<usize>) =
                    points_map.entry(stats.min().as_slice()).or_default();
                point.0.push(local_idx);
                let point = points_map.entry(stats.max().as_slice()).or_default();
                point.1.push(local_idx);
            }
            self.fetch_max_depth_candidates(group, points_map, &indices, blocks, task_budget)?
        };
        debug!(
            "recluster: candidate selection group={} block_count={} task_count={} elapsed={:?}",
            group,
            block_count,
            candidates.len(),
            group_start.elapsed(),
        );

        Ok(candidates)
    }

    fn task_candidate(
        group: ReclusterGroup,
        score: CandidateScore,
        task_indices: &[usize],
        blocks: &[&ReclusterBlock],
    ) -> ReclusterTaskCandidate {
        ReclusterTaskCandidate {
            score,
            selected_blocks: Self::selected_blocks_by_segment(task_indices, blocks),
            output_level: group.output_level(task_indices, blocks),
            all_ordered: task_indices
                .iter()
                .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original)),
        }
    }

    fn selected_blocks_by_segment(
        task_indices: &[usize],
        blocks: &[&ReclusterBlock],
    ) -> Vec<(usize, Vec<usize>)> {
        let mut selected_blocks = Vec::<(usize, Vec<usize>)>::new();
        for &idx in task_indices {
            let block = &blocks[idx];
            if let Some((_, block_indices)) = selected_blocks
                .iter_mut()
                .find(|(segment_idx, _)| *segment_idx == block.index.segment_idx)
            {
                block_indices.push(block.index.block_idx);
            } else {
                selected_blocks.push((block.index.segment_idx, vec![block.index.block_idx]));
            }
        }
        selected_blocks
    }

    fn vector_cluster_key(&self) -> Option<&VectorClusterInfo> {
        self.vector_cluster_info.as_ref()
    }

    fn fetch_vector_task_candidates(
        &self,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        vector_cluster_info: &VectorClusterInfo,
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        let block_count = indices.len();
        let mut overlaps = vec![IndexSet::new(); block_count];
        for (local_idx, overlap) in overlaps.iter_mut().enumerate() {
            overlap.insert(local_idx);
        }

        for left in 0..block_count {
            for right in left + 1..block_count {
                let left_block = blocks[indices[left]];
                let right_block = blocks[indices[right]];
                if !self.cluster_key_types.is_empty()
                    && !self.scalar_cluster_stats_overlap(left_block.stats(), right_block.stats())
                {
                    continue;
                }

                let left_stat =
                    block_meta_vector_stats(left_block.meta.as_ref(), vector_cluster_info);
                let right_stat =
                    block_meta_vector_stats(right_block.meta.as_ref(), vector_cluster_info);
                let vector_overlap = match (left_stat, right_stat) {
                    (Some(left_stat), Some(right_stat)) => {
                        left_stat.spheres_overlap(right_stat, vector_cluster_info.distance_type)?
                    }
                    // Missing vector stats must stay conservative.
                    _ => true,
                };
                if vector_overlap {
                    overlaps[left].insert(right);
                    overlaps[right].insert(left);
                }
            }
        }

        let mut max_depth = 0usize;
        let mut sum_depth = 0usize;
        for overlap in &overlaps {
            let depth = overlap.len();
            max_depth = max_depth.max(depth);
            sum_depth += depth;
        }
        let average_depth = (10000.0 * sum_depth as f64 / block_count as f64).round() / 10000.0;
        let vector_depth_threshold = self.depth_threshold.min(1.0);
        if !Self::passes_depth_gate(vector_depth_threshold, average_depth, max_depth) {
            debug!(
                "recluster: vector candidate selection group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=below_vector_depth_gate",
                group, block_count, average_depth, max_depth,
            );
            return Ok(Vec::new());
        }

        let mut depth_order = overlaps
            .iter()
            .enumerate()
            .map(|(local_idx, overlap)| (local_idx, overlap.len()))
            .filter(|(_, depth)| *depth > 1)
            .collect::<Vec<_>>();
        depth_order.sort_by(|(left_idx, left_depth), (right_idx, right_depth)| {
            right_depth
                .cmp(left_depth)
                .then_with(|| left_idx.cmp(right_idx))
        });

        let mut candidates = Vec::new();
        let mut used_blocks = vec![false; block_count];
        for (seed, depth) in depth_order {
            if candidates.len() >= task_budget {
                break;
            }

            let mut local_indices = overlaps[seed]
                .iter()
                .copied()
                .filter(|local_idx| !used_blocks[*local_idx])
                .collect::<Vec<_>>();
            local_indices.sort_by(|left, right| {
                overlaps[*right]
                    .len()
                    .cmp(&overlaps[*left].len())
                    .then_with(|| left.cmp(right))
            });
            if local_indices.len() < 2 {
                continue;
            }

            let mut task_bytes = 0usize;
            let mut selected_local_indices = Vec::new();
            for local_idx in local_indices {
                let block_size = blocks[indices[local_idx]].meta.block_size as usize;
                if !selected_local_indices.is_empty()
                    && task_bytes.saturating_add(block_size) > self.memory_threshold
                {
                    break;
                }
                task_bytes = task_bytes.saturating_add(block_size);
                selected_local_indices.push(local_idx);
            }
            if selected_local_indices.len() < 2 {
                continue;
            }

            for &local_idx in &selected_local_indices {
                used_blocks[local_idx] = true;
            }
            let task_indices = selected_local_indices
                .into_iter()
                .map(|local_idx| indices[local_idx])
                .collect::<Vec<_>>();
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                max_depth: depth,
                average_depth,
            };
            candidates.push(Self::task_candidate(group, score, &task_indices, blocks));
        }

        debug!(
            "recluster: vector candidate selection group={} block_count={} avg_depth={} depth_threshold={} max_depth={} task_count={}",
            group,
            block_count,
            average_depth,
            vector_depth_threshold,
            max_depth,
            candidates.len(),
        );

        Ok(candidates)
    }

    fn scalar_cluster_stats_overlap(
        &self,
        left: &ClusterStatistics,
        right: &ClusterStatistics,
    ) -> bool {
        let left_min = left.min();
        let left_max = left.max();
        let right_min = right.min();
        let right_max = right.max();

        let cluster_key_count = left_min.len();
        if cluster_key_count == 0 {
            return true;
        }

        if left_max.len() < cluster_key_count
            || right_min.len() < cluster_key_count
            || right_max.len() < cluster_key_count
        {
            return true;
        }

        for key_index in 0..cluster_key_count {
            if !scalar_le(&left_min[key_index], &right_max[key_index])
                || !scalar_le(&right_min[key_index], &left_max[key_index])
            {
                return false;
            }
        }
        true
    }

    fn passes_depth_gate(depth_threshold: f64, average_depth: f64, max_depth: usize) -> bool {
        let mature_gate = if depth_threshold <= MIN_RECLUSTER_DEPTH as f64 {
            depth_threshold
        } else {
            (2.0 * depth_threshold).min(MAX_RECLUSTER_DEPTH as f64)
        };
        average_depth > depth_threshold || max_depth as f64 > mature_gate
    }

    pub(crate) fn passes_early_accept(&self, candidate: &ReclusterTaskCandidate) -> bool {
        let mature_gate = (2.0 * self.depth_threshold).min(MAX_RECLUSTER_DEPTH as f64);
        let early_accept_depth = (mature_gate + 1.0).max(self.depth_threshold * 4.0);
        let min_task_bytes = self.large_task_bytes_threshold();
        candidate.score.max_depth as f64 >= early_accept_depth
            && candidate.score.selected_total_bytes >= min_task_bytes
    }

    fn large_task_bytes_threshold(&self) -> usize {
        self.memory_threshold.saturating_mul(3) / 4
    }

    /// Cut the candidate segments into segment-disjoint windows of at most
    /// `window_len` (non-zero) segments each. A window may exceed `window_len`
    /// when same-point segments are kept together or a small tail is folded in,
    /// so a `LIMIT` is a soft bound.
    pub fn select_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        // Segment selection follows the cluster key shape:
        // - vector-only: use vector sphere overlap directly because there is no scalar range.
        // - scalar-only: use scalar ClusterStatistics min/max overlap.
        // - mixed: first build scalar windows, then refine each window by vector sphere overlap.
        // This avoids running scalar overlap again during vector refinement.
        let vector_cluster_info = self.vector_cluster_key();
        if vector_cluster_info.is_some() && self.cluster_key_types.is_empty() {
            return self.select_vector_only_segments(compact_segments, window_len);
        }

        let scalar_windows = self.select_scalar_segments(compact_segments, window_len)?;
        if let Some(vector_cluster_info) = vector_cluster_info {
            self.refine_scalar_windows_by_vector(scalar_windows, vector_cluster_info, window_len)
        } else {
            Ok(scalar_windows)
        }
    }

    fn select_scalar_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        debug_assert!(window_len > 0);
        let block_per_seg = self.block_thresholds.block_per_segment;

        let mut total_blocks = 0;
        let mut segments = vec![None; compact_segments.len()];
        let mut segment_points: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();

        // Phase 1: collect segment ranges for the sweep-line selection. Large
        // unclustered segments are skipped because rewriting them is not useful.
        for (i, (loc, compact_segment)) in compact_segments.iter().enumerate() {
            let stats = self.build_cluster_stats_for_recluster(
                compact_segment.summary.cluster_stats.as_ref(),
                &compact_segment.summary.col_stats,
            );
            let level = stats.level;

            if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
                continue;
            }

            total_blocks += compact_segment.summary.block_count as usize;
            segment_points
                .entry(stats.min().clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));
            segment_points
                .entry(stats.max().clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            segments[i] = Some(SelectedReclusterSegment {
                loc: loc.clone(),
                info: compact_segment.clone(),
            });
        }

        let mut windows: Vec<(IndexSet<usize>, usize)> = Vec::new();

        // Phase 2: sweep the cluster-key points and cut the candidate segments
        // into consecutive, segment-disjoint windows. Each segment joins exactly
        // one window (at its start point), so windows never share a segment and
        // tasks never read the same block twice. A window closes at `window_len`;
        // `prev_window` holds the last closed one so a small tail can fold into it.
        let mut unfinished_intervals = BTreeMap::new();
        let mut prev_window: Option<(IndexSet<usize>, usize)> = None;
        let mut current_window: IndexSet<usize> = IndexSet::new();
        let mut current_window_max_depth = 0usize;
        let (keys, values): (Vec<_>, Vec<_>) = segment_points.into_iter().unzip();
        let sorted_indices = compare_scalars(&keys, &self.cluster_key_types)?;

        for idx in sorted_indices {
            let start = &values[idx as usize].0;
            let end = &values[idx as usize].1;
            let point_depth = Self::calc_point_depth(unfinished_intervals.len(), start, end);

            // A window is just a contiguous run of segments, so partitioning the
            // run keeps windows segment-disjoint without any extra bookkeeping.
            // Depth only contributes to the window score.
            current_window_max_depth = current_window_max_depth.max(point_depth);
            current_window.extend(start.iter().copied());

            if current_window.len() >= window_len {
                // Emit the previously closed window and rotate the current
                // window into `prev_window`.
                if let Some((segs, depth)) = prev_window.take() {
                    windows.push((segs, depth));
                }
                prev_window = Some((
                    std::mem::take(&mut current_window),
                    current_window_max_depth,
                ));
                current_window_max_depth = 0;
            }

            start.iter().for_each(|&idx| {
                unfinished_intervals.insert(idx, point_depth);
            });
            end.iter().for_each(|idx| {
                unfinished_intervals.remove(idx);
            });
        }

        // Fold the trailing window into the last closed one to avoid a tiny
        // fragment; this may push it past `window_len` (an acceptable soft
        // overshoot under `LIMIT`).
        if let Some((mut prev_segs, prev_depth)) = prev_window.take() {
            prev_segs.extend(current_window);
            windows.push((prev_segs, prev_depth.max(current_window_max_depth)));
        } else if !current_window.is_empty() {
            windows.push((current_window, current_window_max_depth));
        }

        // Try the deepest windows first; for equal depth, prefer the larger
        // window because it gives candidate probing more room to build tasks.
        windows.sort_by(|(left_indices, left_depth), (right_indices, right_depth)| {
            right_depth
                .cmp(left_depth)
                .then_with(|| right_indices.len().cmp(&left_indices.len()))
        });

        if self.mode == ReclusterMode::Conservative {
            // Conservative mode is kept for legacy tables without
            // `aggressive_recluster`. Probe only the deepest window per round to
            // avoid over-reclustering old tables and creating a sharp behavior
            // gap from the pre-option strategy.
            windows.truncate(1);
        }

        debug!(
            "recluster: segment selection windows segments={} blocks={} window_count={}",
            compact_segments.len(),
            total_blocks,
            windows.len(),
        );

        // Convert index windows back to segment objects.
        Ok(windows
            .into_iter()
            .map(|(selected_indices, _)| {
                selected_indices
                    .into_iter()
                    .filter_map(|i| segments[i].clone())
                    .collect::<Vec<_>>()
            })
            .filter(|window| !window.is_empty())
            .collect())
    }

    fn select_vector_only_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        let Some(vector_cluster_info) = self.vector_cluster_key() else {
            return Ok(vec![]);
        };
        let block_per_seg = self.block_thresholds.block_per_segment;
        let window_len = window_len.max(1);
        let mut total_blocks = 0;
        let mut vector_segments = Vec::new();
        let mut small_segments = IndexSet::new();

        for (loc, compact_segment) in compact_segments {
            let stats = self.build_cluster_stats_for_recluster(
                compact_segment.summary.cluster_stats.as_ref(),
                &compact_segment.summary.col_stats,
            );
            let level = stats.level;
            if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
                continue;
            }
            let segment = SelectedReclusterSegment {
                loc: loc.clone(),
                info: compact_segment.clone(),
            };

            let block_metas = compact_segment.block_metas()?;
            let current_blocks_num = compact_segment.summary.block_count as usize;
            let segment_idx = vector_segments.len();
            if current_blocks_num < block_per_seg {
                small_segments.insert(segment_idx);
            }
            total_blocks += current_blocks_num;
            vector_segments.push(VectorReclusterSegment {
                segment,
                block_metas,
                stats,
            });
        }

        let mut windows = Vec::new();
        let mut seen_windows = HashSet::new();
        let mut covered_segments = IndexSet::new();

        if vector_segments.len() > 1 && total_blocks > block_per_seg {
            let overlaps =
                self.build_vector_segment_overlaps(&vector_segments, vector_cluster_info, false)?;
            let mut depth_order = overlaps
                .iter()
                .enumerate()
                .map(|(idx, overlap)| (idx, overlap.len()))
                .filter(|(_, depth)| *depth > 1)
                .collect::<Vec<_>>();
            depth_order.sort_by(|(left_idx, left_depth), (right_idx, right_depth)| {
                right_depth
                    .cmp(left_depth)
                    .then_with(|| left_idx.cmp(right_idx))
            });

            for (idx, depth) in depth_order {
                let mut selected_indices = overlaps[idx].iter().copied().collect::<Vec<_>>();
                selected_indices.sort_by(|left_idx, right_idx| {
                    overlaps[*right_idx]
                        .len()
                        .cmp(&overlaps[*left_idx].len())
                        .then_with(|| left_idx.cmp(right_idx))
                });
                selected_indices.truncate(window_len);

                let mut window_key = selected_indices.clone();
                window_key.sort_unstable();
                if seen_windows.insert(window_key) {
                    covered_segments.extend(selected_indices.iter().copied());
                    windows.push((selected_indices, depth));
                }
            }

            debug!(
                "recluster: vector segment selection overlap windows segments={} blocks={} window_count={} covered_segments={}",
                vector_segments.len(),
                total_blocks,
                windows.len(),
                covered_segments.len(),
            );
        }

        let mut fallback_indices = Vec::new();
        for idx in 0..vector_segments.len() {
            if !covered_segments.contains(&idx) || small_segments.contains(&idx) {
                fallback_indices.push(idx);
            }
        }

        for selected_indices in fallback_indices
            .chunks(window_len)
            .map(|chunk| chunk.to_vec())
        {
            let mut window_key = selected_indices.clone();
            window_key.sort_unstable();
            if seen_windows.insert(window_key) {
                windows.push((selected_indices, 0));
            }
        }

        Ok(windows
            .into_iter()
            .map(|(selected_indices, _)| {
                selected_indices
                    .into_iter()
                    .map(|i| vector_segments[i].segment.clone())
                    .collect::<Vec<_>>()
            })
            .filter(|window| !window.is_empty())
            .collect())
    }

    fn refine_scalar_windows_by_vector(
        &self,
        scalar_windows: Vec<Vec<SelectedReclusterSegment>>,
        vector_cluster_info: &VectorClusterInfo,
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        let window_len = window_len.max(1);
        let mut refined_windows = Vec::with_capacity(scalar_windows.len());
        let mut seen_windows = HashSet::new();

        for scalar_window in scalar_windows {
            if scalar_window.len() < 2 {
                if seen_windows.insert(Self::segment_window_key(&scalar_window)) {
                    refined_windows.push(scalar_window);
                }
                continue;
            }

            let mut vector_segments = Vec::with_capacity(scalar_window.len());
            for segment in scalar_window.iter().cloned() {
                let block_metas = segment.info.block_metas()?;
                let stats = self.build_cluster_stats_for_recluster(
                    segment.info.summary.cluster_stats.as_ref(),
                    &segment.info.summary.col_stats,
                );
                vector_segments.push(VectorReclusterSegment {
                    segment,
                    block_metas,
                    stats,
                });
            }

            let overlaps =
                self.build_vector_segment_overlaps(&vector_segments, vector_cluster_info, true)?;

            let mut visited = vec![false; vector_segments.len()];
            for start in 0..vector_segments.len() {
                if visited[start] {
                    continue;
                }

                let mut stack = vec![start];
                let mut component = Vec::new();
                visited[start] = true;
                while let Some(idx) = stack.pop() {
                    component.push(idx);
                    for next in overlaps[idx].iter().copied() {
                        if !visited[next] {
                            visited[next] = true;
                            stack.push(next);
                        }
                    }
                }

                component.sort_unstable_by_key(|idx| vector_segments[*idx].segment.loc.segment_idx);
                for chunk in component.chunks(window_len) {
                    let window = chunk
                        .iter()
                        .map(|idx| vector_segments[*idx].segment.clone())
                        .collect::<Vec<_>>();
                    if seen_windows.insert(Self::segment_window_key(&window)) {
                        refined_windows.push(window);
                    }
                }
            }
        }

        Ok(refined_windows)
    }

    fn build_vector_segment_overlaps(
        &self,
        vector_segments: &[VectorReclusterSegment],
        vector_cluster_info: &VectorClusterInfo,
        require_scalar_overlap: bool,
    ) -> Result<Vec<IndexSet<usize>>> {
        let mut overlaps = vec![IndexSet::new(); vector_segments.len()];
        for (idx, overlap) in overlaps.iter_mut().enumerate() {
            overlap.insert(idx);
        }

        for left in 0..vector_segments.len() {
            for right in left + 1..vector_segments.len() {
                if require_scalar_overlap
                    && !self.scalar_cluster_stats_overlap(
                        &vector_segments[left].stats,
                        &vector_segments[right].stats,
                    )
                {
                    continue;
                }

                if vector_segment_spheres_overlap(
                    &vector_segments[left].block_metas,
                    &vector_segments[right].block_metas,
                    vector_cluster_info,
                )? {
                    overlaps[left].insert(right);
                    overlaps[right].insert(left);
                }
            }
        }

        Ok(overlaps)
    }

    fn segment_window_key(window: &[SelectedReclusterSegment]) -> Vec<usize> {
        let mut key = window
            .iter()
            .map(|segment| segment.loc.segment_idx)
            .collect::<Vec<_>>();
        key.sort_unstable();
        key
    }

    fn build_cluster_stats_for_recluster(
        &self,
        cluster_stats: Option<&ClusterStatistics>,
        col_stats: &StatisticsOfColumns,
    ) -> ClusterStatistics {
        if let Some(stats) = cluster_stats {
            if stats.cluster_key_id == self.cluster_key_id {
                return stats.clone();
            }
        }

        let (min_stats, max_stats) = get_min_max_stats(
            &self.prepared_cluster_key_exprs,
            col_stats,
            cluster_stats,
            Some(self.cluster_key_id),
        );

        ClusterStatistics::new(self.cluster_key_id, min_stats, max_stats, 0)
    }

    /// Decide how to carry cluster statistics for a candidate block. When the
    /// cached cluster stats already match the current cluster key, return
    /// `Original` so selection borrows them and write-back reuses the original
    /// `Arc<BlockMeta>` without cloning stats. Otherwise recompute them.
    fn recluster_block_stats(&self, block_meta: &BlockMeta) -> ReclusterBlockStats {
        if block_meta
            .cluster_stats
            .as_ref()
            .is_some_and(|stats| stats.cluster_key_id == self.cluster_key_id)
        {
            ReclusterBlockStats::Original
        } else {
            ReclusterBlockStats::Normalized(self.build_cluster_stats_for_recluster(
                block_meta.cluster_stats.as_ref(),
                &block_meta.col_stats,
            ))
        }
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
                // Keep stats handling beside the original block meta. Stats are
                // borrowed when they already match the cluster key, and only
                // recomputed otherwise.
                let stats = self.recluster_block_stats(block_meta);
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

    fn fetch_max_depth_candidates(
        &self,
        group: ReclusterGroup,
        points_map: HashMap<&[Scalar], (Vec<usize>, Vec<usize>)>,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        debug_assert!(!points_map.is_empty());
        let block_count = indices.len();
        let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
        let order = compare_scalars(&keys, &self.cluster_key_types)?;

        // PASS 1: sweep sorted points and record folded point depths plus each
        // block's open/close positions.
        let num_points = order.len();
        let mut point_depths = vec![0usize; num_points];
        let unset_pos = usize::MAX;
        let mut open_pos = vec![unset_pos; block_count];
        let mut close_pos = vec![unset_pos; block_count];
        let mut live = vec![false; block_count];
        let mut live_count = 0usize;
        let mut max_depth = 0;
        // Peak tuple: (max point position, max depth, width of depth > threshold region).
        let mut peaks = Vec::new();
        let mut current_peak: Option<(usize, usize, usize)> = None;
        for i in 0..num_points {
            let value_idx = order[i] as usize;
            let (starts, ends) = &values[value_idx];
            let point_depth = Self::calc_point_depth(live_count, starts, ends);
            point_depths[i] = point_depth;
            if point_depth > max_depth {
                max_depth = point_depth;
            }
            if point_depth as f64 > self.depth_threshold {
                match &mut current_peak {
                    Some((peak_pos, peak_depth, width)) => {
                        *width += 1;
                        if point_depth > *peak_depth {
                            *peak_pos = i;
                            *peak_depth = point_depth;
                        }
                    }
                    None => current_peak = Some((i, point_depth, 1)),
                }
            } else if let Some(peak) = current_peak.take() {
                peaks.push(peak);
            }
            for &s in starts {
                if !live[s] {
                    live[s] = true;
                    live_count += 1;
                }
                open_pos[s] = i;
            }
            for &e in ends {
                if live[e] {
                    live[e] = false;
                    live_count -= 1;
                    close_pos[e] = i;
                }
            }
        }
        if let Some(peak) = current_peak {
            peaks.push(peak);
        }

        // PASS 2: gate by each interval's max folded point depth.
        let mut sum_depth = 0usize;
        let mut closed = 0usize;
        let seg = RangeMaxTree::build(&point_depths);
        for idx in 0..block_count {
            if open_pos[idx] == unset_pos {
                continue;
            }
            let open = open_pos[idx];
            let close = close_pos[idx];
            // Malformed stats can leave an interval unclosed or reversed; skip
            // this group instead of feeding an invalid range into task building.
            if close == unset_pos || close < open {
                debug!(
                    "recluster: candidate selection detail group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=invalid_depth_range",
                    group,
                    block_count,
                    f64::NAN,
                    max_depth,
                );
                return Ok(Vec::new());
            }
            sum_depth += seg.range_max(open, close);
            closed += 1;
        }
        debug_assert!(closed > 0);
        let average_depth = (10000.0 * sum_depth as f64 / closed as f64).round() / 10000.0;

        if !Self::passes_depth_gate(self.depth_threshold, average_depth, max_depth) {
            debug!(
                "recluster: candidate selection detail group={} block_count={} average_depth={} max_depth={} selected_count=0 skip_reason=below_hotspot_depth_gate",
                group, block_count, average_depth, max_depth,
            );
            return Ok(Vec::new());
        }

        peaks.sort_by(
            |(left_pos, left_depth, left_width), (right_pos, right_depth, right_width)| {
                right_depth
                    .cmp(left_depth)
                    .then_with(|| right_width.cmp(left_width))
                    .then_with(|| left_pos.cmp(right_pos))
            },
        );

        let push_task = |candidates: &mut Vec<ReclusterTaskCandidate>,
                         used_blocks: &mut [bool],
                         local_indices: Vec<usize>,
                         task_bytes: usize,
                         max_depth: usize| {
            for &local_idx in &local_indices {
                used_blocks[local_idx] = true;
            }
            let task_indices = local_indices
                .into_iter()
                .map(|local_idx| indices[local_idx])
                .collect::<Vec<_>>();
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                max_depth,
                average_depth,
            };
            candidates.push(Self::task_candidate(group, score, &task_indices, blocks));
        };

        let mut candidates = Vec::new();
        let mut used_blocks = vec![false; block_count];

        for &(peak_pos, peak_depth, _) in &peaks {
            if candidates.len() >= task_budget {
                break;
            }

            // Treat adjacent peak-depth points as one hotspot plateau, so blocks
            // covering the same peak area stay ahead of side expansion.
            let mut hotspot_left = peak_pos;
            while hotspot_left > 0 && point_depths[hotspot_left - 1] == peak_depth {
                hotspot_left -= 1;
            }
            let mut hotspot_right = peak_pos;
            while hotspot_right + 1 < num_points && point_depths[hotspot_right + 1] == peak_depth {
                hotspot_right += 1;
            }

            // Pack hotspot-overlapping blocks first. Tasks split only on memory,
            // so a deep hotspot is not scattered by parallelism balancing.
            // Keep the peak depth from the initial sweep. Here used_blocks only
            // prevents the same block from being assigned to multiple tasks.
            let mut task_bytes = 0usize;
            let mut task_indices = Vec::new();
            for local_idx in 0..block_count {
                if used_blocks[local_idx] {
                    continue;
                }
                if open_pos[local_idx] > hotspot_right || close_pos[local_idx] < hotspot_left {
                    continue;
                }
                let idx = indices[local_idx];
                let block_size = blocks[idx].meta.block_size as usize;
                let should_split_for_memory = !task_indices.is_empty()
                    && task_bytes.saturating_add(block_size) > self.memory_threshold;

                if should_split_for_memory {
                    if task_indices.len() >= 2 {
                        let local_indices = std::mem::take(&mut task_indices);
                        push_task(
                            &mut candidates,
                            &mut used_blocks,
                            local_indices,
                            task_bytes,
                            peak_depth,
                        );
                        if candidates.len() >= task_budget {
                            break;
                        }
                    } else {
                        task_indices.clear();
                    }
                    task_bytes = 0;
                }

                task_bytes = task_bytes.saturating_add(block_size);
                task_indices.push(local_idx);
            }

            if !task_indices.is_empty()
                && (task_bytes < self.memory_threshold || task_indices.len() < 2)
            {
                // Fill only the last hotspot tail from the deeper adjacent side.
                let mut left = hotspot_left;
                let mut right = hotspot_right;
                'fill_remaining: while task_bytes < self.memory_threshold || task_indices.len() < 2
                {
                    let left_depth = if left > 0 {
                        point_depths[left - 1] as f64
                    } else {
                        0.0
                    };
                    let right_depth = if right + 1 < num_points {
                        point_depths[right + 1] as f64
                    } else {
                        0.0
                    };
                    if left_depth.max(right_depth) <= self.depth_threshold {
                        break;
                    }

                    let (cur, use_ends) = if left_depth >= right_depth {
                        left -= 1;
                        (order[left] as usize, true)
                    } else {
                        right += 1;
                        (order[right] as usize, false)
                    };
                    let group_indices = if use_ends {
                        &values[cur].1
                    } else {
                        &values[cur].0
                    };
                    for &local_idx in group_indices {
                        if used_blocks[local_idx] {
                            continue;
                        }
                        let idx = indices[local_idx];
                        let block_size = blocks[idx].meta.block_size as usize;
                        if !task_indices.is_empty()
                            && task_bytes.saturating_add(block_size) > self.memory_threshold
                        {
                            break 'fill_remaining;
                        }
                        task_bytes = task_bytes.saturating_add(block_size);
                        task_indices.push(local_idx);
                    }
                }
            }

            if task_indices.len() >= 2 {
                push_task(
                    &mut candidates,
                    &mut used_blocks,
                    task_indices,
                    task_bytes,
                    peak_depth,
                );
            }
        }

        debug!(
            "recluster: probed task candidates group={} block_count={} avg_depth={} depth_threshold={} max_depth={} peak_count={} task_count={}",
            group,
            block_count,
            average_depth,
            self.depth_threshold,
            max_depth,
            peaks.len(),
            candidates.len(),
        );

        Ok(candidates)
    }

    fn calc_point_depth(open_interval_count: usize, start: &[usize], end: &[usize]) -> usize {
        // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
        if open_interval_count == 1
            && !start.is_empty()
            && !end.is_empty()
            && start.len() + end.len() <= 3
        {
            let set: HashSet<usize> = HashSet::from_iter(start.iter().chain(end.iter()).cloned());
            if set.len() == 2 {
                return 1;
            }
        }

        open_interval_count + start.len()
    }
}

fn vector_segment_spheres_overlap(
    left_blocks: &[Arc<BlockMeta>],
    right_blocks: &[Arc<BlockMeta>],
    vector_cluster_info: &VectorClusterInfo,
) -> Result<bool> {
    let mut left_stats = Vec::new();
    let mut right_stats = Vec::new();
    let mut left_missing_stats = false;
    let mut right_missing_stats = false;

    for block_meta in left_blocks {
        if let Some(vector_stats) =
            block_meta_vector_stats(block_meta.as_ref(), vector_cluster_info)
        {
            left_stats.push(vector_stats);
        } else {
            left_missing_stats = true;
        }
    }

    for block_meta in right_blocks {
        if let Some(vector_stats) =
            block_meta_vector_stats(block_meta.as_ref(), vector_cluster_info)
        {
            right_stats.push(vector_stats);
        } else {
            right_missing_stats = true;
        }
    }

    if left_missing_stats || right_missing_stats || left_stats.is_empty() || right_stats.is_empty()
    {
        return Ok(true);
    }

    for left_stat in &left_stats {
        for right_stat in &right_stats {
            if left_stat.spheres_overlap(right_stat, vector_cluster_info.distance_type)? {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn vector_cluster_info_from_exprs(
    table: &FuseTable,
    cluster_keys: &[Expr<usize>],
) -> Result<Option<VectorClusterInfo>> {
    let table_schema = table.schema();
    let mut vector_cluster_info = None;

    for (key_index, expr) in cluster_keys.iter().enumerate() {
        let DataType::Vector(vector_ty) = expr.data_type().remove_nullable() else {
            continue;
        };

        let Expr::ColumnRef(ColumnRef { id, .. }) = expr else {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key only supports direct column reference",
            ));
        };

        let field = table_schema.field(*id);
        let dimension: usize = vector_ty.dimension().try_into().map_err(|_| {
            ErrorCode::InvalidClusterKeys("Vector cluster key dimension is too large for kmeans")
        })?;
        if dimension == 0 {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key dimension must be greater than zero",
            ));
        }
        let vector_info = vector_cluster_info_from_column(
            &table.table_info.meta.indexes,
            key_index,
            field.column_id(),
            field.name(),
            dimension,
        )?;

        if vector_cluster_info.is_some() {
            return Err(ErrorCode::InvalidClusterKeys(
                "Only one vector column is supported in cluster by",
            ));
        }

        vector_cluster_info = Some(vector_info);
    }

    Ok(vector_cluster_info)
}

fn scalar_cluster_key_exprs(cluster_key_exprs: Vec<Expr<usize>>) -> Vec<Expr<usize>> {
    cluster_key_exprs
        .into_iter()
        .filter(|expr| !matches!(expr.data_type().remove_nullable(), DataType::Vector(_)))
        .collect()
}

fn block_meta_vector_stats<'a>(
    block_meta: &'a BlockMeta,
    vector_cluster_info: &VectorClusterInfo,
) -> Option<&'a VectorColumnStatistics> {
    block_meta.vector_stats.as_ref()?.get(&(
        vector_cluster_info.column_id,
        vector_cluster_info.distance_type,
    ))
}

fn scalar_le(left: &Scalar, right: &Scalar) -> bool {
    matches!(
        left.partial_cmp(right),
        None | Some(cmp::Ordering::Less | cmp::Ordering::Equal)
    )
}
