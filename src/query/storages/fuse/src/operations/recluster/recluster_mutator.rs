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
use databend_common_catalog::plan::ReclusterDepthKind;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnRef;
use databend_common_expression::Expr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_sql::ClusterKeyKind;
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
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use log::debug;
use opendal::Operator;
use tokio::sync::Semaphore;

use super::ReclusterMode;
use super::hilbert_recluster::fetch_hilbert_task_candidates;
use super::hilbert_recluster::select_hilbert_segments;
use super::vector_recluster::fetch_vector_task_candidates;
use super::vector_recluster::select_vector_segments;
use crate::DEFAULT_RECLUSTER_DEPTH;
use crate::FUSE_OPT_KEY_RECLUSTER_DEPTH;
use crate::FuseTable;
use crate::MAX_RECLUSTER_DEPTH;
use crate::MIN_RECLUSTER_DEPTH;
use crate::SegmentLocation;
use crate::io::MetaReaders;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::statistics::PreparedClusterKeyExpr;
use crate::statistics::VectorClusterInfo;
use crate::statistics::cluster_stats_from_col_stats;
use crate::statistics::cluster_stats_hilbert_minmax;
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
pub(super) enum ReclusterGroup {
    /// A single level forms its own group.
    Level(i32),
    /// Aggressive mode: a fixed maturity bin identified by its lower bound `lo`.
    Range(i32),
}

#[derive(Clone)]
enum ReclusterStrategy {
    Linear,
    Vector(VectorClusterInfo),
    Hilbert,
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

/// Ranking score for a recluster task candidate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateScore {
    pub selected_total_bytes: usize,
    pub max_depth: usize,
    pub average_depth: f64,
}

impl CandidateScore {
    /// Compare scores in descending priority order.
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

/// In-memory rewrite candidate produced from one probed window.
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

    /// Whether this candidate only repacks unchanged blocks into fewer segments.
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

/// Candidate tasks plus cached segment metadata for one scanned window.
#[derive(Clone, Default)]
pub struct ReclusterCandidateWindow {
    // Window locations plus cached SegmentInfo for positions touched by candidates.
    pub(crate) segments: Vec<(Location, Option<Arc<SegmentInfo>>)>,
    pub(crate) tasks: Vec<ReclusterTaskCandidate>,
}

impl ReclusterCandidateWindow {
    /// Number of task candidates in this window.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
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
    // Next fixed scan-range start.
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

/// Cluster statistics for a candidate block.
///
/// `Original` means the block already carries cluster statistics matching the
/// current cluster key, so selection can borrow them directly and write-back can
/// reuse the original `Arc<BlockMeta>` without cloning the (potentially large)
/// `ClusterStatistics`. `Normalized` holds statistics recomputed for a block
/// whose cached cluster key differs from the current one. Normalized stats are
/// only a selection-time view; they must not be persisted into unchanged block
/// metas because they do not prove that the physical block is ordered.
pub(super) enum ReclusterBlockStats {
    Original,
    Normalized(ClusterStatistics),
}

/// Block plus the cluster statistics view used for candidate selection.
pub(super) struct ReclusterBlock {
    pub(super) index: BlockIndex,
    pub(super) meta: Arc<BlockMeta>,
    pub(super) stats: ReclusterBlockStats,
}

impl ReclusterBlock {
    /// Cluster statistics used during candidate selection (read-only).
    pub(super) fn stats(&self) -> &ClusterStatistics {
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

/// Segment selected for probing by recluster.
#[derive(Clone)]
pub struct SelectedReclusterSegment {
    pub loc: SegmentLocation,
    pub info: Arc<CompactSegmentInfo>,
}

/// Builds recluster candidate windows and materializes selected tasks.
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
    hilbert_len: usize,
    pub(crate) cluster_key_types: Vec<DataType>,
    strategy: ReclusterStrategy,
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
        let kind = parsed_cluster_keys.kind;
        let hilbert_len = parsed_cluster_keys.hilbert_len();
        let mut cluster_key_exprs = parsed_cluster_keys.keys;
        let strategy = match kind {
            ClusterKeyKind::Linear => ReclusterStrategy::Linear,
            ClusterKeyKind::Vector { vector_index } => {
                let expr = &cluster_key_exprs[vector_index];
                let DataType::Vector(vector_ty) = expr.data_type().remove_nullable() else {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key must be vector type",
                    ));
                };
                let Expr::ColumnRef(ColumnRef { id, .. }) = expr else {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key only supports direct column reference",
                    ));
                };
                let table_schema = table.schema();
                let field = table_schema.field(*id);
                let dimension: usize = vector_ty.dimension().try_into().map_err(|_| {
                    ErrorCode::InvalidClusterKeys(
                        "Vector cluster key dimension is too large for kmeans",
                    )
                })?;
                if dimension == 0 {
                    return Err(ErrorCode::InvalidClusterKeys(
                        "Vector cluster key dimension must be greater than zero",
                    ));
                }
                let vector_cluster_info = vector_cluster_info_from_column(
                    &table.table_info.meta.indexes,
                    vector_index,
                    field.column_id(),
                    field.name(),
                    dimension,
                )?;
                cluster_key_exprs.remove(vector_index);
                ReclusterStrategy::Vector(vector_cluster_info)
            }
            ClusterKeyKind::Hilbert { .. } => {
                debug_assert!(hilbert_len > 0);
                ReclusterStrategy::Hilbert
            }
        };
        if cluster_key_exprs.is_empty() && matches!(&strategy, ReclusterStrategy::Linear) {
            return Err(ErrorCode::Internal(
                "recluster requires non-empty cluster key expressions",
            ));
        }
        let scalar_len = cluster_key_exprs.len().saturating_sub(hilbert_len);
        let cluster_key_types = cluster_key_exprs
            .iter()
            .take(scalar_len)
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
            hilbert_len,
            cluster_key_types,
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
        max_tasks: usize,
        mode: ReclusterMode,
        vector_cluster_info: Option<VectorClusterInfo>,
    ) -> Self {
        let cluster_key_exprs = cluster_key_exprs
            .into_iter()
            .filter(|expr| !matches!(expr.data_type().remove_nullable(), DataType::Vector(_)))
            .collect::<Vec<_>>();
        let hilbert_len = 0;
        let strategy = match vector_cluster_info {
            Some(vector_cluster_info) => ReclusterStrategy::Vector(vector_cluster_info),
            None => ReclusterStrategy::Linear,
        };
        assert!(
            !cluster_key_exprs.is_empty() || !matches!(&strategy, ReclusterStrategy::Linear),
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
            hilbert_len,
            cluster_key_types,
            strategy,
        }
    }

    /// Decode one selected segment window and build candidate tasks from it.
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
        let mut deferred_candidates = Vec::new();
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

            for candidate in candidates {
                let defer = candidate.score.selected_total_bytes < large_task_bytes_threshold
                    && (candidate.score.max_depth as f64) < 4.0 * self.depth_threshold;
                if defer {
                    debug!(
                        "recluster: defer candidate group={} selected_bytes={} max_depth={} depth_threshold={} skip_reason=deferred_small_shallow_task",
                        group,
                        candidate.score.selected_total_bytes,
                        candidate.score.max_depth,
                        self.depth_threshold,
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

    fn depth_kind(&self) -> ReclusterDepthKind {
        let require_scalar_overlap = !self.cluster_key_types.is_empty();
        match &self.strategy {
            ReclusterStrategy::Linear => ReclusterDepthKind::Linear {
                cluster_key_types: self.cluster_key_types.clone(),
            },
            ReclusterStrategy::Hilbert => ReclusterDepthKind::Hilbert {
                require_scalar_overlap,
            },
            ReclusterStrategy::Vector(info) => ReclusterDepthKind::Vector {
                column_id: info.column_id,
                distance_type: info.distance_type,
                require_scalar_overlap,
            },
        }
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
        let depth_kind = self.depth_kind();
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
                    depth_kind: depth_kind.clone(),
                    max_depth: candidate.score.max_depth,
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

        let candidates = match &self.strategy {
            ReclusterStrategy::Linear => {
                self.fetch_linear_task_candidates(group, &indices, blocks, task_budget)?
            }
            ReclusterStrategy::Vector(strategy) => {
                fetch_vector_task_candidates(self, strategy, group, &indices, blocks, task_budget)?
            }
            ReclusterStrategy::Hilbert => {
                fetch_hilbert_task_candidates(self, group, &indices, blocks, task_budget)?
            }
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

    /// Build a task candidate from window-global block indices.
    pub(super) fn task_candidate(
        group: ReclusterGroup,
        score: CandidateScore,
        task_indices: &[usize],
        blocks: &[&ReclusterBlock],
    ) -> ReclusterTaskCandidate {
        let mut selected_block_positions: HashMap<usize, usize> =
            HashMap::with_capacity(task_indices.len());
        let mut selected_blocks = Vec::<(usize, Vec<usize>)>::with_capacity(task_indices.len());
        for &idx in task_indices {
            let block = &blocks[idx];
            if let Some(&position) = selected_block_positions.get(&block.index.segment_idx) {
                selected_blocks[position].1.push(block.index.block_idx);
            } else {
                selected_block_positions.insert(block.index.segment_idx, selected_blocks.len());
                selected_blocks.push((block.index.segment_idx, vec![block.index.block_idx]));
            }
        }

        let output_level = group.output_level(task_indices, blocks);
        let all_ordered = task_indices
            .iter()
            .all(|idx| matches!(&blocks[*idx].stats, ReclusterBlockStats::Original));
        ReclusterTaskCandidate {
            score,
            selected_blocks,
            output_level,
            all_ordered,
        }
    }

    /// Decide whether a group has enough overlap depth to be worth rewriting.
    pub(super) fn passes_depth_gate(
        depth_threshold: f64,
        average_depth: f64,
        max_depth: usize,
    ) -> bool {
        let mature_gate = if depth_threshold <= MIN_RECLUSTER_DEPTH as f64 {
            depth_threshold
        } else {
            (2.0 * depth_threshold).min(MAX_RECLUSTER_DEPTH as f64)
        };
        average_depth > depth_threshold || max_depth as f64 > mature_gate
    }

    /// Fast-path acceptance for very deep, sufficiently large candidates.
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
        match &self.strategy {
            ReclusterStrategy::Linear => self.select_scalar_segments(compact_segments, window_len),
            ReclusterStrategy::Vector(strategy) => {
                select_vector_segments(self, strategy, compact_segments, window_len)
            }
            ReclusterStrategy::Hilbert => {
                select_hilbert_segments(self, compact_segments, window_len)
            }
        }
    }

    /// Normalize or reuse cluster stats for recluster candidate selection.
    pub(super) fn build_cluster_stats_for_recluster(
        &self,
        cluster_stats: Option<&ClusterStatistics>,
        col_stats: &StatisticsOfColumns,
    ) -> ClusterStatistics {
        if let Some(stats) = cluster_stats {
            if self.can_reuse_cluster_stats(stats) {
                return stats.clone();
            }
        }

        let level = if self.has_hilbert_marker() {
            cluster_stats
                .filter(|stats| stats.cluster_key_id == self.cluster_key_id)
                .map_or(0, |stats| stats.level.max(0))
        } else {
            0
        };
        cluster_stats_from_col_stats(
            &self.prepared_cluster_key_exprs,
            col_stats,
            self.cluster_key_id,
            level,
            self.hilbert_len,
        )
    }

    fn can_reuse_cluster_stats(&self, stats: &ClusterStatistics) -> bool {
        if stats.cluster_key_id != self.cluster_key_id {
            return false;
        }
        if !self.has_hilbert_marker() {
            return true;
        }

        cluster_stats_hilbert_minmax(stats).is_some_and(|(min, max)| {
            min.len() == self.hilbert_len && max.len() == self.hilbert_len
        })
    }

    pub(super) fn has_hilbert_marker(&self) -> bool {
        matches!(&self.strategy, ReclusterStrategy::Hilbert)
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
                let stats = if block_meta
                    .cluster_stats
                    .as_ref()
                    .is_some_and(|stats| self.can_reuse_cluster_stats(stats))
                {
                    ReclusterBlockStats::Original
                } else {
                    ReclusterBlockStats::Normalized(self.build_cluster_stats_for_recluster(
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
