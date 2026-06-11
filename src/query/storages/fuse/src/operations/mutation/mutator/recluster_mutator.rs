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
use std::collections::BTreeSet;
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
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use indexmap::IndexSet;
use log::debug;
use log::warn;
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
use crate::statistics::get_min_max_stats;
use crate::statistics::reducers::merge_statistics_mut;

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
    Level(i32),
    Mixed(i32),
}

impl ReclusterGroup {
    fn output_level(self, task_indices: &[usize], blocks: &[&ReclusterBlock]) -> i32 {
        match self {
            ReclusterGroup::Level(level) => level,
            ReclusterGroup::Mixed(_) => {
                let mut level_counts = BTreeMap::new();
                for &idx in task_indices {
                    *level_counts.entry(blocks[idx].stats().level).or_insert(0) += 1;
                }

                level_counts
                    .into_iter()
                    .max_by_key(|(level, count)| (*count, cmp::Reverse(*level)))
                    .map(|(level, _)| level)
                    .unwrap_or(0)
            }
        }
    }
}

impl fmt::Display for ReclusterGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReclusterGroup::Level(level) => write!(f, "{}", level),
            ReclusterGroup::Mixed(band) => write!(f, "mixed-{}", band),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CandidateScore {
    pub selected_block_count: usize,
    pub max_depth: usize,
    pub average_depth: f64,
}

impl CandidateScore {
    pub fn cmp_desc(&self, other: &Self) -> cmp::Ordering {
        // Score order: max depth, average depth, then selected block count.
        self.max_depth
            .cmp(&other.max_depth)
            .then_with(|| {
                self.average_depth
                    .partial_cmp(&other.average_depth)
                    .unwrap_or(cmp::Ordering::Equal)
            })
            .then_with(|| self.selected_block_count.cmp(&other.selected_block_count))
    }
}

#[derive(Clone)]
pub(crate) struct ReclusterTaskCandidate {
    pub(crate) score: CandidateScore,
    selected_total_bytes: usize,
    min_blocks_per_task: usize,
    // Empty means a rebuild-only repack candidate.
    selected_blocks: Vec<(usize, Vec<usize>)>,
    output_level: i32,
}

impl fmt::Display for ReclusterTaskCandidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "output_level={} max_depth={} avg_depth={} selected_count={} bytes={}",
            self.output_level,
            self.score.max_depth,
            self.score.average_depth,
            self.score.selected_block_count,
            self.selected_total_bytes,
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
}

struct DepthSelection {
    selected_idx: IndexSet<usize>,
    average_depth: f64,
    max_depth: usize,
    max_point_overlap_count: usize,
    passed_depth_gate: bool,
}

/// Iterative segment tree answering range-max queries over a fixed sequence.
/// `build` is O(n), `range_max` is O(log n) over an inclusive `[l, r]` range.
struct RangeMaxTree {
    size: usize,
    tree: Vec<usize>,
}

impl RangeMaxTree {
    fn build(values: &[usize]) -> Self {
        let size = values.len();
        debug_assert!(size > 0, "RangeMaxTree requires a non-empty input");
        let mut tree = vec![0usize; size * 2];
        tree[size..(size * 2)].copy_from_slice(values);
        for i in (1..size).rev() {
            tree[i] = tree[2 * i].max(tree[2 * i + 1]);
        }
        Self { size, tree }
    }

    /// Max over the inclusive range `[l, r]`. Caller must ensure `l <= r < size`.
    fn range_max(&self, l: usize, r: usize) -> usize {
        debug_assert!(l <= r && r < self.size, "range [{l}, {r}] out of bounds");
        let mut lo = l + self.size;
        let mut hi = r + self.size + 1;
        let mut acc = 0usize;
        while lo < hi {
            if lo & 1 == 1 {
                acc = acc.max(self.tree[lo]);
                lo += 1;
            }
            if hi & 1 == 1 {
                hi -= 1;
                acc = acc.max(self.tree[hi]);
            }
            lo >>= 1;
            hi >>= 1;
        }
        acc
    }
}

/// Cluster statistics for a candidate block.
///
/// `Original` means the block already carries cluster statistics matching the
/// current cluster key, so selection can borrow them directly and write-back can
/// reuse the original `Arc<BlockMeta>` without cloning the (potentially large)
/// `ClusterStatistics`. `Normalized` holds statistics recomputed for a block
/// whose cached cluster key differs from the current one.
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

#[derive(Clone)]
pub struct ReclusterMutator {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) operator: Operator,
    pub(crate) depth_threshold: f64,
    pub(crate) block_thresholds: BlockThresholds,
    pub(crate) cluster_key_id: u32,
    pub(crate) schema: TableSchemaRef,
    pub(crate) max_tasks: usize,
    pub(crate) memory_threshold: usize,
    pub(crate) cluster_key_exprs: Vec<Expr<usize>>,
    pub(crate) cluster_key_types: Vec<DataType>,
}

impl ReclusterMutator {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
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
        let cluster_key_exprs =
            parse_cluster_keys(ctx.clone(), Arc::new(table.clone()), cluster_keys)?;
        if cluster_key_exprs.is_empty() {
            return Err(ErrorCode::Internal(
                "recluster requires non-empty cluster key expressions",
            ));
        }
        let cluster_key_types = cluster_key_exprs
            .iter()
            .map(|v| v.data_type().clone())
            .collect::<Vec<_>>();

        Ok(Self {
            ctx,
            operator: table.get_operator(),
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            memory_threshold,
            cluster_key_exprs,
            cluster_key_types,
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
    ) -> Self {
        assert!(
            !cluster_key_exprs.is_empty(),
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
        Self {
            ctx,
            operator,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            memory_threshold,
            cluster_key_exprs,
            cluster_key_types,
        }
    }

    #[async_backtrace::framed]
    pub async fn probe_candidate_window(
        &self,
        compact_segments: Vec<SelectedReclusterSegment>,
        mode: ReclusterMode,
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
        let mut blocks_map: BTreeMap<ReclusterGroup, Vec<usize>> = BTreeMap::new();
        for segment_blocks in &blocks_by_segment {
            for block in segment_blocks {
                let idx = blocks.len();
                blocks.push(block);
                if block.stats().level < 0 {
                    continue;
                }
                if block.stats().level >= MAX_RECLUSTER_LEVEL {
                    debug!(
                        "recluster: skip block segment_idx={} block_idx={} level={} skip_reason=max_recluster_level",
                        block.index.segment_idx,
                        block.index.block_idx,
                        block.stats().level,
                    );
                    continue;
                }

                // FINAL isolates level 0 and groups mature levels into fixed two-level bands.
                let group = match mode {
                    ReclusterMode::Normal => ReclusterGroup::Level(block.stats().level),
                    ReclusterMode::Final if block.stats().level == 0 => ReclusterGroup::Level(0),
                    ReclusterMode::Final => ReclusterGroup::Mixed((block.stats().level - 1) / 2),
                };
                blocks_map.entry(group).or_default().push(idx);
            }
        }

        let mut candidate_window = ReclusterCandidateWindow {
            segments: window_segments,
            tasks: Vec::new(),
        };
        let mut selected_window_positions = vec![false; window_segment_infos.len()];

        for (group, indices) in blocks_map {
            if candidate_window.tasks.len() >= task_budget {
                break;
            }
            let remaining_task_budget = task_budget - candidate_window.tasks.len();
            let candidates = self.build_recluster_task_candidates_for_indices(
                group,
                indices,
                &blocks,
                remaining_task_budget,
            )?;
            for candidate in candidates.into_iter().take(remaining_task_budget) {
                for (window_pos, _) in &candidate.selected_blocks {
                    selected_window_positions[*window_pos] = true;
                }
                candidate_window.tasks.push(candidate);
            }
        }

        if candidate_window.tasks.is_empty() {
            let selected_segment_count = window_segment_infos.len();
            let target_segment_count =
                total_block_count.div_ceil(self.block_thresholds.block_per_segment);
            if total_block_count > 0
                && selected_segment_count > 1
                && target_segment_count < selected_segment_count
            {
                // Repack-only candidate removes segments without rewrite tasks.
                // Score it as a dense block group so sorting keeps it ahead of
                // normal rewrite candidates from the same scan range.
                selected_window_positions.fill(true);
                candidate_window.tasks.push(ReclusterTaskCandidate {
                    score: CandidateScore {
                        selected_block_count: total_block_count,
                        max_depth: total_block_count,
                        average_depth: total_block_count as f64,
                    },
                    selected_total_bytes: 0,
                    min_blocks_per_task: 2,
                    selected_blocks: Vec::new(),
                    output_level: 0,
                });
            } else {
                return Ok(candidate_window);
            }
        }

        drop(blocks);

        let mut selected_segment_blocks = vec![Vec::new(); window_segment_infos.len()];
        for (window_pos, selected) in selected_window_positions.iter().copied().enumerate() {
            if !selected {
                continue;
            }
            for block in blocks_by_segment[window_pos].drain(..) {
                let ReclusterBlock { index, meta, stats } = block;
                let block_meta = match stats {
                    ReclusterBlockStats::Original => meta,
                    ReclusterBlockStats::Normalized(stats) => {
                        let mut block_meta = Arc::unwrap_or_clone(meta);
                        block_meta.cluster_stats = Some(stats);
                        Arc::new(block_meta)
                    }
                };
                selected_segment_blocks[window_pos].push((index.block_idx, block_meta));
            }
        }

        for (window_pos, selected) in selected_window_positions.into_iter().enumerate() {
            if !selected {
                continue;
            }
            let info = &window_segment_infos[window_pos];
            let mut block_metas = std::mem::take(&mut selected_segment_blocks[window_pos]);
            block_metas.sort_by_key(|(block_idx, _)| *block_idx);
            let blocks = block_metas
                .into_iter()
                .map(|(_, block_meta)| block_meta)
                .collect::<Vec<_>>();
            candidate_window.segments[window_pos].1 = Some(Arc::new(SegmentInfo {
                format_version: info.format_version,
                blocks,
                summary: info.summary.clone(),
            }));
        }

        Ok(candidate_window)
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

                let mut block_metas = Vec::with_capacity(candidate.score.selected_block_count);
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

        let mut hll_segment_indexes = IndexSet::new();
        let mut remained_blocks = Vec::new();
        for (&segment_idx, segment_info) in &removed_segment_infos {
            for (block_idx, block_meta) in segment_info.blocks.iter().enumerate() {
                if selected_block_keys.contains(&(segment_idx, block_idx)) {
                    continue;
                }
                // HLL is needed only when a removed segment leaves remained blocks.
                hll_segment_indexes.insert(segment_idx);
                remained_blocks.push((
                    BlockIndex {
                        segment_idx,
                        block_idx,
                    },
                    block_meta.clone(),
                ));
            }
        }

        let hlls = self
            .gather_hlls(
                removed_segment_infos
                    .iter()
                    .filter_map(|(segment_idx, segment_info)| {
                        if !hll_segment_indexes.contains(segment_idx) {
                            return None;
                        }
                        segment_info
                            .summary
                            .additional_stats_meta
                            .as_ref()
                            .map(|v| (*segment_idx, v.location.clone()))
                    })
                    .collect::<Vec<_>>(),
            )
            .await?;
        let remained_blocks = remained_blocks
            .into_iter()
            .map(|(block_index, block_meta)| {
                let hll = hlls.get(&block_index).and_then(|hll| hll.clone());
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
        let probe_group_start = Instant::now();
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
        let mut points_map: HashMap<&[Scalar], (Vec<usize>, Vec<usize>)> = HashMap::new();

        for (local_idx, &i) in indices.iter().enumerate() {
            // Use a group-local block index (0..block_count) as the point key so
            // the dense lookup vectors in `fetch_max_depth` are sized by the group
            // block count, not the window-global block index range. `indices` maps
            // each local index back to its `blocks` index.
            let block = &blocks[i];
            let stats = block.stats();
            let point = points_map.entry(stats.min().as_slice()).or_default();
            point.0.push(local_idx);
            let point = points_map.entry(stats.max().as_slice()).or_default();
            point.1.push(local_idx);
            total_rows += block.meta.row_count;
            total_bytes += block.meta.block_size;
        }

        let max_blocks_num_per_node =
            self.max_blocks_num_per_node(total_bytes as usize, block_count);
        let min_blocks_per_task = (max_blocks_num_per_node * 3 / 4).max(2);

        if self
            .block_thresholds
            .check_for_compact(total_rows as usize, total_bytes as usize)
            && total_bytes as usize <= self.memory_threshold
        {
            // Small compactable groups are treated as one dense overlap.
            let score = CandidateScore {
                selected_block_count: block_count,
                max_depth: block_count,
                average_depth: block_count as f64,
            };
            return Ok(vec![ReclusterTaskCandidate {
                score,
                selected_total_bytes: total_bytes as usize,
                min_blocks_per_task,
                selected_blocks: Self::selected_blocks_by_segment(&indices, blocks),
                output_level: group.output_level(&indices, blocks),
            }]);
        }

        let max_blocks_num = max_blocks_num_per_node * task_budget;
        let DepthSelection {
            selected_idx: selected_local_idx,
            average_depth,
            max_depth,
            max_point_overlap_count,
            passed_depth_gate,
        } = Self::fetch_max_depth(
            points_map,
            &self.cluster_key_types,
            self.depth_threshold,
            max_blocks_num,
            block_count,
        )?;
        // Translate group-local indices back to `blocks` indices (preserving the
        // selection order) so the budget/limit and task-splitting steps below can
        // index `blocks` directly.
        let mut selected_idx = selected_local_idx
            .into_iter()
            .map(|local_idx| indices[local_idx])
            .collect::<IndexSet<usize>>();
        let mut selected_total_bytes = 0usize;
        let mut keep_blocks = 0;
        let max_total_bytes = self.memory_threshold.saturating_mul(task_budget);
        for idx in selected_idx.iter().copied() {
            let block_size = blocks[idx].meta.block_size as usize;
            if keep_blocks >= 2 && selected_total_bytes.saturating_add(block_size) > max_total_bytes
            {
                break;
            }
            selected_total_bytes = selected_total_bytes.saturating_add(block_size);
            keep_blocks += 1;
        }
        selected_idx.truncate(keep_blocks);
        let selected_block_count = selected_idx.len();
        if selected_block_count < 2 || !passed_depth_gate {
            debug!(
                "recluster: candidate selection detail group={} block_count={} average_depth={} max_depth={} max_point_overlap_count={} selected_count={} elapsed={:?} skip_reason=below_hotspot_depth_gate",
                group,
                block_count,
                average_depth,
                max_depth,
                max_point_overlap_count,
                selected_block_count,
                probe_group_start.elapsed(),
            );
            return Ok(Vec::new());
        }

        let target_tasks_by_blocks = selected_block_count / min_blocks_per_task;
        let target_tasks_by_memory = selected_total_bytes.div_ceil(self.memory_threshold);
        let max_tasks_by_blocks = selected_block_count / 2;
        let target_tasks = target_tasks_by_blocks
            .max(target_tasks_by_memory)
            .max(1)
            .min(task_budget)
            .min(max_tasks_by_blocks);
        let target_task_bytes = selected_total_bytes.div_ceil(target_tasks);
        let target_task_blocks = selected_block_count.div_ceil(target_tasks);

        let mut candidates = Vec::new();
        let mut task_bytes = 0usize;
        let mut task_indices = Vec::new();
        for (processed_blocks, idx) in selected_idx.into_iter().enumerate() {
            let block = &blocks[idx];
            let block_size = block.meta.block_size as usize;
            let remaining_tasks = target_tasks.saturating_sub(candidates.len() + 1);
            let remaining_blocks = selected_block_count.saturating_sub(processed_blocks);
            let has_enough_remaining_blocks =
                remaining_blocks >= remaining_tasks * min_blocks_per_task;
            let should_split_for_memory =
                task_bytes.saturating_add(block_size) > self.memory_threshold;
            let should_split_for_parallelism = candidates.len() + 1 < target_tasks
                && task_indices.len() >= min_blocks_per_task
                && has_enough_remaining_blocks
                && (task_bytes >= target_task_bytes || task_indices.len() >= target_task_blocks);

            if should_split_for_memory || should_split_for_parallelism {
                if task_indices.len() >= 2 {
                    let score = CandidateScore {
                        selected_block_count: task_indices.len(),
                        max_depth,
                        average_depth,
                    };
                    let output_level = group.output_level(&task_indices, blocks);
                    candidates.push(ReclusterTaskCandidate {
                        score,
                        selected_total_bytes: task_bytes,
                        min_blocks_per_task,
                        selected_blocks: Self::selected_blocks_by_segment(&task_indices, blocks),
                        output_level,
                    });
                }
                task_bytes = 0;
                task_indices.clear();
                if candidates.len() >= target_tasks {
                    break;
                }
            }

            task_bytes += block_size;
            task_indices.push(idx);
        }

        if task_indices.len() > 1 {
            let score = CandidateScore {
                selected_block_count: task_indices.len(),
                max_depth,
                average_depth,
            };
            let output_level = group.output_level(&task_indices, blocks);
            candidates.push(ReclusterTaskCandidate {
                score,
                selected_total_bytes: task_bytes,
                min_blocks_per_task,
                selected_blocks: Self::selected_blocks_by_segment(&task_indices, blocks),
                output_level,
            });
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "recluster: probed task candidates group={} block_count={} avg_depth={} depth_threshold={} max_depth={} max_point_overlap_count={} selected_count={} task_count={} elapsed={:?}",
            group,
            block_count,
            average_depth,
            self.depth_threshold,
            max_depth,
            max_point_overlap_count,
            selected_block_count,
            candidates.len(),
            probe_group_start.elapsed(),
        );

        Ok(candidates)
    }

    fn max_blocks_num_per_node(&self, total_bytes: usize, block_count: usize) -> usize {
        let avg_block_bytes = (total_bytes / block_count).max(1);
        // Clamp the observed average to normal block thresholds so tiny fragments
        // do not inflate the candidate count and unusually large blocks do not
        // make the distributed selection overly conservative.
        let target_block_bytes = avg_block_bytes
            .max(self.block_thresholds.min_bytes_per_block)
            .min(self.block_thresholds.max_bytes_per_block);
        (self.memory_threshold / target_block_bytes).max(2)
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
        candidate.score.max_depth as f64 >= early_accept_depth
            && candidate.score.selected_block_count >= candidate.min_blocks_per_task
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
            &self.cluster_key_exprs,
            col_stats,
            cluster_stats,
            Some(self.cluster_key_id),
            self.schema.as_ref(),
        );

        ClusterStatistics::new(self.cluster_key_id, min_stats, max_stats, 0, None)
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
        hlls: Vec<(usize, Location)>,
    ) -> Result<HashMap<BlockIndex, Option<RawBlockHLL>>> {
        if hlls.is_empty() {
            return Ok(HashMap::new());
        }

        let tasks = hlls.into_iter().map(|(segment_idx, (loc, ver))| {
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
                Ok(stats
                    .block_hlls
                    .iter()
                    .enumerate()
                    .map(|(block_idx, hll)| {
                        let block_index = BlockIndex {
                            segment_idx,
                            block_idx,
                        };
                        (block_index, Some(hll.clone()))
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

    fn fetch_max_depth(
        points_map: HashMap<&[Scalar], (Vec<usize>, Vec<usize>)>,
        cluster_key_types: &[DataType],
        depth_threshold: f64,
        max_len: usize,
        block_count: usize,
    ) -> Result<DepthSelection> {
        debug_assert!(!points_map.is_empty());
        let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
        let order = compare_scalars(&keys, cluster_key_types)?;

        // PASS 1: sweep the sorted points once. `live_count` is the deduplicated
        // open-interval count before inserting starts; `overlap_counts[i]` is the
        // snapshot size after inserting starts and before removing ends.
        let num_points = order.len();
        let mut point_depths = vec![0usize; num_points];
        let mut overlap_counts = vec![0usize; num_points];
        let unset_pos = usize::MAX;
        let mut open_pos = vec![unset_pos; block_count];
        let mut close_pos = vec![unset_pos; block_count];
        let mut live = vec![false; block_count];
        let mut live_count = 0usize;
        let mut max_depth = 0;
        let mut max_point = 0;
        for i in 0..num_points {
            let value_idx = order[i] as usize;
            let (starts, ends) = &values[value_idx];
            let point_depth = Self::calc_point_depth(live_count, starts, ends);
            point_depths[i] = point_depth;
            if point_depth > max_depth {
                max_depth = point_depth;
                max_point = i;
            }
            for &s in starts {
                if !live[s] {
                    live[s] = true;
                    live_count += 1;
                }
                open_pos[s] = i;
            }
            overlap_counts[i] = live_count;
            for &e in ends {
                if live[e] {
                    live[e] = false;
                    live_count -= 1;
                    close_pos[e] = i;
                }
            }
        }
        // `live` now marks intervals whose end point never closed them.
        let mut unfinished_sorted = Vec::new();

        // PASS 2: interval depth is the max folded point depth over the interval's
        // lifetime [open_pos, close_pos], answered by a range-max segment tree
        // instead of re-maxing every open interval at every point.
        let mut interval_depths = vec![0usize; block_count];
        let mut has_interval_depth = vec![false; block_count];
        let mut sum_depth = 0usize;
        let mut closed = 0usize;
        let seg = RangeMaxTree::build(&point_depths);
        for idx in 0..block_count {
            if open_pos[idx] == unset_pos {
                continue;
            }
            if live[idx] {
                unfinished_sorted.push(idx);
                continue;
            }
            let open = open_pos[idx];
            let close = close_pos[idx];
            let depth = seg.range_max(open, close);
            interval_depths[idx] = depth;
            has_interval_depth[idx] = true;
            sum_depth += depth;
            closed += 1;
        }
        let average_depth = if closed == 0 {
            f64::NAN
        } else {
            (10000.0 * sum_depth as f64 / closed as f64).round() / 10000.0
        };
        let max_point_overlap_count = overlap_counts.get(max_point).copied().unwrap_or(0);

        let mut selected_idx = IndexSet::with_capacity(max_len);
        if !unfinished_sorted.is_empty() {
            warn!(
                "recluster: unfinished intervals remain after calculating block overlaps count={} max_selected={}",
                unfinished_sorted.len(),
                max_len,
            );
            // Keep unfinished intervals selected first; they indicate malformed
            // or stale range boundaries but are still valid recluster candidates.
            // Emit them in ascending index order to match sweep snapshot order.
            unfinished_sorted.sort_unstable();
            for idx in unfinished_sorted {
                selected_idx.insert(idx);
            }
        }

        let passed_depth_gate = Self::passes_depth_gate(depth_threshold, average_depth, max_depth);
        if !passed_depth_gate {
            selected_idx.truncate(max_len);
            return Ok(DepthSelection {
                selected_idx,
                average_depth,
                max_depth,
                max_point_overlap_count,
                passed_depth_gate,
            });
        }

        // PASS 3: `heavy_count[i]` counts open intervals at point `i` whose final
        // depth is not exactly 1 (unfinished intervals count as heavy). It lets
        // the expansion replace the old per-point `.all(depth == 1)` scan with an
        // O(1) check. Built with a signed difference array.
        let mut diff = vec![0isize; num_points + 1];
        for idx in 0..block_count {
            if open_pos[idx] == unset_pos {
                continue;
            }
            let heavy = !has_interval_depth[idx] || interval_depths[idx] != 1;
            if heavy {
                let open = open_pos[idx];
                let close = if close_pos[idx] == unset_pos {
                    num_points - 1
                } else {
                    close_pos[idx]
                };
                diff[open] += 1;
                diff[close + 1] -= 1;
            }
        }
        let mut heavy_count = vec![0isize; num_points];
        let mut running = 0isize;
        for i in 0..num_points {
            running += diff[i];
            debug_assert!(running >= 0, "heavy_count underflow at point {i}");
            heavy_count[i] = running;
        }

        // Expansion from `max_point`, maintaining the current snapshot incrementally
        // rather than from a stored per-point copy. A snapshot at point `i` is the
        // open-interval set after inserting starts(i) and before removing ends(i),
        // ordered ascending (BTreeSet) to match sweep snapshot order.
        let mut left_set = BTreeSet::new();
        for idx in 0..block_count {
            let open = open_pos[idx];
            if open == unset_pos {
                continue;
            }
            let close = close_pos[idx];
            if open <= max_point && (close == unset_pos || max_point <= close) {
                left_set.insert(idx);
            }
        }
        let mut right_set = left_set.clone();
        for &idx in &left_set {
            selected_idx.insert(idx);
        }

        let mut left = max_point;
        let mut right = max_point;
        while selected_idx.len() < max_len {
            let left_depth = if left > 0 {
                if heavy_count[left - 1] == 0 {
                    // All overlapping intervals have depth 1: this side is done.
                    left = 0;
                    0.0
                } else {
                    overlap_counts[left - 1] as f64
                }
            } else {
                0.0
            };
            let right_depth = if right < num_points - 1 {
                if heavy_count[right + 1] == 0 {
                    right = num_points - 1;
                    0.0
                } else {
                    overlap_counts[right + 1] as f64
                }
            } else {
                0.0
            };

            if left_depth.max(right_depth) <= depth_threshold {
                break;
            }

            if left_depth >= right_depth {
                left -= 1;
                let next = order[left + 1] as usize;
                for &s in &values[next].0 {
                    left_set.remove(&s);
                }
                let cur = order[left] as usize;
                for &e in &values[cur].1 {
                    left_set.insert(e);
                }
                // Left expansion prepends the (ascending) snapshot ahead of the
                // already-selected indices, preserving first-seen order.
                let mut merged_idx = IndexSet::from_iter(left_set.iter().copied());
                merged_idx.extend(selected_idx);
                selected_idx = merged_idx;
            } else {
                right += 1;
                let prev = order[right - 1] as usize;
                for &e in &values[prev].1 {
                    right_set.remove(&e);
                }
                let cur = order[right] as usize;
                for &s in &values[cur].0 {
                    right_set.insert(s);
                }
                selected_idx.extend(right_set.iter().copied());
            }
        }

        selected_idx.truncate(max_len);
        Ok(DepthSelection {
            selected_idx,
            average_depth,
            max_depth,
            max_point_overlap_count,
            passed_depth_gate,
        })
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
