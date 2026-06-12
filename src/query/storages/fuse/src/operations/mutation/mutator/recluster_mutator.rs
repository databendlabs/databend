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
use databend_storages_common_cache::LoadParams;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use fastrace::Span;
use fastrace::func_path;
use fastrace::future::FutureExt;
use indexmap::IndexSet;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;

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

struct LevelReclusterTasks {
    selected_block_count: usize,
    min_blocks_per_task: usize,
    tasks: Vec<(ReclusterTask, Vec<usize>)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ReclusterGroup {
    Level(i32),
    Mixed(i32),
}

impl ReclusterGroup {
    fn output_level(self, task_indices: &[usize], blocks: &[ReclusterBlock]) -> i32 {
        match self {
            ReclusterGroup::Level(level) => level,
            ReclusterGroup::Mixed(_) => {
                let mut level_counts = BTreeMap::new();
                for &idx in task_indices {
                    *level_counts.entry(blocks[idx].stats.level).or_insert(0) += 1;
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

struct DepthSelection {
    selected_idx: IndexSet<usize>,
    average_depth: f64,
    max_depth: usize,
    max_point_overlap_count: usize,
}

struct ReclusterBlock {
    index: BlockIndex,
    meta: Arc<BlockMeta>,
    stats: ClusterStatistics,
}

#[derive(Clone)]
pub struct SelectedReclusterSegment {
    pub loc: SegmentLocation,
    pub info: Arc<CompactSegmentInfo>,
    pub stats: ClusterStatistics,
}

impl SelectedReclusterSegment {
    pub(crate) fn create(
        mutator: &ReclusterMutator,
        loc: SegmentLocation,
        info: Arc<CompactSegmentInfo>,
    ) -> Self {
        let stats = mutator.build_cluster_stats_for_recluster(
            info.summary.cluster_stats.as_ref(),
            &info.summary.col_stats,
        );
        Self { loc, info, stats }
    }
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

        let depth_threshold = if snapshot.summary.block_count <= SMALL_TABLE_RECLUSTER_BLOCK_COUNT {
            MIN_RECLUSTER_DEPTH as f64
        } else {
            table.get_option(FUSE_OPT_KEY_RECLUSTER_DEPTH, DEFAULT_RECLUSTER_DEPTH) as f64
        };

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
    pub async fn target_select(
        &self,
        compact_segments: Vec<SelectedReclusterSegment>,
        mode: ReclusterMode,
        task_budget: usize,
    ) -> Result<(u64, ReclusterParts)> {
        // Prepare for reclustering by collecting segment indices, statistics, and blocks
        let mut selected_segs_idx = Vec::with_capacity(compact_segments.len());
        let mut selected_statistics = Vec::with_capacity(compact_segments.len());
        let mut selected_seg_stats = Vec::with_capacity(compact_segments.len());
        let mut selected_segments = Vec::with_capacity(compact_segments.len());
        for segment in compact_segments {
            selected_statistics.push(segment.info.summary.clone());
            selected_segs_idx.push(segment.loc.segment_idx);
            selected_seg_stats.push((
                segment.loc.segment_idx,
                segment
                    .info
                    .summary
                    .additional_stats_meta
                    .as_ref()
                    .map(|v| v.location.clone()),
            ));
            selected_segments.push((segment.loc.segment_idx, segment.info));
        }

        // Gather blocks from selected segments.
        let blocks = self.gather_blocks(selected_segments).await?;
        let selected_segment_count = selected_statistics.len() as u64;
        let selected_block_count = blocks.len() as u64;

        let mut blocks_map: BTreeMap<ReclusterGroup, Vec<usize>> = BTreeMap::new();
        for (idx, block) in blocks.iter().enumerate() {
            if block.stats.level < 0 {
                continue;
            }
            if block.stats.level >= MAX_RECLUSTER_LEVEL {
                debug!(
                    "recluster: skip block segment_idx={} block_idx={} level={} skip_reason=max_recluster_level",
                    block.index.segment_idx, block.index.block_idx, block.stats.level,
                );
                continue;
            }

            let group = match mode {
                ReclusterMode::Normal => ReclusterGroup::Level(block.stats.level),
                ReclusterMode::Final if block.stats.level == 0 => ReclusterGroup::Level(0),
                ReclusterMode::Final => {
                    // FINAL intentionally uses fixed, non-overlapping level bands.
                    // Level 0 is isolated; mature levels are grouped as 1-2, 3-4, ...
                    // Cross-band overlaps such as 2-3 are deliberately not
                    // candidates for the same rewrite task, keeping each task
                    // bounded to a narrow mature-level range.
                    let band = (block.stats.level - 1) / 2;
                    ReclusterGroup::Mixed(band)
                }
            };
            blocks_map.entry(group).or_default().push(idx);
        }

        // Prepare task generation parameters
        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        // The caller may already have built tasks in earlier windows, so this
        // window is bounded by the remaining task budget rather than max_tasks.
        let task_budget = task_budget.min(self.max_tasks);
        let mut tasks = Vec::new();
        let mut selected_blocks_idx = IndexSet::new();

        let mut deferred_level_task = None;
        for (group, indices) in blocks_map.into_iter() {
            let block_count = indices.len();
            if block_count < 2 {
                continue;
            }

            let remaining_budget = task_budget.saturating_sub(tasks.len());
            if remaining_budget == 0 {
                debug!(
                    "recluster: level selection level={} block_count={} skip_reason=task_budget_exhausted",
                    group, block_count,
                );
                break;
            }

            let Some(level_tasks) = self.build_recluster_tasks_for_indices(
                group,
                indices,
                &blocks,
                &column_nodes,
                remaining_budget,
            )?
            else {
                continue;
            };

            // When the first constructible batch is too small, defer it once so
            // the next group gets a chance to consume this round's budget first.
            if tasks.is_empty()
                && deferred_level_task.is_none()
                && level_tasks.selected_block_count < level_tasks.min_blocks_per_task
            {
                debug!(
                    "recluster: level selection level={} block_count={} selected_count={} task_count={} skip_reason=deferred_low_level_batch",
                    group,
                    block_count,
                    level_tasks.selected_block_count,
                    level_tasks.tasks.len(),
                );
                deferred_level_task = Some(level_tasks);
                continue;
            }

            for (task, task_indices) in level_tasks.tasks {
                selected_blocks_idx.extend(task_indices);
                tasks.push(task);
            }

            if tasks.len() >= task_budget {
                debug!(
                    "recluster: task budget reached selected_task_count={} task_budget={}",
                    tasks.len(),
                    task_budget,
                );
                break;
            }
        }

        if tasks.len() < task_budget {
            if let Some(level_tasks) = deferred_level_task {
                // Backfill deferred low-level work when later groups did not use
                // the full budget, so finite low-level data still converges.
                let remaining_budget = task_budget - tasks.len();
                for (task, task_indices) in level_tasks.tasks.into_iter().take(remaining_budget) {
                    debug!(
                        "recluster: backfill deferred level={} selected_count={}",
                        task.level,
                        task_indices.len(),
                    );
                    selected_blocks_idx.extend(task_indices);
                    tasks.push(task);
                }
            }
        }

        // Generate recluster parts based on the segments that will actually be
        // replaced. For block rewrite tasks, keep the mutation scope limited to
        // the segments containing selected blocks. A zero-task rebuild is kept
        // only when repacking unchanged blocks can reduce the segment count,
        // avoiding repeated rebuilds of the active tail segment.
        let block_per_segment = self.block_thresholds.block_per_segment as u64;
        let target_segment_count = selected_block_count.div_ceil(block_per_segment);
        let rebuild_segments_only = selected_blocks_idx.is_empty()
            && selected_segment_count > 1
            && target_segment_count < selected_segment_count;
        let removed_segment_set = if !selected_blocks_idx.is_empty() {
            selected_blocks_idx
                .iter()
                .map(|idx| blocks[*idx].index.segment_idx)
                .collect::<IndexSet<_>>()
        } else if rebuild_segments_only {
            selected_segs_idx.iter().copied().collect()
        } else {
            IndexSet::new()
        };
        let parts = if !removed_segment_set.is_empty() {
            let mut removed_segment_indexes =
                removed_segment_set.iter().copied().collect::<Vec<_>>();
            removed_segment_indexes.sort_unstable_by(|a, b| b.cmp(a));

            let default_cluster_key_id = Some(self.cluster_key_id);
            let mut removed_segment_summary = Statistics::default();
            for (idx, stats) in selected_segs_idx.iter().zip(selected_statistics.iter()) {
                if removed_segment_set.contains(idx) {
                    merge_statistics_mut(
                        &mut removed_segment_summary,
                        stats,
                        default_cluster_key_id,
                    );
                }
            }

            let mut hll_segment_indexes = IndexSet::new();
            let mut remained_blocks = Vec::new();
            for (idx, block) in blocks.into_iter().enumerate() {
                if !removed_segment_set.contains(&block.index.segment_idx)
                    || selected_blocks_idx.contains(&idx)
                {
                    continue;
                }
                let mut block_meta = Arc::unwrap_or_clone(block.meta);
                block_meta.cluster_stats = Some(block.stats);
                hll_segment_indexes.insert(block.index.segment_idx);
                remained_blocks.push((block.index, Arc::new(block_meta)));
            }
            let selected_seg_stats = selected_seg_stats
                .into_iter()
                .filter(|(segment_idx, location)| {
                    location.is_some() && hll_segment_indexes.contains(segment_idx)
                })
                .collect::<Vec<_>>();
            let hlls = self.gather_hlls(selected_seg_stats).await?;
            let remained_blocks = remained_blocks
                .into_iter()
                .map(|(block_index, block_meta)| {
                    let hll = hlls.get(&block_index).and_then(|hll| hll.clone());
                    (block_meta, hll)
                })
                .collect();
            ReclusterParts {
                tasks,
                remained_blocks,
                removed_segment_indexes,
                removed_segment_summary,
            }
        } else {
            ReclusterParts::default()
        };

        debug!(
            "recluster: target_select result selected_blocks={} tasks={} remained_blocks={} removed_segments={} segment_rebuild={} input_blocks={} target_segments={}",
            selected_blocks_idx.len(),
            parts.tasks.len(),
            parts.remained_blocks.len(),
            parts.removed_segment_indexes.len(),
            rebuild_segments_only,
            selected_block_count,
            target_segment_count,
        );

        Ok((selected_blocks_idx.len() as u64, parts))
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
        // Whether a task actually fits is checked in target_select using real block
        // sizes, so small-block tables are not rejected here under low memory.
        Ok(recluster_block_size.min(memory_budget))
    }

    fn build_recluster_tasks_for_indices(
        &self,
        group: ReclusterGroup,
        indices: Vec<usize>,
        blocks: &[ReclusterBlock],
        column_nodes: &ColumnNodes,
        task_budget: usize,
    ) -> Result<Option<LevelReclusterTasks>> {
        let block_count = indices.len();
        if block_count == 2
            && indices
                .iter()
                .all(|idx| blocks[*idx].stats.level >= MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS)
        {
            debug!(
                "recluster: level selection level={} block_count={} skip_reason=high_level_two_blocks",
                group, block_count,
            );
            return Ok(None);
        }

        let mut total_rows = 0;
        let mut total_bytes = 0;
        let mut total_compressed = 0;
        let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();

        for &i in indices.iter() {
            let block = &blocks[i];
            let stats = &block.stats;
            points_map.entry(stats.min().clone()).or_default().0.push(i);
            points_map.entry(stats.max().clone()).or_default().1.push(i);

            total_rows += block.meta.row_count;
            total_bytes += block.meta.block_size;
            total_compressed += block.meta.file_size;
        }

        let max_blocks_num_per_node =
            self.max_blocks_num_per_node(total_bytes as usize, block_count);
        let min_blocks_per_task = (max_blocks_num_per_node * 3 / 4).max(2);

        // If total rows and bytes are too small, compact the blocks into one.
        if self
            .block_thresholds
            .check_for_compact(total_rows as usize, total_bytes as usize)
            && total_bytes as usize <= self.memory_threshold
        {
            debug!(
                "recluster: level selection detail level={} block_count={} rows={} bytes={} selected_count={} task_count=1 compact_small_blocks=true",
                group, block_count, total_rows, total_bytes, block_count,
            );
            let block_metas = indices
                .iter()
                .map(|&i| (None, blocks[i].meta.clone()))
                .collect::<Vec<_>>();
            let output_level = group.output_level(&indices, blocks);
            let task = self.generate_task(
                &block_metas,
                column_nodes,
                total_rows as usize,
                total_bytes as usize,
                total_compressed as usize,
                output_level,
            );

            return Ok(Some(LevelReclusterTasks {
                selected_block_count: block_count,
                min_blocks_per_task,
                tasks: vec![(task, indices)],
            }));
        }

        let max_blocks_num = max_blocks_num_per_node * task_budget;
        // Fetch enough candidates for the remaining workers. The per-node quota is based
        // on the observed block size in selected segments instead of the worst
        // allowed block size, otherwise distributed recluster can under-select.
        let DepthSelection {
            mut selected_idx,
            average_depth,
            max_depth,
            max_point_overlap_count,
        } = self.fetch_max_depth(points_map, self.depth_threshold, max_blocks_num)?;
        let max_total_bytes = self.memory_threshold.saturating_mul(task_budget);
        // Keep the first, highest-depth blocks within the remaining execution budget.
        // This is a second-stage guard after candidate selection: the average
        // block size is only an estimate, while task generation uses real bytes.
        let selected_total_bytes =
            Self::limit_selected_blocks_by_budget(&mut selected_idx, blocks, max_total_bytes);
        let selected_block_count = selected_idx.len();
        if selected_block_count < 2 {
            debug!(
                "recluster: level selection detail level={} block_count={} average_depth={} max_depth={} max_point_overlap_count={} selected_count={} skip_reason=selected_less_than_two_after_budget",
                group,
                block_count,
                average_depth,
                max_depth,
                max_point_overlap_count,
                selected_block_count,
            );
            return Ok(None);
        }

        let target_tasks_by_blocks = selected_block_count / min_blocks_per_task;
        let target_tasks_by_memory = selected_total_bytes.div_ceil(self.memory_threshold);
        // A recluster task needs at least two blocks, so this caps parallelism
        // when the selected candidate set is too small for every worker.
        let max_tasks_by_blocks = selected_block_count / 2;
        let target_tasks = target_tasks_by_blocks
            .max(target_tasks_by_memory)
            .max(1)
            .min(task_budget)
            .min(max_tasks_by_blocks);
        let target_task_bytes = selected_total_bytes.div_ceil(target_tasks);
        let target_task_blocks = selected_block_count.div_ceil(target_tasks);

        // Process selected blocks into recluster tasks based on memory and parallelism targets.
        let mut tasks = Vec::new();
        let mut task_bytes = 0usize;
        let mut task_rows = 0;
        let mut task_compressed = 0;
        let mut task_indices = Vec::new();
        let mut selected_blocks = Vec::new();
        for (processed_blocks, idx) in selected_idx.into_iter().enumerate() {
            let block = blocks[idx].meta.clone();
            let block_size = block.block_size as usize;
            let row_count = block.row_count as usize;

            let remaining_tasks = target_tasks.saturating_sub(tasks.len() + 1);
            let remaining_blocks = selected_block_count.saturating_sub(processed_blocks);
            // Only split for parallelism when the remaining blocks can still
            // satisfy the minimum task size for the tasks left to create.
            let has_enough_remaining_blocks =
                remaining_blocks >= remaining_tasks * min_blocks_per_task;
            let should_split_for_memory =
                task_bytes.saturating_add(block_size) > self.memory_threshold;
            // Memory split is the hard safety guard. Parallel split is a load
            // balancing target and is allowed only while preserving task size.
            let should_split_for_parallelism = tasks.len() + 1 < target_tasks
                && selected_blocks.len() >= min_blocks_per_task
                && has_enough_remaining_blocks
                && (task_bytes >= target_task_bytes || selected_blocks.len() >= target_task_blocks);

            if should_split_for_memory || should_split_for_parallelism {
                let selected_task_indices = std::mem::take(&mut task_indices);
                let selected_block_metas = std::mem::take(&mut selected_blocks);
                // Keep the selected block order stable. If the memory boundary
                // is reached while only one block is pending, that singleton
                // cannot form a normal recluster task, so drop it and let the
                // current block start the next accumulator.
                if selected_block_metas.len() >= 2 {
                    let output_level = group.output_level(&selected_task_indices, blocks);
                    tasks.push((
                        self.generate_task(
                            &selected_block_metas,
                            column_nodes,
                            task_rows,
                            task_bytes,
                            task_compressed,
                            output_level,
                        ),
                        selected_task_indices,
                    ));
                }

                task_rows = 0;
                task_bytes = 0;
                task_compressed = 0;

                // Break if maximum task limit is reached
                if tasks.len() >= target_tasks {
                    break;
                }
            }

            task_rows += row_count;
            task_bytes += block_size;
            task_compressed += block.file_size as usize;
            task_indices.push(idx);
            selected_blocks.push((None, block));
        }

        // Add remaining blocks as a final task if any. The tail can be a tiny
        // remainder after splitting; skip it so recluster does not become a
        // small-block compact path.
        if selected_blocks.len() > 1 {
            let output_level = group.output_level(&task_indices, blocks);
            tasks.push((
                self.generate_task(
                    &selected_blocks,
                    column_nodes,
                    task_rows,
                    task_bytes,
                    task_compressed,
                    output_level,
                ),
                task_indices,
            ));
        }

        if tasks.is_empty() {
            debug!(
                "recluster: level selection detail level={} block_count={} average_depth={} max_depth={} max_point_overlap_count={} selected_count={} skip_reason=no_task_after_split",
                group,
                block_count,
                average_depth,
                max_depth,
                max_point_overlap_count,
                selected_block_count,
            );
            return Ok(None);
        }

        info!(
            "recluster: built level task candidates level={} block_count={} avg_depth={} depth_threshold={} max_depth={} max_point_overlap_count={} selected_count={} task_count={}",
            group,
            block_count,
            average_depth,
            self.depth_threshold,
            max_depth,
            max_point_overlap_count,
            selected_block_count,
            tasks.len(),
        );

        Ok(Some(LevelReclusterTasks {
            selected_block_count,
            min_blocks_per_task,
            tasks,
        }))
    }

    fn generate_task(
        &self,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        total_rows: usize,
        total_bytes: usize,
        total_compressed: usize,
        level: i32,
    ) -> ReclusterTask {
        let (stats, parts) =
            FuseTable::to_partitions(Some(&self.schema), block_metas, column_nodes, None, None);
        ReclusterTask {
            parts,
            stats,
            total_rows,
            total_bytes,
            total_compressed,
            level,
        }
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

    fn limit_selected_blocks_by_budget(
        selected_idx: &mut IndexSet<usize>,
        blocks: &[ReclusterBlock],
        max_total_bytes: usize,
    ) -> usize {
        let mut total_bytes = 0usize;
        let mut keep_blocks = 0;
        for idx in selected_idx.iter().copied() {
            let block_size = blocks[idx].meta.block_size as usize;
            if keep_blocks >= 2 && total_bytes.saturating_add(block_size) > max_total_bytes {
                break;
            }
            total_bytes = total_bytes.saturating_add(block_size);
            keep_blocks += 1;
        }
        selected_idx.truncate(keep_blocks);
        total_bytes
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
            let segment =
                SelectedReclusterSegment::create(self, loc.clone(), compact_segment.clone());
            let level = segment.stats.level;

            if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
                continue;
            }

            total_blocks += compact_segment.summary.block_count as usize;
            segment_points
                .entry(segment.stats.min().clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));
            segment_points
                .entry(segment.stats.max().clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            segments[i] = Some(segment);
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
        let sorted_indices = compare_scalars(keys, &self.cluster_key_types)?;

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
        // window because it gives target_select more room to build tasks.
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

    #[async_backtrace::framed]
    async fn gather_blocks(
        &self,
        compact_segments: Vec<(usize, Arc<CompactSegmentInfo>)>,
    ) -> Result<Vec<ReclusterBlock>> {
        let tasks = compact_segments.into_iter().map(|(segment_idx, v)| {
            async move {
                v.block_metas().map(|block_metas| {
                    block_metas
                        .into_iter()
                        .enumerate()
                        .map(|(block_idx, block_meta)| {
                            (
                                BlockIndex {
                                    segment_idx,
                                    block_idx,
                                },
                                block_meta,
                            )
                        })
                        .collect::<Vec<_>>()
                })
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
            .map(|(block_index, block_meta)| {
                // Normalize cluster stats after gathering blocks. Missing or stale
                // stats are rebuilt once and reused by selection and remained block rebuild.
                let stats = self.build_cluster_stats_for_recluster(
                    block_meta.cluster_stats.as_ref(),
                    &block_meta.col_stats,
                );
                ReclusterBlock {
                    index: block_index,
                    meta: block_meta,
                    stats,
                }
            })
            .collect())
    }

    #[async_backtrace::framed]
    async fn gather_hlls(
        &self,
        hlls: Vec<(usize, Option<Location>)>,
    ) -> Result<HashMap<BlockIndex, Option<RawBlockHLL>>> {
        if hlls.is_empty() {
            return Ok(HashMap::new());
        }

        let tasks = hlls.into_iter().map(|(segment_idx, location)| {
            let dal = self.operator.clone();
            async move {
                let Some((loc, ver)) = location else {
                    return Ok(vec![]);
                };
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
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
        max_len: usize,
    ) -> Result<DepthSelection> {
        let mut max_depth = 0;
        let mut max_point = 0;
        let mut interval_depths = HashMap::with_capacity(points_map.len());
        let mut point_overlaps: Vec<Vec<usize>> = Vec::with_capacity(points_map.len());
        let mut unfinished_intervals = BTreeMap::new();
        let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
        let indices = compare_scalars(keys, &self.cluster_key_types)?;
        for (i, idx) in indices.into_iter().enumerate() {
            let start = &values[idx as usize].0;
            let end = &values[idx as usize].1;
            let point_depth = Self::calc_point_depth(unfinished_intervals.len(), start, end);

            if point_depth > max_depth {
                max_depth = point_depth;
                max_point = i;
            }

            unfinished_intervals
                .values_mut()
                .for_each(|val| *val = cmp::max(*val, point_depth));

            start.iter().for_each(|&idx| {
                unfinished_intervals.insert(idx, point_depth);
            });

            point_overlaps.push(unfinished_intervals.keys().cloned().collect());

            end.iter().for_each(|idx| {
                if let Some(v) = unfinished_intervals.remove(idx) {
                    interval_depths.insert(*idx, v);
                }
            });
        }

        let mut selected_idx = IndexSet::with_capacity(max_len);
        if !unfinished_intervals.is_empty() {
            warn!(
                "recluster: unfinished intervals remain after calculating block overlaps count={} max_selected={}",
                unfinished_intervals.len(),
                max_len,
            );
            // Keep unfinished intervals selected first; they indicate malformed
            // or stale range boundaries but are still valid recluster candidates.
            unfinished_intervals.keys().for_each(|idx| {
                selected_idx.insert(*idx);
            });
        }

        let sum_depth: usize = interval_depths.values().sum();
        // round the float to 4 decimal places.
        let average_depth =
            (10000.0 * sum_depth as f64 / interval_depths.len() as f64).round() / 10000.0;
        let max_point_overlap_count = point_overlaps[max_point].len();

        // Decide whether to expand from the highest-depth point to neighboring
        // hot points. Normal and FINAL intentionally share the same gate:
        // average depth follows the configured threshold, while max depth uses
        // a higher threshold to avoid repeatedly rewriting moderate isolated
        // hotspots. Cap that max-depth threshold at MAX_RECLUSTER_DEPTH so a
        // high configured threshold does not make hotspot detection unreachable.
        let max_depth_threshold = (2.0 * depth_threshold).min(MAX_RECLUSTER_DEPTH as f64);
        let should_expand =
            average_depth > depth_threshold || max_depth as f64 > max_depth_threshold;
        if should_expand {
            point_overlaps[max_point].iter().for_each(|idx| {
                selected_idx.insert(*idx);
            });

            let mut left = max_point;
            let mut right = max_point;
            while selected_idx.len() < max_len {
                let left_depth = if left > 0 {
                    let point_overlap = &point_overlaps[left - 1];
                    // Calculate the depth of the point.
                    let depth = point_overlap.len();
                    if point_overlap
                        .iter()
                        .all(|v| interval_depths.get(v) == Some(&1))
                    {
                        // If all overlapping intervals have a depth of 1,
                        // it indicates that these intervals donot overlap significantly.
                        // Set left to indicate that the traversal on the left side is complete.
                        left = 0;
                        0.0
                    } else {
                        depth as f64
                    }
                } else {
                    0.0
                };

                let right_depth = if right < point_overlaps.len() - 1 {
                    let point_overlap = &point_overlaps[right + 1];
                    let depth = point_overlap.len();
                    if point_overlap
                        .iter()
                        .all(|v| interval_depths.get(v) == Some(&1))
                    {
                        // If all overlapping intervals have a depth of 1,
                        // it indicates that these intervals donot overlap significantly.
                        // Set right to indicate that the traversal on the left side is complete.
                        right = point_overlaps.len() - 1;
                        0.0
                    } else {
                        depth as f64
                    }
                } else {
                    0.0
                };

                let max_depth = left_depth.max(right_depth);
                if max_depth <= depth_threshold {
                    break;
                }

                if left_depth >= right_depth {
                    left -= 1;
                    let mut merged_idx = IndexSet::new();
                    point_overlaps[left].iter().for_each(|idx| {
                        merged_idx.insert(*idx);
                    });
                    merged_idx.extend(selected_idx);
                    selected_idx = merged_idx;
                } else {
                    right += 1;
                    point_overlaps[right].iter().for_each(|idx| {
                        selected_idx.insert(*idx);
                    });
                }
            }
        }

        selected_idx.truncate(max_len);
        Ok(DepthSelection {
            selected_idx,
            average_depth,
            max_depth,
            max_point_overlap_count,
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
