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
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
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
use log::warn;
use opendal::Operator;

use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;
use crate::FuseTable;
use crate::SegmentLocation;
use crate::io::MetaReaders;
use crate::operations::common::BlockMetaIndex as BlockIndex;
use crate::statistics::get_min_max_stats;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;

/// Maximum recluster depth allowed when only two blocks remain.
/// For two-block layouts, repeated reclustering beyond this level
/// rarely improves data locality and may cause task churn.
const MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS: i32 = 2;

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

        let avg_depth_threshold = table.get_option(
            FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD,
            DEFAULT_AVG_DEPTH_THRESHOLD,
        );
        let depth_threshold =
            (snapshot.summary.block_count as f64 * avg_depth_threshold).clamp(1.0, 16.0);

        let mut max_tasks = 1;
        let cluster = ctx.get_cluster();
        if !cluster.is_empty() && ctx.get_settings().get_enable_distributed_recluster()? {
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
        Self {
            ctx,
            operator,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            cluster_key_exprs,
            cluster_key_types,
        }
    }

    #[async_backtrace::framed]
    pub async fn target_select(
        &self,
        compact_segments: Vec<SelectedReclusterSegment>,
    ) -> Result<(u64, ReclusterParts)> {
        let mut compact_segments = compact_segments;
        compact_segments.sort_by(|a, b| {
            sort_by_cluster_stats(
                &Some(a.stats.clone()),
                &Some(b.stats.clone()),
                self.cluster_key_id,
            )
        });

        // Prepare for reclustering by collecting segment indices, statistics, and blocks
        let mut selected_segs_idx = Vec::with_capacity(compact_segments.len());
        let mut selected_statistics = Vec::with_capacity(compact_segments.len());
        let mut selected_seg_stats = Vec::with_capacity(compact_segments.len());
        let selected_segments = compact_segments
            .into_iter()
            .map(|segment| {
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
                (segment.loc.segment_idx, segment.info)
            })
            .collect::<Vec<_>>();

        // Gather blocks and create a block map categorized by clustering levels
        let blocks = self.gather_blocks(selected_segments).await?;
        let block_stats = blocks
            .iter()
            .map(|(_, block)| {
                self.build_cluster_stats_for_recluster(
                    block.cluster_stats.as_ref(),
                    &block.col_stats,
                )
            })
            .collect::<Vec<_>>();
        let mut blocks_map: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for (idx, stats) in block_stats.iter().enumerate() {
            blocks_map.entry(stats.level).or_default().push(idx);
        }

        // Use the configured recluster budget as a stable scheduling target. Runtime
        // memory usage is intentionally not folded in here because sort spill can
        // absorb pressure and the available memory snapshot changes during execution.
        let settings = self.ctx.get_settings();
        let memory_threshold = settings.get_recluster_block_size()? as usize;
        let block_per_seg = self.block_thresholds.block_per_segment;

        // Prepare task generation parameters
        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        let mut tasks = Vec::new();
        let mut selected_blocks_idx = IndexSet::new();
        let max_level = blocks_map.keys().last().copied();
        // Process blocks for reclustering based on their level
        for (level, indices) in blocks_map.into_iter() {
            let block_count = indices.len();
            if level == -1 || block_count < 2 {
                continue;
            }

            // For two-block scenarios at the deepest level, limit reclustering depth
            // to avoid generating repeated tasks with little benefit.
            if block_count == 2
                && Some(level) == max_level
                && level >= MAX_RECLUSTER_LEVEL_FOR_TWO_BLOCKS
            {
                debug!(
                    "recluster: only two blocks remain at level {}, skipping to avoid task churn",
                    level
                );
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut total_compressed = 0;
            let mut small_blocks = IndexSet::new();
            let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();

            // Analyze each block's statistics and track min/max points
            for &i in indices.iter() {
                let block = &blocks[i];
                let stats = &block_stats[i];
                points_map.entry(stats.min().clone()).or_default().0.push(i);
                points_map.entry(stats.max().clone()).or_default().1.push(i);

                // Track small blocks for potential compaction
                if self.block_thresholds.check_too_small(
                    block.1.row_count as usize,
                    block.1.block_size as usize,
                    block.1.file_size as usize,
                ) {
                    small_blocks.insert(i);
                }

                total_rows += block.1.row_count;
                total_bytes += block.1.block_size;
                total_compressed += block.1.file_size;
            }

            // If total rows and bytes are too small, compact the blocks into one
            if self
                .block_thresholds
                .check_for_compact(total_rows as usize, total_bytes as usize)
            {
                debug!(
                    "recluster: the statistics of blocks are too small, just merge them into one block"
                );
                selected_blocks_idx.extend(indices.iter());
                let block_metas = indices
                    .into_iter()
                    .map(|i| (None, blocks[i].1.clone()))
                    .collect::<Vec<_>>();
                tasks.push(self.generate_task(
                    &block_metas,
                    &column_nodes,
                    total_rows as usize,
                    total_bytes as usize,
                    total_compressed as usize,
                    level,
                ));
                break;
            }

            let max_blocks_num_per_node =
                self.max_blocks_num_per_node(total_bytes as usize, block_count, memory_threshold);
            let max_blocks_num = max_blocks_num_per_node * self.max_tasks;
            // Fetch enough candidates for all workers. The per-node quota is based
            // on the observed block size in selected segments instead of the worst
            // allowed block size, otherwise distributed recluster can under-select.
            let mut selected_idx =
                self.fetch_max_depth_windows(points_map, self.depth_threshold, max_blocks_num)?;
            if selected_idx.is_empty() {
                if level != 0 || small_blocks.len() < 2 {
                    continue;
                }
                selected_idx = IndexSet::from_iter(small_blocks);
            }

            let max_total_bytes = memory_threshold.saturating_mul(self.max_tasks);
            // Keep the first, highest-depth blocks within the total execution budget.
            // This is a second-stage guard after candidate selection: the average
            // block size is only an estimate, while task generation uses real bytes.
            let selected_total_bytes =
                Self::limit_selected_blocks_by_budget(&mut selected_idx, &blocks, max_total_bytes);
            let selected_block_count = selected_idx.len();
            if selected_block_count < 2 {
                continue;
            }

            let min_blocks_per_task = (max_blocks_num_per_node / 2).max(2);
            let target_tasks_by_blocks = selected_block_count / min_blocks_per_task;
            let target_tasks_by_memory = selected_total_bytes.div_ceil(memory_threshold);
            // A recluster task needs at least two blocks, so this caps parallelism
            // when the selected candidate set is too small for every worker.
            let max_tasks_by_blocks = selected_block_count / 2;
            let target_tasks = target_tasks_by_blocks
                .max(target_tasks_by_memory)
                .max(1)
                .min(self.max_tasks)
                .min(max_tasks_by_blocks);
            let target_task_bytes = selected_total_bytes.div_ceil(target_tasks);
            let target_task_blocks = selected_block_count.div_ceil(target_tasks);

            // Process selected blocks into recluster tasks based on memory and parallelism targets.
            let mut task_bytes = 0usize;
            let mut task_rows = 0;
            let mut task_compressed = 0;
            let mut task_indices = Vec::new();
            let mut selected_blocks = Vec::new();
            for (processed_blocks, idx) in selected_idx.into_iter().enumerate() {
                let block = blocks[idx].1.clone();
                let block_size = block.block_size as usize;
                let row_count = block.row_count as usize;

                let remaining_tasks = target_tasks.saturating_sub(tasks.len() + 1);
                let remaining_blocks = selected_block_count.saturating_sub(processed_blocks);
                // Only split for parallelism when the remaining blocks can still
                // satisfy the minimum task size for the tasks left to create.
                let has_enough_remaining_blocks =
                    remaining_blocks >= remaining_tasks * min_blocks_per_task;
                let should_split_for_memory = task_bytes.saturating_add(block_size)
                    > memory_threshold
                    && selected_blocks.len() > 1;
                // Memory split is the hard safety guard. Parallel split is a load
                // balancing target and is allowed only while preserving task size.
                let should_split_for_parallelism = tasks.len() + 1 < target_tasks
                    && selected_blocks.len() >= min_blocks_per_task
                    && has_enough_remaining_blocks
                    && (task_bytes >= target_task_bytes
                        || selected_blocks.len() >= target_task_blocks);

                if should_split_for_memory || should_split_for_parallelism {
                    selected_blocks_idx.extend(std::mem::take(&mut task_indices));

                    tasks.push(self.generate_task(
                        &selected_blocks,
                        &column_nodes,
                        task_rows,
                        task_bytes,
                        task_compressed,
                        level,
                    ));

                    task_rows = 0;
                    task_bytes = 0;
                    task_compressed = 0;
                    selected_blocks.clear();

                    // Break if maximum task limit is reached
                    if tasks.len() >= self.max_tasks {
                        break;
                    }
                }

                task_rows += row_count;
                task_bytes += block_size;
                task_compressed += block.file_size as usize;
                task_indices.push(idx);
                selected_blocks.push((None, block));
            }

            // Add remaining blocks as a final task if any
            if selected_blocks.len() > 1 {
                selected_blocks_idx.extend(task_indices);
                tasks.push(self.generate_task(
                    &selected_blocks,
                    &column_nodes,
                    task_rows,
                    task_bytes,
                    task_compressed,
                    level,
                ));
            }
            if tasks.len() >= self.max_tasks {
                break;
            }
        }

        // Determine if reclustering is needed.
        // Derived stats may participate in ordering/selection even when no block-level rewrite
        // task is chosen. In that case, a zero-task result is still meaningful: commit will use
        // `remained_blocks` to rebuild segments in the selected order, rather than treating the
        // recluster as a no-op.
        let selected = if selected_blocks_idx.is_empty() {
            let unordered = || {
                block_stats.windows(2).any(|w| {
                    sort_by_cluster_stats(
                        &Some(w[0].clone()),
                        &Some(w[1].clone()),
                        self.cluster_key_id,
                    ) == Ordering::Greater
                })
            };
            (selected_segs_idx.len() > 1 && blocks.len() <= block_per_seg) || unordered()
        } else {
            true
        };

        // Generate recluster parts based on selected segments
        let parts = if selected {
            selected_segs_idx.sort_by(|a, b| b.cmp(a));

            let default_cluster_key_id = Some(self.cluster_key_id);
            let mut removed_segment_summary = Statistics::default();
            selected_statistics.iter().for_each(|v| {
                merge_statistics_mut(&mut removed_segment_summary, v, default_cluster_key_id)
            });

            let remained_blocks = blocks
                .into_iter()
                .enumerate()
                .filter_map(|(idx, (block_index, block_meta))| {
                    if selected_blocks_idx.contains(&idx) {
                        return None;
                    }
                    let mut block_meta = Arc::unwrap_or_clone(block_meta);
                    block_meta.cluster_stats = Some(block_stats[idx].clone());
                    Some((block_index, Arc::new(block_meta)))
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
                removed_segment_indexes: selected_segs_idx,
                removed_segment_summary,
            }
        } else {
            ReclusterParts::default()
        };

        Ok((selected_blocks_idx.len() as u64, parts))
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
        if log::log_enabled!(log::Level::Debug) {
            let locations = block_metas
                .iter()
                .map(|v| &v.1.location.0)
                .collect::<Vec<_>>();
            debug!(
                "recluster: generate recluster task, the selected blocks: {:?}, level: {}",
                locations, level
            );
        }

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

    fn max_blocks_num_per_node(
        &self,
        total_bytes: usize,
        block_count: usize,
        memory_threshold: usize,
    ) -> usize {
        let avg_block_bytes = (total_bytes / block_count).max(1);
        // Clamp the observed average to normal block thresholds so tiny fragments
        // do not inflate the candidate count and unusually large blocks do not
        // make the distributed selection overly conservative.
        let target_block_bytes = avg_block_bytes
            .max(self.block_thresholds.min_bytes_per_block)
            .min(self.block_thresholds.max_bytes_per_block);
        (memory_threshold / target_block_bytes).max(2)
    }

    fn limit_selected_blocks_by_budget(
        selected_idx: &mut IndexSet<usize>,
        blocks: &[(BlockIndex, Arc<BlockMeta>)],
        max_total_bytes: usize,
    ) -> usize {
        let mut total_bytes = 0usize;
        let mut keep_blocks = 0;
        for idx in selected_idx.iter().copied() {
            let block_size = blocks[idx].1.block_size as usize;
            if keep_blocks >= 2 && total_bytes.saturating_add(block_size) > max_total_bytes {
                break;
            }
            total_bytes = total_bytes.saturating_add(block_size);
            keep_blocks += 1;
        }
        selected_idx.truncate(keep_blocks);
        total_bytes
    }

    pub fn select_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        max_len: usize,
    ) -> Result<Vec<SelectedReclusterSegment>> {
        let mut blocks_num = 0;
        let mut indices = IndexSet::new();
        let mut segment_stats = Vec::with_capacity(compact_segments.len());
        let mut segment_block_counts = Vec::with_capacity(compact_segments.len());
        let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
        let mut small_segments = IndexSet::new();
        let block_per_seg = self.block_thresholds.block_per_segment;

        // Iterate over all segments
        for (i, (_, compact_segment)) in compact_segments.iter().enumerate() {
            let stats = self.build_cluster_stats_for_recluster(
                compact_segment.summary.cluster_stats.as_ref(),
                &compact_segment.summary.col_stats,
            );
            let level = stats.level;

            // Skip if segment has more blocks than required and no reclustering is needed
            if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
                segment_stats.push(None);
                segment_block_counts.push(0);
                continue;
            }

            let segment_blocks = compact_segment.summary.block_count as usize;
            segment_block_counts.push(segment_blocks);
            blocks_num += segment_blocks;
            // Track small segments for special handling later
            if blocks_num < block_per_seg {
                small_segments.insert(i);
            }
            // Add to indices for potential reclustering
            indices.insert(i);
            // Update points_map with min and max points of the segment
            points_map
                .entry(stats.min().clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));
            points_map
                .entry(stats.max().clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            segment_stats.push(Some(stats));
        }

        let selected_indices = if indices.len() > 1 && blocks_num > block_per_seg {
            let max_selected_blocks = max_len.saturating_mul(block_per_seg);
            let selected = self.fetch_max_depth_by_block_budget(
                points_map,
                1.0,
                max_len,
                &segment_block_counts,
                max_selected_blocks,
            )?;
            if selected.is_empty() && small_segments.len() > 1 {
                // If no segments were selected but small segments exist, use those.
                small_segments
            } else {
                selected
            }
        } else {
            indices
        };

        Ok(selected_indices
            .into_iter()
            .filter_map(|i| {
                segment_stats[i].take().map(|stats| {
                    let (loc, info) = &compact_segments[i];
                    SelectedReclusterSegment {
                        loc: loc.clone(),
                        info: info.clone(),
                        stats,
                    }
                })
            })
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
    ) -> Result<Vec<(BlockIndex, Arc<BlockMeta>)>> {
        // combine all the tasks.
        let mut iter = compact_segments.into_iter();
        let tasks = std::iter::from_fn(|| {
            iter.next().map(|(segment_idx, v)| {
                async move {
                    v.block_metas().map(|block_metas| {
                        block_metas
                            .into_iter()
                            .enumerate()
                            .map(|(block_idx, block_meta)| {
                                let block_index = BlockIndex {
                                    segment_idx,
                                    block_idx,
                                };
                                (block_index, block_meta)
                            })
                            .collect::<Vec<_>>()
                    })
                }
                .in_span(Span::enter_with_local_parent(func_path!()))
            })
        });

        let thread_nums = self.ctx.get_settings().get_max_threads()? as usize;

        let joint = execute_futures_in_parallel(
            tasks,
            thread_nums,
            thread_nums * 2,
            "convert-segments-worker".to_owned(),
        )
        .await?;
        let mut blocks = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        blocks.sort_by(|left, right| {
            left.0
                .segment_idx
                .cmp(&right.0.segment_idx)
                .then_with(|| left.0.block_idx.cmp(&right.0.block_idx))
        });
        Ok(blocks)
    }

    #[async_backtrace::framed]
    async fn gather_hlls(
        &self,
        hlls: Vec<(usize, Option<Location>)>,
    ) -> Result<HashMap<BlockIndex, Option<RawBlockHLL>>> {
        // combine all the tasks.
        let mut iter = hlls.into_iter();
        let tasks = std::iter::from_fn(|| {
            iter.next().map(|(segment_idx, location)| {
                let dal = self.operator.clone();
                async move {
                    if let Some((loc, ver)) = location {
                        let reader = MetaReaders::segment_stats_reader(dal);
                        let load_params = LoadParams {
                            location: loc,
                            len_hint: None,
                            ver,
                            put_cache: false,
                        };
                        let stats = reader.read(&load_params).await?;
                        let res = stats
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
                            .collect::<Vec<_>>();
                        Ok(res)
                    } else {
                        Ok(vec![])
                    }
                }
                .in_span(Span::enter_with_local_parent(func_path!()))
            })
        });

        let thread_nums = self.ctx.get_settings().get_max_threads()? as usize;

        let joint = execute_futures_in_parallel(
            tasks,
            thread_nums,
            thread_nums * 2,
            "convert-segments-worker".to_owned(),
        )
        .await?;
        let mut blocks = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        blocks.sort_by(|left, right| {
            left.0
                .segment_idx
                .cmp(&right.0.segment_idx)
                .then_with(|| left.0.block_idx.cmp(&right.0.block_idx))
        });
        Ok(blocks.into_iter().collect())
    }

    fn analyze_depth_points(
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
    ) -> Result<(Vec<Vec<usize>>, HashMap<usize, usize>, usize, usize, f64)> {
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
            let point_depth = if unfinished_intervals.len() == 1 && Self::check_point(start, end) {
                1
            } else {
                unfinished_intervals.len() + start.len()
            };

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

        if !unfinished_intervals.is_empty() {
            warn!(
                "Recluster: unfinished_intervals is not empty after calculate the blocks overlaps"
            );
        }

        let sum_depth: usize = interval_depths.values().sum();
        // round the float to 4 decimal places.
        let average_depth =
            (10000.0 * sum_depth as f64 / interval_depths.len() as f64).round() / 10000.0;
        Ok((
            point_overlaps,
            interval_depths,
            max_depth,
            max_point,
            average_depth,
        ))
    }

    fn fetch_max_depth_windows(
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
        max_len: usize,
    ) -> Result<IndexSet<usize>> {
        let (point_overlaps, interval_depths, max_depth, _max_point, average_depth) =
            self.analyze_depth_points(points_map)?;
        debug!(
            "recluster: average_depth: {}, max_depth: {}",
            average_depth, max_depth
        );

        if average_depth <= depth_threshold && max_depth as f64 <= depth_threshold * 2.0 {
            return Ok(IndexSet::new());
        }

        let mut candidates = point_overlaps
            .iter()
            .enumerate()
            .filter_map(|(point, overlaps)| {
                let effective_blocks = overlaps
                    .iter()
                    .filter(|idx| interval_depths.get(idx).is_some_and(|depth| *depth > 1))
                    .copied()
                    .collect::<Vec<_>>();
                let effective_depth = effective_blocks.len();
                let depth_score = effective_blocks
                    .iter()
                    .filter_map(|idx| interval_depths.get(idx).copied())
                    .sum::<usize>();
                let max_interval_depth = effective_blocks
                    .iter()
                    .filter_map(|idx| interval_depths.get(idx).copied())
                    .max()
                    .unwrap_or(0);
                let min_index = effective_blocks.iter().min().copied().unwrap_or(usize::MAX);
                (effective_depth as f64 > depth_threshold).then_some((
                    point,
                    depth_score,
                    max_interval_depth,
                    effective_depth,
                    overlaps.len(),
                    min_index,
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| right.2.cmp(&left.2))
                .then_with(|| right.3.cmp(&left.3))
                .then_with(|| right.4.cmp(&left.4))
                .then_with(|| left.5.cmp(&right.5))
                .then_with(|| left.0.cmp(&right.0))
        });

        let mut selected_idx = IndexSet::with_capacity(max_len);
        let mut selected_windows = 0usize;
        for (point, _, _, _, _, _) in candidates {
            if selected_idx.len() >= max_len {
                break;
            }

            let mut window = point_overlaps[point]
                .iter()
                .filter(|idx| interval_depths.get(idx).is_some_and(|depth| *depth > 1))
                .copied()
                .collect::<Vec<_>>();
            window.sort_unstable();
            window.retain(|idx| !selected_idx.contains(idx));
            if window.len() < 2 {
                continue;
            }

            let remaining = max_len.saturating_sub(selected_idx.len());
            if window.len() > remaining {
                continue;
            }

            if window.len() < 2 {
                continue;
            }

            selected_idx.extend(window);
            selected_windows += 1;
        }

        selected_idx.truncate(max_len);
        debug!(
            "recluster: selected {} windows, {} blocks",
            selected_windows,
            selected_idx.len()
        );
        Ok(selected_idx)
    }

    fn fetch_max_depth_by_block_budget(
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
        max_len: usize,
        block_counts: &[usize],
        max_blocks: usize,
    ) -> Result<IndexSet<usize>> {
        let (point_overlaps, interval_depths, _max_depth, max_point, average_depth) =
            self.analyze_depth_points(points_map)?;
        debug!("recluster: average_depth: {}", average_depth);

        let mut selected_idx = IndexSet::with_capacity(max_len);
        if average_depth <= depth_threshold {
            return Ok(selected_idx);
        }

        let mut selected_blocks = 0usize;
        point_overlaps[max_point].iter().for_each(|idx| {
            selected_blocks = selected_blocks.saturating_add(block_counts[*idx]);
            selected_idx.insert(*idx);
        });

        let mut left = max_point;
        let mut right = max_point;
        while selected_idx.len() < max_len {
            let left_depth = if left > 0 {
                let point_overlap = &point_overlaps[left - 1];
                let depth = point_overlap.len();
                if point_overlap
                    .iter()
                    .all(|v| interval_depths.get(v) == Some(&1))
                {
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
                let point = left - 1;
                let next_blocks =
                    Self::new_selected_blocks(&point_overlaps[point], &selected_idx, block_counts);
                if selected_blocks > 0 && selected_blocks.saturating_add(next_blocks) > max_blocks {
                    break;
                }

                left -= 1;
                let mut merged_idx = IndexSet::new();
                point_overlaps[left].iter().for_each(|idx| {
                    merged_idx.insert(*idx);
                });
                merged_idx.extend(selected_idx);
                selected_idx = merged_idx;
                selected_blocks = selected_blocks.saturating_add(next_blocks);
            } else {
                let point = right + 1;
                let next_blocks =
                    Self::new_selected_blocks(&point_overlaps[point], &selected_idx, block_counts);
                if selected_blocks > 0 && selected_blocks.saturating_add(next_blocks) > max_blocks {
                    break;
                }

                right += 1;
                point_overlaps[right].iter().for_each(|idx| {
                    selected_idx.insert(*idx);
                });
                selected_blocks = selected_blocks.saturating_add(next_blocks);
            }
        }

        selected_idx.truncate(max_len);
        Ok(selected_idx)
    }

    fn new_selected_blocks(
        candidates: &[usize],
        selected_idx: &IndexSet<usize>,
        block_counts: &[usize],
    ) -> usize {
        if block_counts.is_empty() {
            return 0;
        }
        candidates
            .iter()
            .filter(|idx| !selected_idx.contains(*idx))
            .map(|idx| block_counts[*idx])
            .sum()
    }

    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    pub fn check_point(start: &[usize], end: &[usize]) -> bool {
        if start.len() + end.len() > 3 || start.is_empty() || end.is_empty() {
            return false;
        }

        let mut first = None;
        let mut second = None;
        for idx in start.iter().chain(end.iter()) {
            match (first, second) {
                (None, _) => first = Some(idx),
                (Some(first), None) if first != idx => second = Some(idx),
                (Some(first), Some(second)) if first != idx && second != idx => return false,
                _ => {}
            }
        }
        first.is_some() && second.is_some()
    }
}
