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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
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

        // Compute memory threshold and maximum number of blocks allowed for reclustering.
        let settings = self.ctx.get_settings();
        let avail_memory_usage =
            settings.get_max_memory_usage()? - GLOBAL_MEM_STAT.get_memory_usage() as u64;
        let memory_threshold = settings
            .get_recluster_block_size()?
            .min(avail_memory_usage * 30 / 100) as usize;
        // specify a rather small value, so that `recluster_block_size` might be tuned to lower value.
        let max_blocks_num =
            (memory_threshold / self.block_thresholds.max_bytes_per_block).max(2) * self.max_tasks;
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

            // Select blocks for reclustering based on depth threshold and max block size
            let mut selected_idx =
                self.fetch_max_depth(points_map, self.depth_threshold, max_blocks_num)?;
            if selected_idx.is_empty() {
                if level != 0 || small_blocks.len() < 2 {
                    continue;
                }
                selected_idx = IndexSet::from_iter(small_blocks);
            }

            // Process selected blocks into recluster tasks based on memory threshold
            let mut task_bytes = 0;
            let mut task_rows = 0;
            let mut task_compressed = 0;
            let mut task_indices = Vec::new();
            let mut selected_blocks = Vec::new();
            for idx in selected_idx {
                let block = blocks[idx].1.clone();
                let block_size = block.block_size as usize;
                let row_count = block.row_count as usize;

                // If memory threshold exceeded, generate a new task and reset accumulators
                if task_bytes + block_size > memory_threshold && selected_blocks.len() > 1 {
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
            break;
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

    pub fn select_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        max_len: usize,
    ) -> Result<Vec<SelectedReclusterSegment>> {
        let mut blocks_num = 0;
        let mut indices = IndexSet::new();
        let mut segments = vec![None; compact_segments.len()];
        let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
        let mut small_segments = IndexSet::new();
        let block_per_seg = self.block_thresholds.block_per_segment;

        // Iterate over all segments
        for (i, (loc, compact_segment)) in compact_segments.iter().enumerate() {
            let segment =
                SelectedReclusterSegment::create(self, loc.clone(), compact_segment.clone());
            let level = segment.stats.level;

            // Skip if segment has more blocks than required and no reclustering is needed
            if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
                continue;
            }

            blocks_num += compact_segment.summary.block_count as usize;
            // Track small segments for special handling later
            if blocks_num < block_per_seg {
                small_segments.insert(i);
            }
            // Add to indices for potential reclustering
            indices.insert(i);
            // Update points_map with min and max points of the segment
            points_map
                .entry(segment.stats.min().clone())
                .and_modify(|v| v.0.push(i))
                .or_insert((vec![i], vec![]));
            points_map
                .entry(segment.stats.max().clone())
                .and_modify(|v| v.1.push(i))
                .or_insert((vec![], vec![i]));
            segments[i] = Some(segment);
        }

        let selected_indices = if indices.len() > 1 && blocks_num > block_per_seg {
            let selected = self.fetch_max_depth(points_map, 1.0, max_len)?;
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
            .filter_map(|i| segments[i].take())
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
        let blocks = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
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
        let blocks = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<HashMap<_, _>>();
        Ok(blocks)
    }

    fn fetch_max_depth(
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
        max_len: usize,
    ) -> Result<IndexSet<usize>> {
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

        let mut selected_idx = IndexSet::with_capacity(max_len);
        if !unfinished_intervals.is_empty() {
            warn!(
                "Recluster: unfinished_intervals is not empty after calculate the blocks overlaps"
            );
            // re-sort the unfinished unfinished_intervals firstly.
            unfinished_intervals.keys().for_each(|idx| {
                selected_idx.insert(*idx);
            });
        }

        let sum_depth: usize = interval_depths.values().sum();
        // round the float to 4 decimal places.
        let average_depth =
            (10000.0 * sum_depth as f64 / interval_depths.len() as f64).round() / 10000.0;
        debug!("recluster: average_depth: {}", average_depth);

        // find the max point, gather the indices.
        if average_depth > depth_threshold {
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
        Ok(selected_idx)
    }

    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    pub fn check_point(start: &[usize], end: &[usize]) -> bool {
        if start.len() + end.len() > 3 || start.is_empty() || end.is_empty() {
            return false;
        }

        let set: HashSet<usize> = HashSet::from_iter(start.iter().chain(end.iter()).cloned());
        set.len() == 2
    }
}
