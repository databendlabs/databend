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

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::ColumnNodes;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use fastrace::func_path;
use fastrace::future::FutureExt;
use fastrace::Span;
use indexmap::IndexSet;
use log::debug;
use log::warn;

use crate::operations::mutation::SegmentCompactChecker;
use crate::operations::BlockCompactMutator;
use crate::operations::CompactLazyPartInfo;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;
use crate::FuseTable;
use crate::SegmentLocation;
use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;

pub enum ReclusterMode {
    Recluster,
    Compact,
}

#[derive(Clone)]
pub struct ReclusterMutator {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) depth_threshold: f64,
    pub(crate) block_thresholds: BlockThresholds,
    pub(crate) cluster_key_id: u32,
    pub(crate) schema: TableSchemaRef,
    pub(crate) max_tasks: usize,
    pub(crate) block_per_seg: usize,
    pub(crate) cluster_key_types: Vec<DataType>,
    pub(crate) column_ids: HashSet<u32>,
}

impl ReclusterMutator {
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
    ) -> Result<Self> {
        let schema = table.schema_with_stream();
        let cluster_key_id = table.cluster_key_meta.clone().unwrap().0;
        let block_thresholds = table.get_block_thresholds();
        let block_per_seg =
            table.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

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

        let cluster_key_types = table.cluster_key_types(ctx.clone());

        // NOTE: The snapshot schema does not contain the stream column.
        let column_ids = snapshot.schema.to_leaf_column_id_set();

        Ok(Self {
            ctx,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            block_per_seg,
            cluster_key_types,
            column_ids,
        })
    }

    /// Used for tests.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        cluster_key_types: Vec<DataType>,
        depth_threshold: f64,
        block_thresholds: BlockThresholds,
        cluster_key_id: u32,
        max_tasks: usize,
        block_per_seg: usize,
        column_ids: HashSet<u32>,
    ) -> Self {
        Self {
            ctx,
            schema,
            depth_threshold,
            block_thresholds,
            cluster_key_id,
            max_tasks,
            block_per_seg,
            cluster_key_types,
            column_ids,
        }
    }

    #[async_backtrace::framed]
    pub async fn target_select(
        &self,
        compact_segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
        mode: ReclusterMode,
    ) -> Result<(u64, ReclusterParts)> {
        match mode {
            ReclusterMode::Compact => self.generate_compact_tasks(compact_segments).await,
            ReclusterMode::Recluster => self.generate_recluster_tasks(compact_segments).await,
        }
    }

    #[async_backtrace::framed]
    pub async fn generate_recluster_tasks(
        &self,
        compact_segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    ) -> Result<(u64, ReclusterParts)> {
        let mut compact_segments = compact_segments;
        compact_segments.sort_by(|a, b| {
            sort_by_cluster_stats(
                &a.1.summary.cluster_stats,
                &b.1.summary.cluster_stats,
                self.cluster_key_id,
            )
        });

        let mut selected_segments = Vec::with_capacity(compact_segments.len());
        let mut selected_segs_idx = Vec::with_capacity(compact_segments.len());
        let mut selected_statistics = Vec::with_capacity(compact_segments.len());
        compact_segments.into_iter().for_each(|(loc, info)| {
            selected_statistics.push(info.summary.clone());
            selected_segments.push(info);
            selected_segs_idx.push(loc.segment_idx);
        });

        let blocks = self.gather_blocks(selected_segments).await?;
        let mut blocks_map: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for (idx, block) in blocks.iter().enumerate() {
            if let Some(stats) = &block.cluster_stats {
                if stats.cluster_key_id == self.cluster_key_id {
                    blocks_map.entry(stats.level).or_default().push(idx);
                }
            }
        }

        let mem_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;
        let recluster_block_size = self.ctx.get_settings().get_recluster_block_size()? as usize;
        let memory_threshold = recluster_block_size.min(mem_info.avail as usize * 1024 * 35 / 100);

        let max_blocks_num = std::cmp::max(
            memory_threshold / self.block_thresholds.max_bytes_per_block,
            // specify a rather small value, so that setting `recluster_block_size`
            // might be tuned to lower value.
            2,
        ) * self.max_tasks;

        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        let mut tasks = Vec::new();
        let mut selected_blocks_idx = IndexSet::new();
        for (level, indices) in blocks_map.into_iter() {
            if level == -1 || indices.len() < 2 {
                continue;
            }

            let mut total_rows = 0;
            let mut total_bytes = 0;
            let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
            for i in indices.iter() {
                if let Some(stats) = &blocks[*i].cluster_stats {
                    points_map
                        .entry(stats.min().clone())
                        .and_modify(|v| v.0.push(*i))
                        .or_insert((vec![*i], vec![]));
                    points_map
                        .entry(stats.max().clone())
                        .and_modify(|v| v.1.push(*i))
                        .or_insert((vec![], vec![*i]));
                }
                total_rows += blocks[*i].row_count;
                total_bytes += blocks[*i].block_size;
            }

            // If the statistics of blocks are too small, just merge them into one block.
            if self
                .block_thresholds
                .check_for_recluster(total_rows as usize, total_bytes as usize)
            {
                debug!(
                    "recluster: the statistics of blocks are too small, just merge them into one block"
                );
                selected_blocks_idx.extend(indices.iter());
                let block_metas = indices
                    .into_iter()
                    .map(|i| (None, blocks[i].clone()))
                    .collect::<Vec<_>>();
                tasks.push(self.generate_task(
                    &block_metas,
                    &column_nodes,
                    total_rows as usize,
                    total_bytes as usize,
                    level,
                ));
                break;
            }

            let selected_idx =
                self.fetch_max_depth(points_map, self.depth_threshold, max_blocks_num)?;
            if selected_idx.is_empty() {
                continue;
            }

            let mut task_bytes = 0;
            let mut task_rows = 0;
            let mut task_indices = Vec::new();
            let mut selected_blocks = Vec::new();
            for idx in selected_idx {
                let block_meta = blocks[idx].clone();
                let block_size = block_meta.block_size as usize;
                let row_count = block_meta.row_count as usize;
                if task_bytes + block_size > memory_threshold && selected_blocks.len() > 1 {
                    selected_blocks_idx.extend(task_indices.iter());
                    task_indices.clear();

                    tasks.push(self.generate_task(
                        &selected_blocks,
                        &column_nodes,
                        task_rows,
                        task_bytes,
                        level,
                    ));

                    task_rows = 0;
                    task_bytes = 0;
                    selected_blocks.clear();

                    if tasks.len() >= self.max_tasks {
                        break;
                    }
                }

                task_rows += row_count;
                task_bytes += block_size;
                task_indices.push(idx);
                selected_blocks.push((None, block_meta));
            }

            // the remains.
            if selected_blocks.len() > 1 {
                selected_blocks_idx.extend(task_indices);
                tasks.push(self.generate_task(
                    &selected_blocks,
                    &column_nodes,
                    task_rows,
                    task_bytes,
                    level,
                ));
            }
            break;
        }

        let selected = if selected_blocks_idx.is_empty() {
            let unordered = || {
                blocks.windows(2).any(|w| {
                    sort_by_cluster_stats(
                        &w[0].cluster_stats,
                        &w[1].cluster_stats,
                        self.cluster_key_id,
                    ) == Ordering::Greater
                })
            };
            (selected_segs_idx.len() > 1 && blocks.len() <= self.block_per_seg) || unordered()
        } else {
            true
        };

        let parts = if selected {
            selected_segs_idx.sort_by(|a, b| b.cmp(a));

            let default_cluster_key_id = Some(self.cluster_key_id);
            let mut removed_segment_summary = Statistics::default();
            selected_statistics.iter().for_each(|v| {
                merge_statistics_mut(&mut removed_segment_summary, v, default_cluster_key_id)
            });

            let blocks_idx: IndexSet<usize> = IndexSet::from_iter(0..blocks.len());
            let diff = blocks_idx.difference(&selected_blocks_idx);
            let remained_blocks = diff
                .into_iter()
                .map(|v| blocks[*v].clone())
                .collect::<Vec<_>>();

            ReclusterParts::Recluster {
                tasks,
                remained_blocks,
                removed_segment_indexes: selected_segs_idx,
                removed_segment_summary,
            }
        } else {
            ReclusterParts::new_recluster_parts()
        };
        Ok((selected_blocks_idx.len() as u64, parts))
    }

    async fn generate_compact_tasks(
        &self,
        compact_segments: Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>,
    ) -> Result<(u64, ReclusterParts)> {
        debug!("recluster: generate compact tasks");
        let settings = self.ctx.get_settings();
        let num_block_limit = settings.get_compact_max_block_selection()? as usize;
        let num_segment_limit = compact_segments.len();
        let mut recluster_blocks_count = 0;

        let mut parts = Vec::new();
        let mut checker =
            SegmentCompactChecker::new(self.block_per_seg as u64, Some(self.cluster_key_id));

        for (loc, compact_segment) in compact_segments.into_iter() {
            recluster_blocks_count += compact_segment.summary.block_count;
            let segments_vec = checker.add(loc.segment_idx, compact_segment);
            for segments in segments_vec {
                checker.generate_part(segments, &mut parts);
            }

            if checker.is_limit_reached(num_segment_limit, num_block_limit) {
                break;
            }
        }
        // finalize the compaction.
        checker.finalize(&mut parts);

        let cluster = self.ctx.get_cluster();
        let max_threads = settings.get_max_threads()? as usize;
        let enable_distributed_compact = settings.get_enable_distributed_compact()?;
        let partitions = if !enable_distributed_compact
            || cluster.is_empty()
            || parts.len() < cluster.nodes.len() * max_threads
        {
            let lazy_parts = parts
                .into_iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();
            Partitions::create(
                PartitionsShuffleKind::Mod,
                BlockCompactMutator::build_compact_tasks(
                    self.ctx.clone(),
                    self.column_ids.clone(),
                    Some(self.cluster_key_id),
                    self.block_thresholds,
                    lazy_parts,
                )
                .await?,
            )
        } else {
            Partitions::create(PartitionsShuffleKind::Mod, parts)
        };

        Ok((recluster_blocks_count, ReclusterParts::Compact(partitions)))
    }

    fn generate_task(
        &self,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        total_rows: usize,
        total_bytes: usize,
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
            level,
        }
    }

    pub fn select_segments(
        &self,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        max_len: usize,
    ) -> Result<(ReclusterMode, IndexSet<usize>)> {
        let mut blocks_num = 0;
        let mut indices = IndexSet::new();
        let mut points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();
        let mut unclustered_sg = IndexSet::new();
        for (i, (loc, compact_segment)) in compact_segments.iter().enumerate() {
            let mut level = -1;
            let clustered = compact_segment
                .summary
                .cluster_stats
                .as_ref()
                .is_some_and(|v| {
                    level = v.level;
                    v.cluster_key_id == self.cluster_key_id
                });
            if !clustered {
                debug!(
                    "recluster: segment '{}' is unclustered, need to be compacted",
                    loc.location.0
                );
                unclustered_sg.insert(i);
                continue;
            }

            if level < 0 && (compact_segment.summary.block_count as usize) >= self.block_per_seg {
                continue;
            }

            if let Some(stats) = &compact_segment.summary.cluster_stats {
                blocks_num += compact_segment.summary.block_count as usize;
                indices.insert(i);
                points_map
                    .entry(stats.min().clone())
                    .and_modify(|v| v.0.push(i))
                    .or_insert((vec![i], vec![]));
                points_map
                    .entry(stats.max().clone())
                    .and_modify(|v| v.1.push(i))
                    .or_insert((vec![], vec![i]));
            }
        }

        if !unclustered_sg.is_empty() {
            return Ok((ReclusterMode::Compact, unclustered_sg));
        }

        let res = if indices.len() > 1 && blocks_num > self.block_per_seg {
            self.fetch_max_depth(points_map, 1.0, max_len)?
        } else {
            indices
        };

        Ok((ReclusterMode::Recluster, res))
    }

    pub fn segment_can_recluster(&self, summary: &Statistics) -> bool {
        if let Some(stats) = &summary.cluster_stats {
            stats.cluster_key_id == self.cluster_key_id
                && (stats.level >= 0 || (summary.block_count as usize) < self.block_per_seg)
        } else {
            false
        }
    }

    #[async_backtrace::framed]
    async fn gather_blocks(
        &self,
        compact_segments: Vec<Arc<CompactSegmentInfo>>,
    ) -> Result<Vec<Arc<BlockMeta>>> {
        // combine all the tasks.
        let mut iter = compact_segments.into_iter();
        let tasks = std::iter::from_fn(|| {
            iter.next().map(|v| {
                async move {
                    v.block_metas()
                        .map_err(|_| ErrorCode::Internal("Failed to get block metas"))
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

    fn fetch_max_depth(
        &self,
        points_map: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)>,
        depth_threshold: f64,
        max_len: usize,
    ) -> Result<IndexSet<usize>> {
        let mut max_depth = 0;
        let mut max_point = 0;
        let mut interval_depths = HashMap::new();
        let mut point_overlaps: Vec<Vec<usize>> = Vec::new();
        let mut unfinished_intervals = HashMap::new();
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

        let mut selected_idx = IndexSet::new();
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
