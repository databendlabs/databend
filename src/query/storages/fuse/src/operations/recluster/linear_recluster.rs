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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Scalar;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use indexmap::IndexMap;
use indexmap::IndexSet;
use indexmap::map::Entry;
use log::debug;

use crate::SegmentLocation;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::passes_depth_gate;
use crate::operations::recluster::task_candidate;
use crate::statistics::RangeMaxTree;
#[cfg(test)]
use crate::statistics::calculate_block_depths;

/// Linear cluster-key recluster behavior.
pub(crate) struct LinearReclusterStrategy;

pub(crate) struct ReclusterDepthStats {
    positions: HashMap<String, usize>,
    // Endpoint order is shared by all candidates; simulated outputs introduce no new keys.
    ranges: Vec<(usize, usize)>,
    point_count: usize,
    depth_sum: i128,
    depth_peaks: Vec<BlockDepthPeak>,
}

/// One task's estimated output run, not the ranges of individual output blocks.
struct ReclusterOutputRun {
    /// Inclusive positions in the shared, sorted cluster-key endpoint table.
    start_point: usize,
    end_point: usize,
    /// Estimated output block count; weights depth mass, not coverage layers.
    block_count: usize,
}

struct BlockDepthPeak {
    depth: usize,
    first: usize,
    last: usize,
}

impl ReclusterDepthStats {
    pub(crate) fn create<'a>(
        blocks: impl Iterator<Item = &'a ReclusterBlock>,
        key_types: &[DataType],
    ) -> Result<Self> {
        let mut positions = HashMap::new();
        let mut ranges = Vec::new();
        for block in blocks {
            let stats = block.stats();
            if stats.min().len() != key_types.len() || stats.max().len() != key_types.len() {
                return Err(ErrorCode::Internal(
                    "Invalid cluster-key arity in recluster window",
                ));
            }
            if positions
                .insert(block.meta.location.0.clone(), ranges.len())
                .is_some()
            {
                return Err(ErrorCode::Internal("Duplicate block in recluster windows"));
            }
            ranges.push((stats.min().as_slice(), stats.max().as_slice()));
        }
        let (ranges, point_count) = index_depth_ranges(&ranges, key_types)?;
        let depth_peaks = block_depth_peaks(&ranges, point_count);
        let depth_sum = depth_peaks.iter().map(|p| p.depth as i128).sum();
        Ok(Self {
            positions,
            ranges,
            point_count,
            depth_sum,
            depth_peaks,
        })
    }

    /// Evaluate a batch against one fixed pre-state; a single task is a one-element batch.
    pub(crate) fn gain<'a, I>(
        &self,
        tasks: impl IntoIterator<Item = I>,
        properties: &ReclusterProperties,
    ) -> Result<i64>
    where
        I: IntoIterator<Item = &'a ReclusterBlock>,
    {
        let mut selected = HashSet::new();
        let mut outputs = Vec::new();
        for blocks in tasks {
            let (mut total_rows, mut total_bytes, mut total_compressed) = (0usize, 0usize, 0usize);
            let (mut start, mut end) = (usize::MAX, 0);
            for block in blocks {
                let idx = *self
                    .positions
                    .get(&block.meta.location.0)
                    .ok_or_else(|| ErrorCode::Internal("Candidate missing from decoded windows"))?;
                if !selected.insert(idx) {
                    return Err(ErrorCode::Internal("Shared input in joint recluster tasks"));
                }
                start = start.min(self.ranges[idx].0);
                end = end.max(self.ranges[idx].1);
                total_rows += block.meta.row_count as usize;
                total_bytes += block.meta.block_size as usize;
                total_compressed += block.meta.file_size as usize;
            }
            let count = estimate_output_block_count(
                total_rows,
                total_bytes,
                total_compressed,
                &properties.block_thresholds,
            )
            .ok_or_else(|| ErrorCode::Internal("Missing output size for recluster candidate"))?;
            outputs.push(ReclusterOutputRun {
                start_point: start,
                end_point: end,
                block_count: count,
            });
        }
        let mut indices = selected.iter().copied();
        if let [output] = outputs.as_slice()
            && let (Some(left), Some(right), None) =
                (indices.next(), indices.next(), indices.next())
            && let Some(gain) = estimate_pair_depth_gain(
                left,
                right,
                &self.ranges,
                &self.depth_peaks,
                self.depth_sum,
                output.block_count,
            )
        {
            return Ok(gain);
        }
        Ok(estimate_depth_gain(
            &selected,
            &outputs,
            &self.ranges,
            self.point_count,
            self.depth_sum,
        ))
    }
}

struct CandidatePlan {
    peak_pos: usize,
    score: CandidateScore,
}

impl ReclusterStrategy for LinearReclusterStrategy {
    fn select_segments(
        &self,
        properties: &ReclusterProperties,
        compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
        window_len: usize,
    ) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
        select_scalar_segments(self, properties, compact_segments, window_len)
    }

    fn fetch_task_candidates(
        &self,
        properties: &ReclusterProperties,
        group: ReclusterGroup,
        indices: &[usize],
        blocks: &[&ReclusterBlock],
        task_budget: usize,
        depth_stats: Option<&super::ReclusterDepthStats>,
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        let mut points_map = BTreeMap::new();
        for (local_idx, &i) in indices.iter().enumerate() {
            // Use a group-local block index (0..block_count) as the point key so
            // dense lookup vectors are sized by the group block count, not the
            // window-global block index range. `indices` maps each local index
            // back to its `blocks` index.
            let stats = blocks[i].stats();
            let (min, max) = (stats.min().as_slice(), stats.max().as_slice());
            if min.len() != properties.scalar_cluster_key_types.len()
                || max.len() != properties.scalar_cluster_key_types.len()
            {
                continue;
            }
            let point: &mut (Vec<usize>, Vec<usize>) =
                points_map.entry(ScalarSlice(min)).or_default();
            point.0.push(local_idx);
            let point = points_map.entry(ScalarSlice(max)).or_default();
            point.1.push(local_idx);
        }
        if points_map.is_empty() {
            return Ok(Vec::new());
        }
        let block_count = indices.len();
        let values = points_map.into_values().collect::<Vec<_>>();

        // PASS 1: sweep sorted points and record folded point depths plus each
        // block's open/close positions.
        let num_points = values.len();
        let mut point_depths = vec![0usize; num_points];
        let unset_pos = usize::MAX;
        let mut open_pos = vec![unset_pos; block_count];
        let mut close_pos = vec![unset_pos; block_count];
        let mut live = vec![false; block_count];
        let mut live_count = 0usize;
        let mut max_depth = 0;
        let mut peaks = Vec::new();
        let mut current_peak: Option<(usize, usize, usize)> = None;
        for (i, (starts, ends)) in values.iter().enumerate() {
            let point_depth = calc_point_depth(live_count, starts, ends);
            point_depths[i] = point_depth;
            if point_depth > max_depth {
                max_depth = point_depth;
            }
            if point_depth as f64 > properties.depth_threshold {
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

        if !passes_depth_gate(properties.depth_threshold, average_depth, max_depth) {
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

        let enable_task_selection_v2 = properties.enable_task_selection_v2;
        let local_depths = if enable_task_selection_v2 && !peaks.is_empty() && depth_stats.is_none()
        {
            Some(ReclusterDepthStats::create(
                blocks.iter().copied(),
                &properties.scalar_cluster_key_types,
            )?)
        } else {
            None
        };
        let depth_stats = depth_stats.or(local_depths.as_ref());
        let estimate_depth_gain = |selected: &[usize]| -> Result<i64> {
            match depth_stats {
                Some(stats) => stats.gain(
                    [selected.iter().map(|&idx| blocks[indices[idx]])],
                    properties,
                ),
                None => Ok(0),
            }
        };

        let push_task = |candidates: &mut Vec<ReclusterTaskCandidate>,
                         used_blocks: &mut [bool],
                         local_indices: Vec<usize>,
                         task_bytes: usize,
                         max_depth: usize| {
            for &local_idx in &local_indices {
                used_blocks[local_idx] = true;
            }
            // Memory cap may have truncated the hotspot's full peak-depth set;
            // the actual overlap depth of what got selected cannot exceed how
            // many blocks made it into the task.
            let max_depth = max_depth.min(local_indices.len());
            let task_indices = local_indices
                .into_iter()
                .map(|local_idx| indices[local_idx])
                .collect::<Vec<_>>();
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                selected_block_count: task_indices.len(),
                max_depth,
                average_depth,
                estimated_depth_gain: 0,
                task_threshold_bytes: properties.memory_threshold,
                // Filled in by `task_candidate`, which groups blocks by segment.
                touched_segment_count: 0,
            };
            candidates.push(task_candidate(group, score, &task_indices, blocks));
        };

        let push_plan = |plans: &mut IndexMap<Vec<usize>, CandidatePlan>,
                         peak_pos: usize,
                         mut local_indices: Vec<usize>,
                         task_bytes: usize,
                         max_depth: usize|
         -> Result<()> {
            local_indices.sort_unstable();
            let Entry::Vacant(entry) = plans.entry(local_indices) else {
                return Ok(());
            };
            let local_indices = entry.key();
            let estimated_depth_gain = estimate_depth_gain(local_indices)?;
            // Memory cap may have truncated the hotspot's full peak-depth set;
            // the actual overlap depth of what got selected cannot exceed how
            // many blocks made it into the task.
            let max_depth = max_depth.min(local_indices.len());
            // v2 ranks plans before they become candidates, so the segment span
            // has to be known here for scoring to see it.
            // Group indices preserve segment-major decode order; the plan key is sorted.
            let touched_segment_count = usize::from(!local_indices.is_empty())
                + local_indices
                    .windows(2)
                    .filter(|pair| {
                        blocks[indices[pair[0]]].index.segment_idx
                            != blocks[indices[pair[1]]].index.segment_idx
                    })
                    .count();
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                selected_block_count: local_indices.len(),
                max_depth,
                average_depth,
                estimated_depth_gain,
                task_threshold_bytes: properties.memory_threshold,
                touched_segment_count,
            };
            entry.insert(CandidatePlan { peak_pos, score });
            Ok(())
        };

        let mut candidates = Vec::new();
        let mut used_blocks = vec![false; block_count];
        let mut plans = IndexMap::new();

        for &(peak_pos, peak_depth, _) in &peaks {
            if !enable_task_selection_v2 && candidates.len() >= task_budget {
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
            let hotspot_blocks = (0..block_count)
                .filter(|&idx| open_pos[idx] <= hotspot_right && close_pos[idx] >= hotspot_left)
                .map(|idx| (idx, blocks[indices[idx]].meta.block_size as usize))
                .collect::<Vec<_>>();
            // Pack hotspot-overlapping blocks first. Tasks split only on memory,
            // so a deep hotspot is not scattered by parallelism balancing.
            // Keep the peak depth from the initial sweep. Blocks are not marked
            // as used until all peak plans have been scored and ranked.
            let mut task_bytes = 0usize;
            let mut task_indices = Vec::new();
            for &(local_idx, block_size) in &hotspot_blocks {
                if !enable_task_selection_v2 && used_blocks[local_idx] {
                    continue;
                }
                let should_split_for_memory = !task_indices.is_empty()
                    && task_bytes.saturating_add(block_size) > properties.memory_threshold;

                if should_split_for_memory {
                    if task_indices.len() >= 2 {
                        let local_indices = std::mem::take(&mut task_indices);
                        if enable_task_selection_v2 {
                            push_plan(&mut plans, peak_pos, local_indices, task_bytes, peak_depth)?;
                        } else {
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
                && (task_bytes < properties.memory_threshold || task_indices.len() < 2)
            {
                // Fill only the last hotspot tail from the deeper adjacent side.
                let mut left = hotspot_left;
                let mut right = hotspot_right;
                'fill_remaining: while task_bytes < properties.memory_threshold
                    || task_indices.len() < 2
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
                    if left_depth.max(right_depth) <= properties.depth_threshold {
                        break;
                    }

                    let (cur, use_ends) = if left_depth >= right_depth {
                        left -= 1;
                        (left, true)
                    } else {
                        right += 1;
                        (right, false)
                    };
                    let group_indices = if use_ends {
                        &values[cur].1
                    } else {
                        &values[cur].0
                    };
                    for &local_idx in group_indices {
                        if !enable_task_selection_v2 && used_blocks[local_idx] {
                            continue;
                        }
                        let idx = indices[local_idx];
                        let block_size = blocks[idx].meta.block_size as usize;
                        if !task_indices.is_empty()
                            && task_bytes.saturating_add(block_size) > properties.memory_threshold
                        {
                            break 'fill_remaining;
                        }
                        task_bytes = task_bytes.saturating_add(block_size);
                        task_indices.push(local_idx);
                    }
                }
            }

            if task_indices.len() >= 2 {
                if enable_task_selection_v2 {
                    push_plan(&mut plans, peak_pos, task_indices, task_bytes, peak_depth)?;
                } else {
                    push_task(
                        &mut candidates,
                        &mut used_blocks,
                        task_indices,
                        task_bytes,
                        peak_depth,
                    );
                }
            }
            if enable_task_selection_v2 {
                // Each block gets a fresh chance to form a task. An oversized next block
                // must not prevent a later, smaller block from joining the current task.
                for (local_indices, task_bytes) in
                    forward_packs(&hotspot_blocks, properties.memory_threshold)
                {
                    push_plan(&mut plans, peak_pos, local_indices, task_bytes, peak_depth)?;
                }
            }
        }

        let mut rejected_conflict_count = 0usize;
        if enable_task_selection_v2 {
            plans.sort_by(|_, left, _, right| {
                right
                    .score
                    .cmp_desc_v2(&left.score)
                    .then_with(|| left.peak_pos.cmp(&right.peak_pos))
            });

            used_blocks.fill(false);
            for (local_indices, plan) in &plans {
                if candidates.len() >= task_budget {
                    break;
                }
                if local_indices.iter().any(|idx| used_blocks[*idx]) {
                    rejected_conflict_count += 1;
                    continue;
                }
                for &local_idx in local_indices {
                    used_blocks[local_idx] = true;
                }
                let task_indices = local_indices
                    .iter()
                    .map(|local_idx| indices[*local_idx])
                    .collect::<Vec<_>>();
                candidates.push(task_candidate(group, plan.score, &task_indices, blocks));
            }
        }

        debug!(
            "recluster: probed task candidates group={} block_count={} avg_depth={} depth_threshold={} max_depth={} peak_count={} task_count={} rejected_conflict_count={}",
            group,
            block_count,
            average_depth,
            properties.depth_threshold,
            max_depth,
            peaks.len(),
            candidates.len(),
            rejected_conflict_count,
        );

        Ok(candidates)
    }
}

// At most one candidate per starting block, with O(n²) size checks but O(n) candidates.
// Blocks may occur in multiple candidates; execution conflicts are resolved after scoring.
fn forward_packs(
    blocks: &[(usize, usize)],
    budget: usize,
) -> impl Iterator<Item = (Vec<usize>, usize)> + '_ {
    let mut suffix_min = vec![usize::MAX; blocks.len() + 1];
    for i in (0..blocks.len()).rev() {
        suffix_min[i] = blocks[i].1.min(suffix_min[i + 1]);
    }
    blocks
        .iter()
        .enumerate()
        .filter_map(move |(start, &(idx, size))| {
            if size > budget {
                return None;
            }
            let mut selected = vec![idx];
            let mut bytes = size;
            for (pos, &(idx, size)) in blocks.iter().enumerate().skip(start + 1) {
                if suffix_min[pos] > budget - bytes {
                    break;
                }
                if size <= budget - bytes {
                    selected.push(idx);
                    bytes += size;
                }
            }
            (selected.len() >= 2).then_some((selected, bytes))
        })
}

// Reuse the executor's target sizing; final compaction may change this count.
fn estimate_output_block_count(
    total_rows: usize,
    total_bytes: usize,
    total_compressed: usize,
    thresholds: &BlockThresholds,
) -> Option<usize> {
    if total_rows == 0 || total_bytes == 0 || total_compressed == 0 {
        return None;
    }
    let (rows_per_block, _) =
        thresholds.calc_rows_for_recluster(total_rows, total_bytes, total_compressed);
    Some(total_rows.div_ceil(rows_per_block))
}

// Sort distinct keys once, using the same key identity and ordering as the scalar sweep.
fn index_depth_ranges<T: AsRef<[Scalar]>>(
    ranges: &[(T, T)],
    key_types: &[DataType],
) -> Result<(Vec<(usize, usize)>, usize)> {
    if ranges.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let mut positions = HashMap::new();
    for (min, max) in ranges {
        positions.entry(min.as_ref()).or_insert(0);
        positions.entry(max.as_ref()).or_insert(0);
    }
    let keys = positions.keys().copied().collect::<Vec<_>>();
    let order = compare_scalars(&keys, key_types)?;
    for (pos, idx) in order.into_iter().enumerate() {
        *positions.get_mut(keys[idx as usize]).unwrap() = pos;
    }
    let ranges = ranges
        .iter()
        .map(|(min, max)| (positions[min.as_ref()], positions[max.as_ref()]))
        .collect::<Vec<_>>();
    if ranges.iter().any(|(start, end)| start > end) {
        return Err(ErrorCode::Internal("Invalid recluster block range"));
    }
    Ok((ranges, positions.len()))
}

// Cache each block's depth and the outermost positions attaining that depth once per
// shared window set. Removing one layer lowers a block's maximum only if ALL its
// maximum positions are covered, not merely one peak.
fn block_depth_peaks(ranges: &[(usize, usize)], point_count: usize) -> Vec<BlockDepthPeak> {
    if ranges.is_empty() {
        return Vec::new();
    }
    let mut changes = vec![0i64; point_count + 1];
    for &(start, end) in ranges {
        changes[start] += 1;
        changes[end + 1] -= 1;
    }
    let mut positions = HashMap::<usize, Vec<usize>>::new();
    let mut depth = 0i64;
    let tree = RangeMaxTree::from_iter(changes[..point_count].iter().enumerate().map(
        |(pos, change)| {
            depth += change;
            positions.entry(depth as usize).or_default().push(pos);
            depth as usize
        },
    ));
    ranges
        .iter()
        .map(|&(start, end)| {
            let depth = tree.range_max(start, end);
            let positions = &positions[&depth];
            BlockDepthPeak {
                depth,
                first: positions[positions.partition_point(|&pos| pos < start)],
                last: positions[positions.partition_point(|&pos| pos <= end) - 1],
            }
        })
        .collect()
}

// Two intersecting inputs replaced by their union remove exactly one layer on
// the intersection. Keep the same output-run peak approximation as the general path.
fn estimate_pair_depth_gain(
    left: usize,
    right: usize,
    ranges: &[(usize, usize)],
    peaks: &[BlockDepthPeak],
    depth_before: i128,
    output_blocks: usize,
) -> Option<i64> {
    let start = ranges[left].0.max(ranges[right].0);
    let end = ranges[left].1.min(ranges[right].1);
    if start > end || output_blocks == 0 {
        return None;
    }
    let covered = |p: &BlockDepthPeak| p.first >= start && p.last <= end;
    let left_peak = &peaks[left];
    let right_peak = &peaks[right];
    let neighbor_depth = depth_before
        - left_peak.depth as i128
        - right_peak.depth as i128
        - peaks.iter().filter(|p| covered(p)).count() as i128
        + i128::from(covered(left_peak))
        + i128::from(covered(right_peak));
    // The union's maxima are precisely the maxima of its input intervals.
    let peak = left_peak.depth.max(right_peak.depth);
    let output_depth = peak
        - usize::from(
            (left_peak.depth < peak || covered(left_peak))
                && (right_peak.depth < peak || covered(right_peak)),
        );
    Some(depth_gain_from_sums(
        depth_before,
        neighbor_depth + output_blocks as i128 * output_depth as i128,
        ranges.len(),
        (ranges.len() - 2) as i128 + output_blocks as i128,
    ))
}

fn depth_gain_from_sums(
    before: i128,
    after: i128,
    blocks_before: usize,
    blocks_after: i128,
) -> i64 {
    // Floor so a small negative estimate cannot truncate to zero.
    let gain = (before * blocks_after - after * blocks_before as i128).div_euclid(blocks_after);
    gain.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

// All output runs coexist in one simulation. Counts weight depth mass, while each
// run contributes one coverage layer, matching the existing single-task model.
fn estimate_depth_gain(
    selected: &HashSet<usize>,
    outputs: &[ReclusterOutputRun],
    ranges: &[(usize, usize)],
    point_count: usize,
    depth_before: i128,
) -> i64 {
    if selected.is_empty() {
        return 0;
    }
    let mut changes = vec![0i64; point_count + 1];
    for (idx, &(start, end)) in ranges.iter().enumerate() {
        if !selected.contains(&idx) {
            changes[start] += 1;
            changes[end + 1] -= 1;
        }
    }
    for output in outputs {
        changes[output.start_point] += 1;
        changes[output.end_point + 1] -= 1;
    }
    let mut depth = 0i64;
    let tree = RangeMaxTree::from_iter(changes[..point_count].iter().map(|change| {
        depth += change;
        depth as usize
    }));
    let neighbor_depth: i128 = ranges
        .iter()
        .enumerate()
        .filter(|(idx, _)| !selected.contains(idx))
        .map(|(_, &(start, end))| tree.range_max(start, end) as i128)
        .sum();
    let depth_after = neighbor_depth
        + outputs
            .iter()
            .map(|output| {
                output.block_count as i128
                    * tree.range_max(output.start_point, output.end_point) as i128
            })
            .sum::<i128>();
    let blocks_after = (ranges.len() - selected.len()) as i128
        + outputs
            .iter()
            .map(|output| output.block_count as i128)
            .sum::<i128>();
    depth_gain_from_sums(depth_before, depth_after, ranges.len(), blocks_after)
}

/// Select scalar segment windows with a sweep over segment min/max points.
pub(crate) fn select_scalar_segments(
    strategy: &dyn ReclusterStrategy,
    properties: &ReclusterProperties,
    compact_segments: &[(SegmentLocation, Arc<CompactSegmentInfo>)],
    window_len: usize,
) -> Result<Vec<Vec<SelectedReclusterSegment>>> {
    let window_len = window_len.max(1);
    let block_per_seg = properties.block_thresholds.block_per_segment;

    let mut total_blocks = 0;
    let mut segments = vec![None; compact_segments.len()];
    let mut segment_stats = Vec::with_capacity(compact_segments.len());

    // Phase 1: collect segment ranges for the sweep-line selection. Large
    // unclustered segments are skipped because rewriting them is not useful.
    for (i, (loc, compact_segment)) in compact_segments.iter().enumerate() {
        let stats = strategy.build_cluster_stats(
            properties,
            compact_segment.summary.cluster_stats.as_ref(),
            &compact_segment.summary.col_stats,
        );
        let level = stats.level;

        if level < 0 && compact_segment.summary.block_count as usize >= block_per_seg {
            continue;
        }

        total_blocks += compact_segment.summary.block_count as usize;
        let (min, max) = (stats.min().as_slice(), stats.max().as_slice());
        if min.len() != properties.scalar_cluster_key_types.len()
            || max.len() != properties.scalar_cluster_key_types.len()
        {
            continue;
        }
        segment_stats.push((i, stats.min, stats.max));
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
    let mut segment_points = BTreeMap::new();
    for (i, min, max) in &segment_stats {
        let point: &mut (Vec<usize>, Vec<usize>) =
            segment_points.entry(ScalarSlice(min)).or_default();
        point.0.push(*i);
        let point = segment_points.entry(ScalarSlice(max)).or_default();
        point.1.push(*i);
    }

    for (_, (start, end)) in segment_points {
        let point_depth = calc_point_depth(unfinished_intervals.len(), &start, &end);

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

    if properties.mode == ReclusterMode::Conservative {
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

#[derive(Clone, Copy)]
struct ScalarSlice<'a>(&'a [Scalar]);

impl Ord for ScalarSlice<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .iter()
            .map(Scalar::as_ref)
            .cmp(other.0.iter().map(Scalar::as_ref))
    }
}

impl PartialOrd for ScalarSlice<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScalarSlice<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ScalarSlice<'_> {}

fn calc_point_depth(open_interval_count: usize, start: &[usize], end: &[usize]) -> usize {
    // block1: [1, 2], block2: [2, 3]. The depth of point '2' is 1.
    if open_interval_count == 1
        && !start.is_empty()
        && !end.is_empty()
        && start.len() + end.len() <= 3
    {
        let first = start[0];
        let mut others = start.iter().chain(end).filter(|&&idx| idx != first);
        if let Some(second) = others.next()
            && others.all(|idx| idx == second)
        {
            return 1;
        }
    }

    open_interval_count + start.len()
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberDataType;

    use super::*;

    #[test]
    fn test_boundary_depth_matches_distinct_block_rule() {
        let lists = [vec![], vec![0], vec![1], vec![0, 0], vec![0, 1], vec![1, 2]];
        for live in 0..=3 {
            for start in &lists {
                for end in &lists {
                    let fold = live == 1
                        && !start.is_empty()
                        && !end.is_empty()
                        && start.len() + end.len() <= 3
                        && start.iter().chain(end).collect::<HashSet<_>>().len() == 2;
                    assert_eq!(
                        calc_point_depth(live, start, end),
                        if fold { 1 } else { live + start.len() }
                    );
                }
            }
        }
    }

    #[test]
    fn test_borrowed_range_index_matches_owned() {
        let types = [DataType::Number(NumberDataType::Int32)];
        let owned = [(0i32, 10i32), (5, 10), (5, 5)]
            .map(|(min, max)| (vec![Scalar::from(min)], vec![Scalar::from(max)]));
        let borrowed = owned
            .each_ref()
            .map(|(min, max)| (min.as_slice(), max.as_slice()));
        assert_eq!(
            index_depth_ranges(&owned, &types).unwrap(),
            index_depth_ranges(&borrowed, &types).unwrap()
        );
    }

    #[test]
    fn test_joint_gain_accounts_for_shared_neighbor() {
        let ranges = [(0, 1), (0, 1), (3, 4), (3, 4), (0, 4)];
        let a = HashSet::from([0, 1]);
        let b = HashSet::from([2, 3]);
        let both = HashSet::from([0, 1, 2, 3]);
        let left = ReclusterOutputRun {
            start_point: 0,
            end_point: 1,
            block_count: 2,
        };
        let right = ReclusterOutputRun {
            start_point: 3,
            end_point: 4,
            block_count: 2,
        };
        assert_eq!(
            estimate_depth_gain(&a, std::slice::from_ref(&left), &ranges, 5, 15),
            2
        );
        assert_eq!(
            estimate_depth_gain(&b, std::slice::from_ref(&right), &ranges, 5, 15),
            2
        );
        // Both sides must change to lower the long neighbor's maximum.
        assert_eq!(
            estimate_depth_gain(&both, &[left, right], &ranges, 5, 15),
            5
        );
        assert_eq!(
            estimate_depth_gain(
                &both,
                &[
                    ReclusterOutputRun {
                        start_point: 3,
                        end_point: 4,
                        block_count: 1
                    },
                    ReclusterOutputRun {
                        start_point: 0,
                        end_point: 1,
                        block_count: 1
                    },
                ],
                &ranges,
                5,
                15
            ),
            5
        );
        assert_eq!(estimate_depth_gain(&HashSet::new(), &[], &[], 0, 0), 0);
    }

    #[test]
    fn test_pair_gain_matches_general_simulation() {
        let choices = [(0, 0), (0, 1), (0, 2), (1, 1), (1, 2), (2, 2)];
        for a in choices {
            for b in choices {
                for neighbor in choices {
                    let ranges = [a, b, neighbor];
                    let peaks = block_depth_peaks(&ranges, 3);
                    let before = peaks.iter().map(|p| p.depth as i128).sum();
                    for output_blocks in [1, 2, 4] {
                        let fast =
                            estimate_pair_depth_gain(0, 1, &ranges, &peaks, before, output_blocks);
                        if a.0.max(b.0) > a.1.min(b.1) {
                            assert!(fast.is_none());
                        } else {
                            let general = estimate_depth_gain(
                                &HashSet::from([0, 1]),
                                &[ReclusterOutputRun {
                                    start_point: a.0.min(b.0),
                                    end_point: a.1.max(b.1),
                                    block_count: output_blocks,
                                }],
                                &ranges,
                                3,
                                before,
                            );
                            assert_eq!(fast, Some(general), "{ranges:?}, outputs={output_blocks}");
                        }
                    }
                }
            }
        }
        assert!(block_depth_peaks(&[], 0).is_empty());
    }

    #[test]
    fn test_forward_packs_suffix_min_preserves_candidates() {
        for a in [0, 50, 100, 201] {
            for b in [0, 50, 100, 201] {
                for c in [0, 50, 100, 201] {
                    let blocks = [(0, a), (1, b), (2, c)];
                    let expected = blocks
                        .iter()
                        .enumerate()
                        .filter_map(|(start, &(idx, size))| {
                            if size > 200 {
                                return None;
                            }
                            let mut selected = vec![idx];
                            let mut bytes = size;
                            for &(idx, size) in &blocks[start + 1..] {
                                if size <= 200 - bytes {
                                    selected.push(idx);
                                    bytes += size;
                                }
                            }
                            (selected.len() >= 2).then_some((selected, bytes))
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(forward_packs(&blocks, 200).collect::<Vec<_>>(), expected);
                }
            }
        }
    }

    #[test]
    fn test_forward_packs_skip_non_fitting_blocks() {
        let blocks = [(0, 103), (1, 163), (2, 103), (3, 55)];
        let packs = forward_packs(&blocks, 256).collect::<Vec<_>>();
        assert_eq!(packs, vec![
            (vec![0, 2], 206),
            (vec![1, 3], 218),
            (vec![2, 3], 158)
        ]);
    }

    #[test]
    fn test_forward_packs_reuse_blocks_across_candidates() {
        let blocks = [(0, 100), (1, 100), (2, 100)];
        assert_eq!(forward_packs(&blocks, 200).collect::<Vec<_>>(), vec![
            (vec![0, 1], 200),
            (vec![1, 2], 200)
        ]);
        assert!(forward_packs(&blocks, 99).next().is_none());
        assert!(forward_packs(&[], 200).next().is_none());
    }

    #[test]
    fn test_shared_depth_stats_include_normalized_blocks_from_other_windows() {
        use databend_storages_common_table_meta::meta::BlockMeta;
        use databend_storages_common_table_meta::meta::ClusterStatistics;
        use databend_storages_common_table_meta::meta::Compression;

        use crate::operations::recluster::ReclusterBlockStats;

        let types = [DataType::Number(NumberDataType::Int32)];
        let make_block = |name: &str, segment_idx| {
            let stats =
                ClusterStatistics::new(0, vec![Scalar::from(0i32)], vec![Scalar::from(10i32)], 0);
            let meta = Arc::new(BlockMeta::new(
                100,
                1000,
                100,
                HashMap::new(),
                HashMap::new(),
                None,
                (name.to_string(), 2),
                None,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Compression::Lz4Raw,
                None,
            ));
            ReclusterBlock {
                index: crate::operations::common::BlockMetaIndex {
                    segment_idx,
                    block_idx: 0,
                },
                meta,
                stats: ReclusterBlockStats::Normalized(stats),
            }
        };
        let blocks = [
            make_block("a", 0),
            make_block("b", 0),
            make_block("neighbor", 1),
        ];
        let shared = ReclusterDepthStats::create(blocks.iter(), &types).unwrap();
        let selected = HashSet::from([shared.positions["a"], shared.positions["b"]]);
        assert_eq!(
            estimate_depth_gain(
                &selected,
                &[ReclusterOutputRun {
                    start_point: 0,
                    end_point: 1,
                    block_count: 2
                }],
                &shared.ranges,
                shared.point_count,
                shared.depth_sum,
            ),
            3
        );
        assert!(ReclusterDepthStats::create([&blocks[0], &blocks[0]].into_iter(), &types).is_err());
        let empty = ReclusterDepthStats::create(std::iter::empty(), &types).unwrap();
        assert!(empty.ranges.is_empty());
        assert_eq!(empty.depth_sum, 0);
    }

    fn gain(ranges: &[(i32, i32)], selected: &[usize]) -> i64 {
        let types = [DataType::Number(NumberDataType::Int32)];
        let ranges = ranges
            .iter()
            .map(|&(min, max)| (vec![Scalar::from(min)], vec![Scalar::from(max)]))
            .collect::<Vec<_>>();
        let before = calculate_block_depths(&ranges, &types).unwrap();
        let depth_sum = before.iter().map(|&depth| depth as i128).sum();
        let selected = selected.iter().copied().collect::<HashSet<_>>();
        let (ranges, point_count) = index_depth_ranges(&ranges, &types).unwrap();
        let outputs = selected
            .iter()
            .map(|&i| ranges[i])
            .reduce(|(lo, hi), (a, b)| (lo.min(a), hi.max(b)))
            .map(|(lo, hi)| ReclusterOutputRun {
                start_point: lo,
                end_point: hi,
                block_count: selected.len(),
            });
        estimate_depth_gain(
            &selected,
            outputs.as_slice(),
            &ranges,
            point_count,
            depth_sum,
        )
    }

    #[test]
    fn test_gain_accounts_for_output_block_count_and_neighbors() {
        assert_eq!(gain(&[(0, 10); 10], &[0, 1, 2, 3]), 30);
        assert_eq!(gain(&[(0, 10); 3], &[0, 1]), 3);
        assert_eq!(gain(&[(0, 10); 3], &[0, 1, 2]), 6);
    }

    #[test]
    fn test_gain_does_not_grow_with_key_span() {
        assert_eq!(gain(&[(0, 1); 3], &[0, 1, 2]), 6);
        assert_eq!(gain(&[(0, 100); 3], &[0, 1, 2]), 6);
    }

    #[test]
    fn test_gain_preserves_other_peaks_and_ignores_distant_blocks() {
        assert_eq!(
            gain(&[(0, 10), (0, 10), (20, 30), (20, 30), (0, 30)], &[0, 1]),
            2
        );
        assert_eq!(gain(&[(0, 10), (0, 10), (0, 10), (20, 30)], &[0, 1]), 3);
    }

    #[test]
    fn test_negative_gain_is_preserved_after_accounting_for_output() {
        assert!(gain(&[(0, 0), (4, 4), (2, 2)], &[0, 1]) < 0);
        assert_eq!(gain(&[(0, 1), (4, 5)], &[0, 1]), 0);
    }

    #[test]
    fn test_merging_disjoint_blocks_can_increase_neighbor_depth() {
        let ranges = [(0, 1), (3, 4), (2, 2)];
        assert_eq!(
            estimate_depth_gain(
                &HashSet::from([0, 1]),
                &[ReclusterOutputRun {
                    start_point: 0,
                    end_point: 4,
                    block_count: 1
                }],
                &ranges,
                5,
                3
            ),
            -3
        );
    }

    #[test]
    fn test_gain_shared_endpoints() {
        assert_eq!(gain(&[(0, 1), (1, 2), (1, 1)], &[0, 1]), 3);
    }

    #[test]
    fn test_gain_empty_and_single_block_simulations() {
        assert_eq!(gain(&[(0, 10); 3], &[]), 0);
        assert_eq!(gain(&[(0, 10); 3], &[0]), 0);
        assert_eq!(gain(&[(0, 10); 3], &[0, 0]), 0);
    }

    #[test]
    fn test_output_count_does_not_turn_block_removal_into_depth_gain() {
        let ranges = [(0, 1), (4, 5)];
        for output_blocks in [1, 4] {
            assert_eq!(
                estimate_depth_gain(
                    &HashSet::from([0, 1]),
                    &[ReclusterOutputRun {
                        start_point: 0,
                        end_point: 5,
                        block_count: output_blocks
                    }],
                    &ranges,
                    6,
                    2
                ),
                0
            );
        }
    }

    #[test]
    fn test_output_count_uses_average_depth_denominator() {
        let ranges = [(0, 1), (0, 1), (2, 3)];
        // Before: 5/3. After: 1 for any output count, so gain in input-block units is 2.
        for output_blocks in [1, 2, 4] {
            assert_eq!(
                estimate_depth_gain(
                    &HashSet::from([0, 1]),
                    &[ReclusterOutputRun {
                        start_point: 0,
                        end_point: 1,
                        block_count: output_blocks
                    }],
                    &ranges,
                    4,
                    5
                ),
                2
            );
        }
    }

    #[test]
    fn test_executor_sizing_drives_output_count() {
        use databend_common_expression::BlockThresholds;
        let thresholds = BlockThresholds::new(1000, 1_000_000, 200_000, 4);
        let (rows_per_block, _) = thresholds.calc_rows_for_recluster(10_000, 2_000_000, 100_000);
        assert_eq!(10_000usize.div_ceil(rows_per_block), 12);
        let (rows_per_block, _) = thresholds.calc_rows_for_recluster(10_000, 30_000_000, 1_000_000);
        assert_eq!(10_000usize.div_ceil(rows_per_block), 15);
    }

    #[test]
    fn test_gain_nullable_composite_keys() {
        let types = [
            DataType::String.wrap_nullable(),
            DataType::Number(NumberDataType::Int32),
        ];
        let range = (vec![Scalar::Null, Scalar::from(0i32)], vec![
            Scalar::Null,
            Scalar::from(10i32),
        ]);
        let ranges = vec![range; 3];
        let before = calculate_block_depths(&ranges, &types).unwrap();
        let depth_sum = before.iter().map(|&depth| depth as i128).sum();
        let (ranges, point_count) = index_depth_ranges(&ranges, &types).unwrap();
        assert_eq!(
            estimate_depth_gain(
                &HashSet::from([0, 1]),
                &[ReclusterOutputRun {
                    start_point: 0,
                    end_point: 1,
                    block_count: 2
                }],
                &ranges,
                point_count,
                depth_sum
            ),
            3
        );
    }
}
