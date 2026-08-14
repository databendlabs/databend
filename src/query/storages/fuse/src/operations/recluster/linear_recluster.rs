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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::compare_scalars;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use indexmap::IndexSet;
use log::debug;

use crate::SegmentLocation;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterBlock;
use crate::operations::recluster::ReclusterGroup;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterProperties;
use crate::operations::recluster::ReclusterStrategy;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::operations::recluster::ReclusterTaskKind;
use crate::operations::recluster::SelectedReclusterSegment;
use crate::operations::recluster::passes_depth_gate;
use crate::operations::recluster::task_candidate;
use crate::statistics::RangeMaxTree;

/// Linear cluster-key recluster behavior.
pub(crate) struct LinearReclusterStrategy;

// Bound the expensive all-peak probing stage. Peaks are already sorted by
// depth, width, and position, while only `task_budget` candidates can be used in
// one round. Probe at least 32 peaks to keep enough choice when only one task
// slot is available, then grow the search space proportionally to the task
// budget. This keeps fragmented windows from rescanning all blocks for thousands
// of disjoint peaks before most plans are discarded.
const MIN_LINEAR_PEAK_PROBE_COUNT: usize = 32;
const LINEAR_PEAK_PROBE_FACTOR: usize = 4;

struct CandidatePlan {
    peak_pos: usize,
    local_indices: Vec<usize>,
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
    ) -> Result<Vec<ReclusterTaskCandidate>> {
        let mut points_map = HashMap::new();
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
            let point: &mut (Vec<usize>, Vec<usize>) = points_map.entry(min.to_vec()).or_default();
            point.0.push(local_idx);
            let point = points_map.entry(max.to_vec()).or_default();
            point.1.push(local_idx);
        }
        if points_map.is_empty() {
            return Ok(Vec::new());
        }
        let block_count = indices.len();
        let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
        let order = compare_scalars(&keys, &properties.scalar_cluster_key_types)?;

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

        let push_plan = |plans: &mut Vec<CandidatePlan>,
                         peak_pos: usize,
                         local_indices: Vec<usize>,
                         task_bytes: usize,
                         max_depth: usize| {
            let estimated_depth_gain =
                estimate_selected_depth_gain(&local_indices, &open_pos, &close_pos, &point_depths);
            // Memory cap may have truncated the hotspot's full peak-depth set;
            // the actual overlap depth of what got selected cannot exceed how
            // many blocks made it into the task.
            let max_depth = max_depth.min(local_indices.len());
            let score = CandidateScore {
                selected_total_bytes: task_bytes,
                selected_block_count: local_indices.len(),
                max_depth,
                average_depth,
                estimated_depth_gain,
            };
            plans.push(CandidatePlan {
                peak_pos,
                local_indices,
                score,
            });
        };

        let mut plans = Vec::new();

        let peak_probe_count = peaks.len().min(
            task_budget
                .saturating_mul(LINEAR_PEAK_PROBE_FACTOR)
                .max(MIN_LINEAR_PEAK_PROBE_COUNT),
        );
        for &(peak_pos, peak_depth, _) in peaks.iter().take(peak_probe_count) {
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
            // Keep the peak depth from the initial sweep. Blocks are not marked
            // as used until all peak plans have been scored and ranked.
            let mut task_bytes = 0usize;
            let mut task_indices = Vec::new();
            for local_idx in 0..block_count {
                if open_pos[local_idx] > hotspot_right || close_pos[local_idx] < hotspot_left {
                    continue;
                }
                let idx = indices[local_idx];
                let block_size = blocks[idx].meta.block_size as usize;
                let should_split_for_memory = !task_indices.is_empty()
                    && task_bytes.saturating_add(block_size) > properties.memory_threshold;

                if should_split_for_memory {
                    if task_indices.len() >= 2 {
                        let local_indices = std::mem::take(&mut task_indices);
                        push_plan(&mut plans, peak_pos, local_indices, task_bytes, peak_depth);
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
                push_plan(&mut plans, peak_pos, task_indices, task_bytes, peak_depth);
            }
        }

        plans.sort_by(|left, right| {
            right
                .score
                .cmp_desc(&left.score)
                .then_with(|| left.peak_pos.cmp(&right.peak_pos))
        });

        let mut candidates = Vec::new();
        let mut used_blocks = vec![false; block_count];
        let mut rejected_conflict_count = 0usize;
        for plan in &plans {
            if candidates.len() >= task_budget {
                break;
            }
            if plan.local_indices.iter().any(|idx| used_blocks[*idx]) {
                rejected_conflict_count += 1;
                continue;
            }
            for &local_idx in &plan.local_indices {
                used_blocks[local_idx] = true;
            }
            let task_indices = plan
                .local_indices
                .iter()
                .map(|local_idx| indices[*local_idx])
                .collect::<Vec<_>>();
            candidates.push(task_candidate(
                group,
                plan.score,
                &task_indices,
                blocks,
                ReclusterTaskKind::Recluster,
            ));
        }

        debug!(
            "recluster: probed task candidates group={} block_count={} avg_depth={} depth_threshold={} max_depth={} peak_count={} peak_probe_count={} task_count={} rejected_conflict_count={}",
            group,
            block_count,
            average_depth,
            properties.depth_threshold,
            max_depth,
            peaks.len(),
            peak_probe_count,
            candidates.len(),
            rejected_conflict_count,
        );

        Ok(candidates)
    }
}

fn estimate_selected_depth_gain(
    local_indices: &[usize],
    open_pos: &[usize],
    close_pos: &[usize],
    point_depths: &[usize],
) -> u64 {
    if local_indices.len() < 2 {
        return 0;
    }

    // Estimate how much overlap-depth area this rewrite can remove.
    //
    // At each folded cluster-key point, `point_depths[pos]` is the overlap depth
    // of the whole candidate window, while `selected_depth` is the overlap depth
    // contributed by the blocks selected for this task. Rewriting N selected
    // blocks that overlap at the same point can reduce that point's depth by at
    // most N - 1, because one rewritten output range may still cover it. Summing
    // `selected_depth - 1` over all points gives an area-like benefit estimate:
    // a task that fixes a wide moderate overlap can outrank a very deep but tiny
    // peak if it removes more depth per byte rewritten.
    let mut events = Vec::with_capacity(local_indices.len() * 2);
    for &local_idx in local_indices {
        events.push((open_pos[local_idx], 1i32));
        events.push((close_pos[local_idx].saturating_add(1), -1i32));
    }
    events.sort_unstable_by_key(|(pos, _)| *pos);

    let mut gain = 0u64;
    let mut selected_depth = 0i32;
    let mut event_idx = 0usize;
    for (pos, point_depth) in point_depths.iter().enumerate() {
        while event_idx < events.len() && events[event_idx].0 == pos {
            selected_depth += events[event_idx].1;
            event_idx += 1;
        }
        if selected_depth > 1 && *point_depth > 1 {
            gain += (selected_depth.min(*point_depth as i32) - 1) as u64;
        }
    }
    gain
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
    let mut segment_points: HashMap<Vec<Scalar>, (Vec<usize>, Vec<usize>)> = HashMap::new();

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
        segment_points
            .entry(min.to_vec())
            .and_modify(|v| v.0.push(i))
            .or_insert((vec![i], vec![]));
        segment_points
            .entry(max.to_vec())
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
    let sorted_indices = compare_scalars(&keys, &properties.scalar_cluster_key_types)?;

    for idx in sorted_indices {
        let start = &values[idx as usize].0;
        let end = &values[idx as usize].1;
        let point_depth = calc_point_depth(unfinished_intervals.len(), start, end);

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
