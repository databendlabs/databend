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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::MAX_SEGMENT_LOCATIONS_PER_CLAIM;
use databend_common_metrics::storage::metrics_inc_recluster_build_task_milliseconds;
use databend_common_metrics::storage::metrics_inc_recluster_segment_nums_scheduled;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::FuseTable;
use crate::SegmentLocation;
use crate::operations::recluster::CandidateScore;
use crate::operations::recluster::ReclusterCandidateWindow;
use crate::operations::recluster::ReclusterFinalCarry;
use crate::operations::recluster::ReclusterMode;
use crate::operations::recluster::ReclusterMutator;
use crate::operations::recluster::ReclusterTaskCandidate;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;

const DEFAULT_RECLUSTER_SEGMENT_LIMIT: usize = 1024;
const DEFAULT_MIN_RECLUSTER_SEGMENT_WINDOW: usize = 32;

type RankedTaskCandidate = (usize, usize, CandidateScore, bool);

fn sort_task_candidates(tasks: &mut [RankedTaskCandidate]) {
    tasks.sort_by(|left, right| right.3.cmp(&left.3).then_with(|| right.2.cmp_desc(&left.2)));
}

fn select_task_candidates(
    pending_windows: &[ReclusterCandidateWindow],
    sorted_tasks: &[RankedTaskCandidate],
    max_tasks: usize,
) -> Vec<Vec<usize>> {
    let mut selected_task_indices = vec![Vec::new(); pending_windows.len()];
    let mut selected_segment_locations = HashSet::new();
    let mut selected_count = 0;
    let mut selected_repack_only = false;
    let mut task_segment_locations = HashSet::new();

    for &(window_idx, task_idx, _, _) in sorted_tasks {
        if selected_count >= max_tasks {
            break;
        }
        let window = &pending_windows[window_idx];
        let task = &window.tasks[task_idx];
        // Repack-only candidates rewrite no blocks, but each one consumes a
        // whole window. Keep one per round so max_tasks does not repack
        // multiple disjoint windows at once.
        if task.is_repack_only() && selected_repack_only {
            continue;
        }

        task_segment_locations.clear();
        task_segment_locations.extend(window.task_segment_locations(task_idx));
        if !fits_segment_claim(&selected_segment_locations, &task_segment_locations) {
            debug!(
                "recluster: skip task candidate window_idx={} task_idx={} task_segments={} selected_segments={} max_claim_segments={}",
                window_idx,
                task_idx,
                task_segment_locations.len(),
                selected_segment_locations.len(),
                MAX_SEGMENT_LOCATIONS_PER_CLAIM,
            );
            continue;
        }

        if task.is_repack_only() {
            selected_repack_only = true;
        }
        selected_segment_locations.extend(task_segment_locations.iter().copied());
        selected_task_indices[window_idx].push(task_idx);
        selected_count += 1;
    }

    selected_task_indices
}

// Both selectors enforce the same union-of-segments claim limit.
fn fits_segment_claim(selected: &HashSet<&str>, additional: &HashSet<&str>) -> bool {
    selected.union(additional).count() <= MAX_SEGMENT_LOCATIONS_PER_CLAIM
}

// Bounded joint search: keep at most K batches at each size, and choose the
// best across all sizes. This is approximate (including K=2), but does not
// force full batches or stop at a non-improving intermediate layer.
// With C candidates, score calls are bounded by C + (K-1)*K*C.
fn select_joint_task_candidates(
    windows: &[ReclusterCandidateWindow],
    tasks: &[RankedTaskCandidate],
    max_tasks: usize,
    stats: &super::ReclusterDepthStats,
    properties: &super::ReclusterProperties,
) -> Result<Vec<Vec<usize>>> {
    let limit = max_tasks.min(tasks.len());
    let mut selected = vec![Vec::new(); windows.len()];
    if limit == 0 {
        return Ok(selected);
    }
    let inputs = |i: usize| {
        let (window, task, _, _) = tasks[i];
        windows[window].task_blocks(task)
    };
    // Sort higher gain first, then lower bytes, then canonical task indices.
    let compare = |gain: i64, bytes: usize, batch: &[usize], other: &(i64, usize, Vec<usize>)| {
        other
            .0
            .cmp(&gain)
            .then_with(|| bytes.cmp(&other.1))
            .then_with(|| batch.cmp(&other.2))
    };
    let mut best = None::<(i64, usize, Vec<usize>)>;
    let mut frontier = vec![(0, 0, Vec::new())];
    let mut next = Vec::<(i64, usize, Vec<usize>)>::with_capacity(limit);
    let mut extended = Vec::with_capacity(limit);
    let mut seen = HashSet::new();
    let mut segments = HashSet::new();
    let mut blocks = HashSet::new();
    for _ in 0..limit {
        next.clear();
        seen.clear();
        for (_, _, batch) in &frontier {
            for candidate in 0..tasks.len() {
                let Err(position) = batch.binary_search(&candidate) else {
                    continue;
                };
                extended.clone_from(batch);
                extended.insert(position, candidate);
                if seen.contains(&extended) {
                    continue;
                }
                seen.insert(extended.clone());
                segments.clear();
                blocks.clear();
                let mut repack = false;
                let mut bytes = 0;
                let legal = extended.iter().all(|&i| {
                    let (w, t, score, _) = tasks[i];
                    let task = &windows[w].tasks[t];
                    if task.is_repack_only() && repack {
                        return false;
                    }
                    repack |= task.is_repack_only();
                    bytes += score.selected_total_bytes;
                    segments.extend(windows[w].task_segment_locations(t));
                    segments.len() <= MAX_SEGMENT_LOCATIONS_PER_CLAIM
                        && inputs(i).all(|block| blocks.insert(block.meta.location.0.as_str()))
                });
                if !legal {
                    continue;
                }
                let gain = stats.gain(extended.iter().copied().map(inputs), properties)?;
                if best
                    .as_ref()
                    .is_none_or(|b| compare(gain, bytes, &extended, b).is_lt())
                {
                    let best = best.get_or_insert_with(|| (gain, bytes, Vec::new()));
                    best.0 = gain;
                    best.1 = bytes;
                    best.2.clone_from(&extended);
                }
                let position = next.partition_point(|b| compare(gain, bytes, &extended, b).is_gt());
                if position < limit {
                    // Reuse an evicted batch's capacity; only retained states own copies.
                    let mut retained = if next.len() == limit {
                        next.pop().unwrap().2
                    } else {
                        Vec::new()
                    };
                    retained.clone_from(&extended);
                    next.insert(position, (gain, bytes, retained));
                }
            }
        }
        if next.is_empty() {
            break;
        }
        std::mem::swap(&mut frontier, &mut next);
    }
    if let Some((_, _, batch)) = best {
        for i in batch {
            let (window, task, _, _) = tasks[i];
            selected[window].push(task);
        }
    }
    Ok(selected)
}

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
        mode: ReclusterMode,
        carry: &mut ReclusterFinalCarry,
        claimed_segments: &HashSet<String>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let start = Instant::now();

        ctx.set_status_info("[FUSE-RECLUSTER] Starting recluster operation");

        if self.cluster_key_id().is_none() {
            return Ok(None);
        }

        // `LIMIT 0` requests no work.
        if limit == Some(0) {
            return Ok(None);
        }

        let Some(snapshot) = self.read_table_snapshot().await? else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let mutator = Arc::new(ReclusterMutator::try_create(
            self,
            ctx.clone(),
            snapshot.as_ref(),
            mode,
        )?);

        // Carry is tied to the current cluster key because cached block metas
        // may be normalized during candidate probing.
        let carry_has_state = !carry.pending.is_empty() || carry.scan_cursor != 0;
        if carry_has_state
            && carry.cluster_key_id != mutator.properties.cluster_key_info.cluster_key_id()
        {
            debug!(
                "recluster: reset carry reason=cluster_key_changed old_cluster_key_id={} new_cluster_key_id={}",
                carry.cluster_key_id,
                mutator.properties.cluster_key_info.cluster_key_id(),
            );
            carry.scan_cursor = 0;
            carry.pending.clear();
        }
        carry.cluster_key_id = mutator.properties.cluster_key_info.cluster_key_id();

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let segment_limit = limit.unwrap_or(DEFAULT_RECLUSTER_SEGMENT_LIMIT);
        // LIMIT is applied to scan/window sizing, not task selection. It is a
        // soft scan bound because recluster still keeps a minimum candidate window.
        let chunk_size = segment_limit.max(max_threads * 4);
        // LIMIT also caps one candidate window; select_segments treats this as
        // a soft upper bound when hotspot blocks are inseparable.
        let max_seg_num =
            segment_limit.min((max_threads * 2).max(DEFAULT_MIN_RECLUSTER_SEGMENT_WINDOW));

        // Snapshot index for carry validation and task materialization.
        let live_segments = snapshot
            .segments
            .iter()
            .enumerate()
            .map(|(idx, location)| (location, idx))
            .collect::<HashMap<_, _>>();
        let number_segments = snapshot.segments.len();
        let mut repack_count = 0;
        let mut recluster_segment_pruner = None;
        let mut decode_semaphore = None;

        let parts = loop {
            // Step 1: validate carried windows against the fresh snapshot.
            let carry_in = carry.pending.len();
            let valid_carry = std::mem::take(&mut carry.pending)
                .into_iter()
                .filter(|window| {
                    let valid = window.segments.iter().all(|(location, _)| {
                        live_segments.contains_key(location)
                            && !claimed_segments.contains(location.0.as_str())
                    });
                    if !valid {
                        debug!(
                            "recluster: carried window invalidated locations={} skip_reason=carried_location_missing",
                            window.segments.len(),
                        );
                    }
                    valid
                })
                .collect::<Vec<_>>();

            // Count only stable windows inherited from the previous round. Windows found by the
            // verification scan below are not counted until another `do_recluster` call, so
            // clearing this cache can trigger at most one immediate retry of the current range.
            let cached_stable_window_count = valid_carry
                .iter()
                .filter(|window| !window.has_tasks())
                .count();
            let has_cached_stable_windows = cached_stable_window_count > 0;
            let scan_start = carry.scan_cursor.min(number_segments);
            let scan_end = scan_start.saturating_add(chunk_size).min(number_segments);

            debug!(
                "recluster: candidate scan mode={:?} carry_in={} carry_valid={} scan_cursor={} scan_start={} scan_end={} chunk_size={}",
                mode,
                carry_in,
                valid_carry.len(),
                carry.scan_cursor,
                scan_start,
                scan_end,
                chunk_size,
            );

            // Step 2: scan this fixed range, excluding still-carried windows.
            // Carried tasks count toward early accept before new probing.
            let mut early_accept_count = if mutator.properties.enable_task_selection_v2 {
                0
            } else {
                valid_carry
                    .iter()
                    .flat_map(|window| window.tasks.iter())
                    .filter(|task| mutator.passes_early_accept(task))
                    .count()
            };
            let scan_locations = if scan_start < scan_end
                && (mutator.properties.enable_task_selection_v2
                    || early_accept_count < mutator.max_tasks)
            {
                let scan_range = &snapshot.segments[scan_start..scan_end];
                let carry_locations = (!valid_carry.is_empty()).then(|| {
                    valid_carry
                        .iter()
                        .flat_map(|window| window.segments.iter().map(|(location, _)| location))
                        .collect::<HashSet<_>>()
                });
                let mut scan_locations = Vec::with_capacity(scan_range.len());
                for (offset, location) in scan_range.iter().enumerate() {
                    // Intentional FINAL semantics: claimed segments are treated as work delegated
                    // to concurrent recluster tasks. This statement does not wait for claim owners;
                    // if every remaining candidate is claimed, it may return success even if an
                    // owner later fails. A subsequent RECLUSTER statement can pick that work up.
                    if claimed_segments.contains(location.0.as_str())
                        || carry_locations
                            .as_ref()
                            .is_some_and(|locations| locations.contains(location))
                    {
                        continue;
                    }
                    scan_locations.push(SegmentLocation {
                        segment_idx: scan_start + offset,
                        location: location.clone(),
                        snapshot_loc: None,
                    });
                }
                scan_locations
            } else {
                Vec::new()
            };
            let mut pending_windows = valid_carry;
            // Tracks whether LIMIT has consumed a non-pruned scan range.
            let mut scan_had_compact_segments = false;

            if !scan_locations.is_empty() {
                let scan_segments = scan_locations.len();
                if recluster_segment_pruner.is_none() {
                    recluster_segment_pruner = Some(Self::create_recluster_segment_pruner(
                        &ctx,
                        self.schema_with_stream(),
                        self.get_operator(),
                        &push_downs,
                    )?);
                }
                let (pruning_ctx, segment_pruner, max_concurrency) = {
                    let (pruning_ctx, segment_pruner, max_concurrency) =
                        recluster_segment_pruner.as_ref().unwrap();
                    (
                        pruning_ctx.clone(),
                        segment_pruner.clone(),
                        *max_concurrency,
                    )
                };
                let probe_segments = Self::segment_pruning(
                    pruning_ctx,
                    segment_pruner,
                    max_concurrency,
                    scan_locations,
                )
                .await?;

                let status = format!(
                    "[FUSE-RECLUSTER] Scanned segment range: scan_start={} scan_end={} scanned_segments={} probe_segments={} segment_progress={}/{} elapsed={:?}",
                    scan_start,
                    scan_end,
                    scan_segments,
                    probe_segments.len(),
                    scan_end,
                    number_segments,
                    start.elapsed()
                );
                ctx.set_status_info(&status);

                if probe_segments.is_empty() {
                    debug!(
                        "recluster: build candidates skipped scan_start={} scan_end={} skip_reason=empty_compact_segments",
                        scan_start, scan_end,
                    );
                } else {
                    scan_had_compact_segments = true;
                    let segment_windows = mutator.select_segments(&probe_segments, max_seg_num)?;
                    let windows_num = segment_windows.len();
                    debug!(
                        "recluster: selected segment windows probe_segments={} window_count={} max_segments={}",
                        probe_segments.len(),
                        windows_num,
                        max_seg_num,
                    );

                    let probe_start = Instant::now();
                    let mut probe_windows = 0usize;
                    let mut probe_tasks = 0usize;
                    let probe_parallelism = (max_threads / 4).clamp(1, 8);
                    let decode_runtime = carry.decode_runtime(max_threads)?;
                    let decode_semaphore = decode_semaphore
                        .get_or_insert_with(|| Arc::new(Semaphore::new(max_threads * 2)))
                        .clone();
                    let mut segment_windows = segment_windows.into_iter().enumerate();
                    loop {
                        let remaining_task_budget = if mutator.properties.enable_task_selection_v2 {
                            mutator.max_tasks
                        } else {
                            mutator.max_tasks.saturating_sub(early_accept_count)
                        };
                        if remaining_task_budget == 0 {
                            break;
                        }

                        let batch = segment_windows
                            .by_ref()
                            .take(probe_parallelism)
                            .collect::<Vec<_>>();
                        if batch.is_empty() {
                            break;
                        }

                        let batch_parallelism = probe_parallelism.min(batch.len());
                        let futures = batch.into_iter().map(|(_, selected_segs)| {
                            let mutator = mutator.clone();
                            let decode_runtime = decode_runtime.clone();
                            let decode_semaphore = decode_semaphore.clone();
                            async move {
                                if mutator.builds_tasks_after_decode() {
                                    mutator
                                        .decode_candidate_window(
                                            selected_segs,
                                            decode_runtime,
                                            decode_semaphore,
                                        )
                                        .await
                                } else {
                                    mutator
                                        .probe_candidate_window(
                                            selected_segs,
                                            remaining_task_budget,
                                            decode_runtime,
                                            decode_semaphore,
                                        )
                                        .await
                                }
                            }
                        });

                        let probed = execute_futures_in_parallel(
                            futures,
                            batch_parallelism,
                            batch_parallelism,
                            "recluster-probe-window-worker".to_owned(),
                        )
                        .await?
                        .into_iter()
                        .collect::<Result<Vec<_>>>()?;

                        for window in probed {
                            probe_windows += 1;
                            probe_tasks += window.tasks.len();
                            if !mutator.properties.enable_task_selection_v2 {
                                early_accept_count += window
                                    .tasks
                                    .iter()
                                    .filter(|task| mutator.passes_early_accept(task))
                                    .count();
                            }
                            pending_windows.push(window);
                        }
                    }
                    debug!(
                        event = "recluster.probed",
                        table_id = self.get_id(),
                        candidate_windows = windows_num,
                        probed_windows = probe_windows,
                        probe_tasks = probe_tasks,
                        pending_windows = pending_windows.len(),
                        elapsed :? = probe_start.elapsed();
                        "Recluster windows probed"
                    );
                }
            }

            let joint_stats = mutator.build_decoded_window_tasks(&mut pending_windows)?;

            let (_, parts) = if pending_windows.is_empty() {
                (0, ReclusterParts::default())
            } else {
                // Step 3: choose task candidates. Preserve score-only ranking unless
                // early accepts fill the task budget. In that case early candidates
                // rank first, while other probed tasks remain available to fill budget
                // left by candidates skipped due to the segment-claim limit.
                let enable_v2 = mutator.properties.enable_task_selection_v2;
                let prioritize_early_accept = !enable_v2 && early_accept_count >= mutator.max_tasks;
                let mut sorted_tasks = Vec::new();
                for (window_idx, window) in pending_windows.iter().enumerate() {
                    for (task_idx, task) in window.tasks.iter().enumerate() {
                        sorted_tasks.push((
                            window_idx,
                            task_idx,
                            task.score,
                            prioritize_early_accept && mutator.passes_early_accept(task),
                        ));
                    }
                }
                let mut selected_task_indices = match joint_stats.as_ref() {
                    // Joint selection preserves candidate order.
                    Some(stats) => select_joint_task_candidates(
                        &pending_windows,
                        &sorted_tasks,
                        mutator.max_tasks,
                        stats,
                        &mutator.properties,
                    )?,
                    None => {
                        if enable_v2 {
                            sorted_tasks.sort_by(|left, right| {
                                right.2.cmp_desc_v2(&left.2).then_with(|| {
                                    pending_windows[left.0].tasks[left.1]
                                        .base_level
                                        .cmp(&pending_windows[right.0].tasks[right.1].base_level)
                                })
                            });
                            let mut chain: Vec<&ReclusterTaskCandidate> = Vec::new();
                            let mut overlapping = Vec::new();
                            let mut ordered = Vec::with_capacity(sorted_tasks.len());
                            for candidate in sorted_tasks {
                                let task = &pending_windows[candidate.0].tasks[candidate.1];
                                if chain.iter().any(|picked| picked.key_span_intersects(task)) {
                                    overlapping.push(candidate);
                                } else {
                                    chain.push(task);
                                    ordered.push(candidate);
                                }
                            }
                            ordered.extend(overlapping);
                            sorted_tasks = ordered;
                        } else {
                            sort_task_candidates(&mut sorted_tasks);
                        }
                        select_task_candidates(&pending_windows, &sorted_tasks, mutator.max_tasks)
                    }
                };

                let mut selected = Vec::new();
                let mut remaining_windows = Vec::with_capacity(pending_windows.len());
                for (window_idx, window) in pending_windows.into_iter().enumerate() {
                    let task_indices = std::mem::take(&mut selected_task_indices[window_idx]);
                    if task_indices.is_empty() {
                        remaining_windows.push(window);
                    } else {
                        for &task_idx in &task_indices {
                            let task = &window.tasks[task_idx];
                            repack_count += usize::from(task.is_repack_only());
                            info!(
                                event = "recluster.candidate_selected",
                                table_id = self.get_id(),
                                window_idx,
                                task_idx,
                                base_level = task.base_level,
                                repack_only = task.is_repack_only(),
                                max_depth = task.score.max_depth,
                                avg_depth = task.score.average_depth,
                                block_count = task.selected_block_count(),
                                block_size = task.score.selected_total_bytes;
                                "Recluster candidate selected"
                            );
                        }
                        // Any selected task consumes its whole window.
                        selected.push((window, task_indices));
                    }
                }
                pending_windows = remaining_windows;

                // Step 4: build ReclusterParts from selected candidates.
                mutator
                    .materialize_task_candidates(&live_segments, selected)
                    .await?
            };

            if parts.removed_segment_indexes.len() > MAX_SEGMENT_LOCATIONS_PER_CLAIM {
                return Err(ErrorCode::Internal(format!(
                    "recluster selected {} segments, exceeding the maximum claim size of {}",
                    parts.removed_segment_indexes.len(),
                    MAX_SEGMENT_LOCATIONS_PER_CLAIM,
                )));
            }
            if !parts.is_empty() {
                // Keep unselected windows as best-effort continuation state for this scan range.
                // Empty windows cache stable probes while other tasks are still being rewritten.
                // Once real tasks are exhausted, the branch below drops this cache and verifies the
                // whole range once against the latest snapshot before declaring it stable.
                carry.pending = pending_windows;
                carry.scan_cursor = scan_start;
                break parts;
            }

            if has_cached_stable_windows {
                debug!(
                    "recluster: verify candidate range without stable-window cache scan_start={} scan_end={} stable_windows={}",
                    scan_start, scan_end, cached_stable_window_count,
                );
                carry.pending.clear();
                continue;
            }

            // Step 5: stable range, advance to the next scan range.
            carry.pending.clear();
            carry.scan_cursor = scan_end;
            debug!(
                "recluster: candidate stable range advanced next_scan_start={} number_segments={}",
                scan_end, number_segments,
            );
            // LIMIT stops after the first non-pruned scan range.
            if scan_end >= number_segments || (limit.is_some() && scan_had_compact_segments) {
                break parts;
            }
        };

        if mode != ReclusterMode::Aggressive {
            *carry = ReclusterFinalCarry::default();
        }

        let recluster_seg_num = parts.removed_segment_indexes.len() as u64;
        let elapsed_time = start.elapsed();
        info!(
            event = "recluster.planned",
            table_id = self.get_id(),
            strategy = format!("{:?}", mutator.properties.cluster_key_info.cluster_type).as_str(),
            mode = format!("{:?}", mode).as_str(),
            tasks = parts.tasks.len(),
            repack_count,
            segments = recluster_seg_num,
            block_count = parts.tasks.iter().map(|task| task.parts.len()).sum::<usize>();
            "Recluster planned"
        );
        ctx.set_status_info(&format!(
            "[FUSE-RECLUSTER] Task planning completed: elapsed={:?}",
            elapsed_time,
        ));
        metrics_inc_recluster_build_task_milliseconds(elapsed_time.as_millis() as u64);
        metrics_inc_recluster_segment_nums_scheduled(recluster_seg_num);

        Ok(Some((parts, snapshot)))
    }

    pub fn create_recluster_segment_pruner(
        ctx: &Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        push_down: &Option<PushDownInfo>,
    ) -> Result<(Arc<PruningContext>, Arc<SegmentPruner>, usize)> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrency = std::cmp::max(max_threads, 10);
        if max_concurrency > max_threads {
            warn!(
                "recluster: max_threads setting too low {}, increased to {}",
                max_threads, max_concurrency
            );
        }

        // During re-cluster, we do not rebuild missing bloom index.
        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            schema.clone(),
            push_down,
            None,
            BloomIndexColumns::None,
            vec![],
            HashSet::new(),
            max_concurrency,
            None,
        )?;
        let segment_pruner =
            SegmentPruner::create(pruning_ctx.clone(), schema, Default::default())?;

        Ok((pruning_ctx, segment_pruner, max_concurrency))
    }

    pub async fn segment_pruning(
        pruning_ctx: Arc<PruningContext>,
        segment_pruner: Arc<SegmentPruner>,
        max_concurrency: usize,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        if segment_locs.is_empty() {
            return Ok(Vec::new());
        }

        // Keep segment metadata pruning bounded for large snapshot ranges. Results are accumulated
        // in input order so strategy tie-breaking remains deterministic across pruning waves.
        let wave_size = max_concurrency.saturating_mul(4).max(1);
        let mut metas = Vec::new();
        let mut segment_locs = segment_locs.into_iter();
        loop {
            let wave = segment_locs.by_ref().take(wave_size).collect::<Vec<_>>();
            if wave.is_empty() {
                break;
            }

            let worker_count = max_concurrency.min(wave.len()).max(1);
            let batch_size = wave.len().div_ceil(worker_count);
            let works = wave.chunks(batch_size).map(|batch| {
                pruning_ctx.pruning_runtime.spawn({
                    let segment_pruner = segment_pruner.clone();
                    let batch = batch.to_vec();
                    async move {
                        let pruned_segments = segment_pruner.pruning(batch).await?;
                        Result::<_>::Ok(pruned_segments)
                    }
                })
            });

            let workers = futures::future::try_join_all(works).await?;
            for worker in workers {
                metas.extend(worker?);
            }
        }

        Ok(metas)
    }
}

#[cfg(test)]
mod tests {
    use databend_storages_common_table_meta::meta::SegmentInfo;
    use databend_storages_common_table_meta::meta::Statistics;

    use super::*;
    use crate::operations::recluster::ReclusterTaskCandidate;

    fn score(priority: usize) -> CandidateScore {
        CandidateScore {
            selected_total_bytes: priority,
            selected_block_count: priority,
            estimated_depth_gain: priority as i64,
            task_threshold_bytes: priority,
            touched_segment_count: 1,
            max_depth: priority,
            average_depth: priority as f64,
        }
    }

    fn candidate(selected_positions: impl IntoIterator<Item = usize>) -> ReclusterTaskCandidate {
        ReclusterTaskCandidate {
            score: score(1),
            selected_blocks: selected_positions
                .into_iter()
                .map(|position| (position, vec![0]))
                .collect(),
            base_level: 0,
            input_level_stats: Vec::new(),
            all_ordered: false,
            key_span: None,
        }
    }

    fn window(prefix: &str, segment_count: usize) -> ReclusterCandidateWindow {
        ReclusterCandidateWindow {
            segments: (0..segment_count)
                .map(|index| ((format!("{}-{}", prefix, index), 0), None))
                .collect(),
            tasks: vec![candidate(0..segment_count)],
            decoded_blocks: Arc::new(Vec::new()),
        }
    }

    fn repack_window(prefix: &str, segment_count: usize) -> ReclusterCandidateWindow {
        let mut window = window(prefix, segment_count);
        let segment_info = Arc::new(SegmentInfo::new(vec![], Statistics::default()));
        for (_, info) in &mut window.segments {
            *info = Some(segment_info.clone());
        }
        window.tasks = vec![candidate(std::iter::empty())];
        window
    }

    #[test]
    fn test_joint_selection_prefers_singleton_and_plateau_members() {
        use databend_common_expression::BlockThresholds;
        use databend_common_expression::Scalar;
        use databend_common_expression::types::DataType;
        use databend_common_expression::types::NumberDataType;
        use databend_storages_common_table_meta::meta::BlockMeta;
        use databend_storages_common_table_meta::meta::ClusterKeyInfo;
        use databend_storages_common_table_meta::meta::ClusterStatistics;
        use databend_storages_common_table_meta::meta::Compression;
        use databend_storages_common_table_meta::table::ClusterType;

        use crate::operations::recluster::ReclusterBlock;
        use crate::operations::recluster::ReclusterBlockStats;
        use crate::operations::recluster::ReclusterDepthStats;
        use crate::operations::recluster::ReclusterGroup;
        use crate::operations::recluster::ReclusterProperties;
        use crate::operations::recluster::linear_recluster::LinearReclusterStrategy;
        use crate::operations::recluster::task_candidate;

        let properties = ReclusterProperties {
            mode: ReclusterMode::Aggressive,
            depth_threshold: 1.0,
            block_thresholds: BlockThresholds::new(10, 10, 10, 10),
            cluster_key_info: ClusterKeyInfo::new((0, "(key)".to_string()), ClusterType::Linear),
            partition_key_count: 0,
            memory_threshold: 20,
            enable_task_selection_v2: true,
            prepared_cluster_key_exprs: Vec::new(),
            scalar_cluster_key_types: vec![DataType::Number(NumberDataType::Int32)],
        };
        let blocks = [(0i32, 1i32), (0, 1), (3, 3), (5, 5), (4, 4)]
            .into_iter()
            .enumerate()
            .map(|(i, (lo, hi))| ReclusterBlock {
                index: crate::operations::common::BlockMetaIndex {
                    segment_idx: 0,
                    block_idx: i,
                },
                meta: Arc::new(BlockMeta::new(
                    8,
                    10,
                    10,
                    HashMap::new(),
                    HashMap::new(),
                    None,
                    (format!("b{i}"), 2),
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
                )),
                stats: ReclusterBlockStats::Normalized(ClusterStatistics::new(
                    0,
                    vec![Scalar::from(lo)],
                    vec![Scalar::from(hi)],
                    0,
                )),
            })
            .collect::<Vec<_>>();
        let refs = blocks.iter().collect::<Vec<_>>();
        let stats =
            ReclusterDepthStats::create(blocks.iter(), &properties.scalar_cluster_key_types)
                .unwrap();
        let a = task_candidate(ReclusterGroup::Level(0), score(20), &[0, 1], &refs);
        let b = task_candidate(ReclusterGroup::Level(0), score(20), &[2, 3], &refs);
        assert!(
            stats
                .gain([refs[..2].iter().copied()], &properties)
                .unwrap()
                > stats
                    .gain(
                        [refs[..2].iter().copied(), refs[2..4].iter().copied()],
                        &properties
                    )
                    .unwrap()
        );
        let windows = vec![ReclusterCandidateWindow {
            segments: vec![(("segment".to_string(), 4), None)],
            tasks: vec![a, b],
            decoded_blocks: Arc::new(blocks),
        }];
        let ranked = vec![(0, 0, score(20), false), (0, 1, score(20), false)];
        assert_eq!(
            select_joint_task_candidates(&windows, &ranked, 2, &stats, &properties).unwrap(),
            vec![vec![0]]
        );
        assert_eq!(
            select_joint_task_candidates(&windows, &ranked, 0, &stats, &properties).unwrap(),
            vec![Vec::<usize>::new()]
        );

        // More than two slots uses the same search, without rejecting the budget.
        // Independent pairs in a fully overlapping layout have additive improvement.
        let blocks = (0..16)
            .map(|i| ReclusterBlock {
                index: crate::operations::common::BlockMetaIndex {
                    segment_idx: 0,
                    block_idx: i,
                },
                meta: windows[0].decoded_blocks[0].meta.clone(),
                stats: ReclusterBlockStats::Normalized(ClusterStatistics::new(
                    0,
                    vec![Scalar::from(0i32)],
                    vec![Scalar::from(1i32)],
                    0,
                )),
            })
            .map(|mut b| {
                Arc::make_mut(&mut b.meta).location.0 =
                    format!("independent-{}", b.index.block_idx);
                b
            })
            .collect::<Vec<_>>();
        let refs = blocks.iter().collect::<Vec<_>>();
        let many_stats =
            ReclusterDepthStats::create(blocks.iter(), &properties.scalar_cluster_key_types)
                .unwrap();
        let many_tasks = (0..8)
            .map(|i| {
                task_candidate(
                    ReclusterGroup::Level(0),
                    score(20),
                    &[2 * i, 2 * i + 1],
                    &refs,
                )
            })
            .collect::<Vec<_>>();
        let many_windows = vec![ReclusterCandidateWindow {
            segments: vec![(("segment".into(), 4), None)],
            tasks: many_tasks,
            decoded_blocks: Arc::new(blocks),
        }];
        let many_ranked = (0..8).map(|i| (0, i, score(20), false)).collect::<Vec<_>>();
        for budget in [1, 2, 3, 4, 8] {
            let selected = select_joint_task_candidates(
                &many_windows,
                &many_ranked,
                budget,
                &many_stats,
                &properties,
            )
            .unwrap();
            assert_eq!(selected[0], (0..budget).collect::<Vec<_>>());
        }

        // A lower shoulder intersects the component but not its highest plateau.
        let mut blocks = windows[0]
            .decoded_blocks
            .iter()
            .map(|b| ReclusterBlock {
                index: b.index.clone(),
                meta: b.meta.clone(),
                stats: ReclusterBlockStats::Normalized(b.stats().clone()),
            })
            .collect::<Vec<_>>();
        for (block, (lo, hi)) in blocks
            .iter_mut()
            .zip([(0, 4), (2, 4), (2, 6), (0, 1), (5, 6)])
        {
            block.stats = ReclusterBlockStats::Normalized(ClusterStatistics::new(
                0,
                vec![Scalar::from(lo)],
                vec![Scalar::from(hi)],
                0,
            ));
        }
        let refs = blocks.iter().collect::<Vec<_>>();
        let ids = (0..5).collect::<Vec<_>>();
        let scan = LinearReclusterStrategy::scan_hotspots(&properties, &ids, &refs).unwrap();
        let candidates = scan.peak_candidates(
            scan.peaks[0].0,
            1,
            ReclusterGroup::Level(0),
            &ids,
            &refs,
            &properties,
        );
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].selected_blocks, vec![(0, vec![0, 1])]);

        // Two orders share one quota; neither shoulder block enters either task.
        for (block, (lo, hi)) in blocks
            .iter_mut()
            .zip([(0, 10), (4, 6), (4, 6), (0, 1), (9, 10)])
        {
            block.stats = ReclusterBlockStats::Normalized(ClusterStatistics::new(
                0,
                vec![Scalar::from(lo)],
                vec![Scalar::from(hi)],
                0,
            ));
        }
        let refs = blocks.iter().collect::<Vec<_>>();
        let scan = LinearReclusterStrategy::scan_hotspots(&properties, &ids, &refs).unwrap();
        let candidates = scan.peak_candidates(
            scan.peaks[0].0,
            2,
            ReclusterGroup::Level(0),
            &ids,
            &refs,
            &properties,
        );
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].selected_blocks, vec![(0, vec![1, 2])]);
        assert_eq!(candidates[1].selected_blocks, vec![(0, vec![0, 1])]);
        let windows = vec![ReclusterCandidateWindow {
            segments: vec![(("segment".into(), 4), None)],
            tasks: candidates,
            decoded_blocks: Arc::new(blocks),
        }];
        let stats = ReclusterDepthStats::create(
            windows[0].decoded_blocks.iter(),
            &properties.scalar_cluster_key_types,
        )
        .unwrap();
        // The two orderings share block1; never schedule both despite two slots.
        let selected =
            select_joint_task_candidates(&windows, &ranked, 2, &stats, &properties).unwrap();
        assert_eq!(selected[0].len(), 1);
    }

    #[test]
    fn test_task_ranking_uses_score_without_early_priority() {
        let mut ranked = vec![
            (0, 0, score(1), false),
            (1, 0, score(3), false),
            (2, 0, score(2), false),
        ];

        sort_task_candidates(&mut ranked);

        assert_eq!(
            ranked
                .into_iter()
                .map(|(window_idx, _, _, _)| window_idx)
                .collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
    }

    #[test]
    fn test_task_selection_skips_claim_overflow_and_uses_remaining_budget() {
        let windows = vec![
            window("first", 10),
            window("oversized", 120),
            window("last", 5),
        ];
        let mut ranked = vec![
            (2, 0, score(100), false),
            (1, 0, score(2), true),
            (0, 0, score(3), true),
        ];
        sort_task_candidates(&mut ranked);

        let selected = select_task_candidates(&windows, &ranked, 3);

        assert_eq!(selected, vec![vec![0], vec![], vec![0]]);
    }

    #[test]
    fn test_task_selection_accepts_exact_claim_limit() {
        let windows = vec![window("first", 10), window("second", 118)];
        let ranked = vec![(0, 0, score(2), false), (1, 0, score(1), false)];

        let selected = select_task_candidates(&windows, &ranked, 2);

        assert_eq!(selected, vec![vec![0], vec![0]]);
    }

    #[test]
    fn test_task_selection_counts_repack_window_segments() {
        let windows = vec![
            window("first", 10),
            repack_window("repack", 120),
            window("last", 5),
        ];
        let ranked = vec![
            (0, 0, score(3), false),
            (1, 0, score(2), false),
            (2, 0, score(1), false),
        ];

        let selected = select_task_candidates(&windows, &ranked, 3);

        assert_eq!(selected, vec![vec![0], vec![], vec![0]]);
    }

    #[test]
    fn test_task_selection_counts_segment_union() {
        let mut shared_window = window("shared", MAX_SEGMENT_LOCATIONS_PER_CLAIM);
        shared_window.tasks = vec![candidate(0..120), candidate(110..128)];
        let windows = vec![shared_window];
        let ranked = vec![(0, 0, score(2), false), (0, 1, score(1), false)];

        let selected = select_task_candidates(&windows, &ranked, 2);

        assert_eq!(selected, vec![vec![0, 1]]);
    }
}
