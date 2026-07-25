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
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
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
use crate::operations::ReclusterFinalCarry;
use crate::operations::ReclusterMutator;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;

const DEFAULT_RECLUSTER_SEGMENT_LIMIT: usize = 1024;
const DEFAULT_MIN_RECLUSTER_SEGMENT_WINDOW: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclusterMode {
    Conservative,
    Aggressive,
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
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let start = Instant::now();

        ctx.set_status_info("[FUSE-RECLUSTER] Starting recluster operation");

        if self.cluster_key_meta().is_none() {
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
        if carry_has_state && carry.cluster_key_id != mutator.cluster_key_id {
            debug!(
                "recluster: reset carry reason=cluster_key_changed old_cluster_key_id={} new_cluster_key_id={}",
                carry.cluster_key_id, mutator.cluster_key_id,
            );
            carry.scan_cursor = 0;
            carry.pending.clear();
        }
        carry.cluster_key_id = mutator.cluster_key_id;

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
        let mut recluster_blocks_count = 0;
        let mut recluster_segment_pruner = None;
        let mut decode_semaphore = None;

        let parts = loop {
            // Step 1: validate carried windows against the fresh snapshot.
            let carry_in = carry.pending.len();
            let valid_carry = std::mem::take(&mut carry.pending)
                .into_iter()
                .filter(|window| {
                    let valid = window
                        .segments
                        .iter()
                        .all(|(location, _)| live_segments.contains_key(location));
                    if !valid {
                        debug!(
                            "recluster: carried window invalidated locations={} skip_reason=carried_location_missing",
                            window.segments.len(),
                        );
                    }
                    valid
                })
                .collect::<Vec<_>>();

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
            let mut early_accept_count = valid_carry
                .iter()
                .flat_map(|window| window.tasks.iter())
                .filter(|task| mutator.passes_early_accept(task))
                .count();
            let scan_locations = if scan_start < scan_end && early_accept_count < mutator.max_tasks
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
                    if carry_locations
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
                    "[FUSE-RECLUSTER] Scanned segment range: scan_start={} scan_end={} scan_segments={} probe_segments={} segment_progress={}/{}, elapsed={:?}",
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
                    while early_accept_count < mutator.max_tasks {
                        let remaining_task_budget =
                            mutator.max_tasks.saturating_sub(early_accept_count);
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
                                mutator
                                    .probe_candidate_window(
                                        selected_segs,
                                        remaining_task_budget,
                                        decode_runtime,
                                        decode_semaphore,
                                    )
                                    .await
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
                            early_accept_count += window
                                .tasks
                                .iter()
                                .filter(|task| mutator.passes_early_accept(task))
                                .count();
                            pending_windows.push(window);
                        }
                    }
                    info!(
                        "recluster: probed candidate windows candidate_windows={} probe_windows={} probe_tasks={} pending_windows={} elapsed={:?}",
                        windows_num,
                        probe_windows,
                        probe_tasks,
                        pending_windows.len(),
                        probe_start.elapsed(),
                    );
                }
            }

            let (block_count, parts) = if pending_windows.is_empty() {
                (0, ReclusterParts::default())
            } else {
                // Step 3: choose task candidates. If early accept fills the budget,
                // only early-accept tasks compete; otherwise rank all pending tasks.
                let early_accept_only = early_accept_count >= mutator.max_tasks;
                let mut sorted_tasks = Vec::new();
                for (window_idx, window) in pending_windows.iter().enumerate() {
                    for (task_idx, task) in window.tasks.iter().enumerate() {
                        if !early_accept_only || mutator.passes_early_accept(task) {
                            sorted_tasks.push((window_idx, task_idx, task.score));
                        }
                    }
                }
                sorted_tasks.sort_by(|left, right| right.2.cmp_desc(&left.2));

                let mut selected_task_indices = vec![Vec::new(); pending_windows.len()];
                let mut selected_count = 0;
                let mut selected_repack_only = false;
                for (window_idx, task_idx, _) in sorted_tasks {
                    if selected_count >= mutator.max_tasks {
                        break;
                    }
                    let task = &pending_windows[window_idx].tasks[task_idx];
                    // Repack-only candidates rewrite no blocks, but each one
                    // consumes a whole window. Keep one per round so max_tasks
                    // does not repack multiple disjoint windows at once.
                    if task.is_repack_only() {
                        if selected_repack_only {
                            continue;
                        }
                        selected_repack_only = true;
                    }
                    selected_task_indices[window_idx].push(task_idx);
                    selected_count += 1;
                }

                let mut selected = Vec::new();
                let mut remaining_windows = Vec::with_capacity(pending_windows.len());
                for (window_idx, window) in pending_windows.into_iter().enumerate() {
                    let task_indices = std::mem::take(&mut selected_task_indices[window_idx]);
                    if task_indices.is_empty() {
                        remaining_windows.push(window);
                    } else {
                        for &task_idx in &task_indices {
                            let task = &window.tasks[task_idx];
                            info!(
                                "recluster: selected task candidate window_idx={} task_idx={} {}",
                                window_idx, task_idx, task,
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

            recluster_blocks_count += block_count;

            if !parts.is_empty() {
                // Keep unselected windows as coverage for this fixed FINAL scan range.
                // A successful task consumes only the selected window. We intentionally do
                // not invalidate unrelated probed windows for overlaps with newly written
                // segments; doing so would turn FINAL into an expensive fixed-point rescan
                // over its own intermediate output.
                carry.pending = pending_windows;
                carry.scan_cursor = scan_start;
                break parts;
            }

            // Step 5: stable range, advance to the next fixed scan range.
            carry.pending.clear();
            let next_scan_start = scan_end;
            carry.scan_cursor = next_scan_start;
            debug!(
                "recluster: candidate stable chunk advanced next_scan_start={} number_segments={}",
                next_scan_start, number_segments,
            );
            // LIMIT stops after the first non-pruned scan range.
            if next_scan_start >= number_segments || (limit.is_some() && scan_had_compact_segments)
            {
                break parts;
            }
        };

        if mode != ReclusterMode::Aggressive {
            *carry = ReclusterFinalCarry::default();
        }

        let recluster_seg_num = parts.removed_segment_indexes.len() as u64;
        let elapsed_time = start.elapsed();
        ctx.set_status_info(&format!(
            "[FUSE-RECLUSTER] Built recluster tasks: tasks={} segments={} blocks={} elapsed={:?}",
            parts.tasks.len(),
            recluster_seg_num,
            recluster_blocks_count,
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
        mut segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        if segment_locs.is_empty() {
            return Ok(Vec::new());
        }

        let mut remain = segment_locs.len() % max_concurrency;
        let batch_size = segment_locs.len() / max_concurrency;
        let mut works = Vec::with_capacity(max_concurrency);

        while !segment_locs.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
            works.push(pruning_ctx.pruning_runtime.spawn({
                let segment_pruner = segment_pruner.clone();

                async move {
                    let pruned_segments = segment_pruner.pruning(batch).await?;
                    Result::<_>::Ok(pruned_segments)
                }
            }));
        }

        let mut metas = vec![];
        let workers = futures::future::try_join_all(works).await?;
        for worker in workers {
            let res = worker?;
            metas.extend(res);
        }

        Ok(metas)
    }
}
