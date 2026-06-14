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
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ClusterType;
use log::debug;
use log::warn;
use opendal::Operator;

use crate::FuseTable;
use crate::SegmentLocation;
use crate::operations::ReclusterCandidateWindow;
use crate::operations::ReclusterFinalCarry;
use crate::operations::ReclusterMutator;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;
use crate::pruning::create_segment_location_vector;

const DEFAULT_RECLUSTER_SEGMENT_LIMIT: usize = 1024;
const DEFAULT_MIN_RECLUSTER_SEGMENT_WINDOW: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReclusterMode {
    Normal,
    Final,
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

        if self.cluster_type().is_none_or(|v| v != ClusterType::Linear) {
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

        let mutator = ReclusterMutator::try_create(self, ctx.clone(), snapshot.as_ref())?;

        // Carry is tied to the current cluster key because cached block metas
        // may be normalized during candidate probing.
        let carry_has_state = !carry.pending.is_empty() || carry.scan_cursor != 0;
        if carry_has_state && carry.cluster_key_id != mutator.cluster_key_id {
            debug!(
                "recluster: reset carry reason=cluster_key_changed old_cluster_key_id={} new_cluster_key_id={}",
                carry.cluster_key_id, mutator.cluster_key_id,
            );
            *carry = ReclusterFinalCarry::default();
        }
        carry.cluster_key_id = mutator.cluster_key_id;

        let segment_locations = create_segment_location_vector(snapshot.segments.clone(), None);

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let segment_limit = limit.unwrap_or(DEFAULT_RECLUSTER_SEGMENT_LIMIT);
        let candidate_window_limit = (max_threads * 4).max(DEFAULT_MIN_RECLUSTER_SEGMENT_WINDOW);
        // LIMIT is applied to scan/window sizing, not task selection. It is a
        // soft scan bound because recluster still keeps a minimum candidate window.
        let chunk_size = segment_limit.max(candidate_window_limit);
        // LIMIT also caps one candidate window; select_segments treats this as
        // a soft upper bound when hotspot blocks are inseparable.
        let max_seg_num = segment_limit.min(candidate_window_limit);
        let max_tasks = mutator.max_tasks;

        // Snapshot index for carry validation and task materialization.
        let live_segments = snapshot
            .segments
            .iter()
            .enumerate()
            .map(|(idx, location)| (location, idx))
            .collect::<HashMap<_, _>>();
        let number_segments = segment_locations.len();
        let mut recluster_blocks_count = 0;

        let parts = loop {
            // Step 1: validate carried windows against the fresh snapshot.
            let carry_in = carry.pending.len();
            let valid_carry = Self::take_valid_carry(carry, &live_segments);

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
            let scan_locations = if scan_start < scan_end && early_accept_count < max_tasks {
                let scan_range = &segment_locations[scan_start..scan_end];
                if valid_carry.is_empty() {
                    scan_range.to_vec()
                } else {
                    let carry_locations = valid_carry
                        .iter()
                        .flat_map(|window| window.segments.iter().map(|(location, _)| location))
                        .collect::<HashSet<_>>();
                    scan_range
                        .iter()
                        .filter(|segment| !carry_locations.contains(&segment.location))
                        .cloned()
                        .collect::<Vec<_>>()
                }
            } else {
                Vec::new()
            };
            let mut pending_windows = valid_carry;
            // Tracks whether LIMIT has consumed a non-pruned scan range.
            let mut scan_had_compact_segments = false;

            if !scan_locations.is_empty() {
                let compact_segments = Self::segment_pruning(
                    &ctx,
                    self.schema_with_stream(),
                    self.get_operator(),
                    &push_downs,
                    scan_locations,
                )
                .await?;

                debug!(
                    "recluster: scanned segment range scan_start={} scan_end={} compact_segments={} segment_progress={}/{}",
                    scan_start,
                    scan_end,
                    compact_segments.len(),
                    scan_end,
                    segment_locations.len(),
                );
                let status = format!(
                    "[FUSE-RECLUSTER] Read segment files: {}/{}, elapsed: {:?}",
                    scan_end,
                    segment_locations.len(),
                    start.elapsed()
                );
                ctx.set_status_info(&status);

                if compact_segments.is_empty() {
                    debug!(
                        "recluster: build candidates skipped scan_start={} scan_end={} skip_reason=empty_compact_segments",
                        scan_start, scan_end,
                    );
                } else {
                    scan_had_compact_segments = true;
                    let segment_windows =
                        mutator.select_segments(&compact_segments, max_seg_num)?;
                    debug!(
                        "recluster: selected segment windows compact_segments={} window_count={} max_segments={}",
                        compact_segments.len(),
                        segment_windows.len(),
                        max_seg_num,
                    );

                    for selected_segs in segment_windows {
                        // Probe only; materialize after all pending tasks are ranked.
                        let window = mutator.probe_candidate_window(selected_segs, mode).await?;
                        early_accept_count += window
                            .tasks
                            .iter()
                            .filter(|task| mutator.passes_early_accept(task))
                            .count();
                        if !window.tasks.is_empty() {
                            pending_windows.push(window);
                        }

                        if early_accept_count >= max_tasks {
                            break;
                        }
                    }
                }
            }

            let (block_count, parts) = if pending_windows.is_empty() {
                (0, ReclusterParts::default())
            } else {
                // Step 3: choose task candidates. If early accept fills the budget,
                // only early-accept tasks compete; otherwise rank all pending tasks.
                let early_accept_only = early_accept_count >= max_tasks;
                let mut sorted_tasks = Vec::new();
                for (window_idx, window) in pending_windows.iter().enumerate() {
                    for (task_idx, task) in window.tasks.iter().enumerate() {
                        if !early_accept_only || mutator.passes_early_accept(task) {
                            sorted_tasks.push((window_idx, task_idx, task.score));
                        }
                    }
                }
                sorted_tasks.sort_by(|left, right| {
                    right
                        .2
                        .cmp_desc(&left.2)
                        .then_with(|| left.0.cmp(&right.0))
                        .then_with(|| left.1.cmp(&right.1))
                });

                let mut selected_task_indices = vec![Vec::new(); pending_windows.len()];
                for (window_idx, task_idx, _) in sorted_tasks.into_iter().take(max_tasks) {
                    selected_task_indices[window_idx].push(task_idx);
                }

                let mut selected = Vec::new();
                let mut remaining_windows = Vec::with_capacity(pending_windows.len());
                for (window_idx, window) in pending_windows.into_iter().enumerate() {
                    let task_indices = std::mem::take(&mut selected_task_indices[window_idx]);
                    if task_indices.is_empty() {
                        remaining_windows.push(window);
                    } else {
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

            let stable_chunk = parts.is_empty() && pending_windows.is_empty();
            recluster_blocks_count += block_count;

            if !stable_chunk {
                // Keep unselected windows for the next FINAL round.
                carry.pending = pending_windows;
                carry.scan_cursor = scan_start;
                break parts;
            }

            // Step 5: stable range, advance to the next fixed scan range.
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

        if mode != ReclusterMode::Final {
            *carry = ReclusterFinalCarry::default();
        }

        let recluster_seg_num = parts.removed_segment_indexes.len() as u64;
        let elapsed_time = start.elapsed();
        ctx.set_status_info(&format!(
            "[FUSE-RECLUSTER] Built recluster tasks: segments={} blocks={} elapsed={:?}",
            recluster_seg_num, recluster_blocks_count, elapsed_time,
        ));
        metrics_inc_recluster_build_task_milliseconds(elapsed_time.as_millis() as u64);
        metrics_inc_recluster_segment_nums_scheduled(recluster_seg_num);

        Ok(Some((parts, snapshot)))
    }

    fn take_valid_carry(
        carry: &mut ReclusterFinalCarry,
        live_segments: &HashMap<&Location, usize>,
    ) -> Vec<ReclusterCandidateWindow> {
        std::mem::take(&mut carry.pending)
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
            .collect()
    }

    pub async fn segment_pruning(
        ctx: &Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        mut segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrency = std::cmp::max(max_threads, 10);
        if max_concurrency > max_threads {
            warn!(
                "recluster: max_threads setting too low {}, increased to {}",
                max_threads, max_concurrency
            );
        }

        // during re-cluster, we do not rebuild missing bloom index
        let bloom_index_builder = None;
        // Only use push_down here.
        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            schema.clone(),
            push_down,
            None,
            vec![],
            BloomIndexColumns::None,
            vec![],
            HashSet::new(),
            max_concurrency,
            bloom_index_builder,
        )?;

        let segment_pruner =
            SegmentPruner::create(pruning_ctx.clone(), schema, Default::default())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_final_carry_invalid_locations_are_discarded() {
        let live_location = ("live".to_string(), 0);
        let still_live_location = ("still-live".to_string(), 0);
        let mut carry = ReclusterFinalCarry {
            pending: vec![
                ReclusterCandidateWindow {
                    segments: vec![
                        (live_location.clone(), None),
                        (("missing".to_string(), 0), None),
                    ],
                    tasks: Vec::new(),
                },
                ReclusterCandidateWindow {
                    segments: vec![(still_live_location.clone(), None)],
                    tasks: Vec::new(),
                },
            ],
            scan_cursor: 0,
            cluster_key_id: 0,
        };
        let live_segments = HashMap::from([(&live_location, 0usize), (&still_live_location, 1)]);

        let valid = FuseTable::take_valid_carry(&mut carry, &live_segments);

        assert_eq!(valid.len(), 1);
        assert_eq!(valid[0].segments[0].0.0, "still-live");
        assert!(carry.pending.is_empty());
    }
}
