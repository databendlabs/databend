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

// Logs from this module will show up as "[RECLUSTER-HOOK] ...".
databend_common_tracing::register_module_tag!("[RECLUSTER-HOOK]");

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_sql::plans::ReclusterPlan;
use log::info;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::interpreter_table_recluster::ReclusterJob;
use crate::interpreters::interpreter_table_recluster::ReclusterRoundOutcome;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::table_maintenance::scheduler::TaskKind;
use crate::table_maintenance::task::MaintenanceOutcome;

pub(super) fn recluster_plan(
    ctx: &QueryContext,
    target: &TableHookTarget,
    table: &dyn Table,
    background: bool,
) -> Result<ReclusterPlan> {
    Ok(ReclusterPlan {
        catalog: target.catalog.clone(),
        database: target.database.clone(),
        table: target.table.clone(),
        target: if is_materialized_view_engine(table.engine()) {
            MaintenanceTarget::MaterializedView {
                table_id: target.table_id,
            }
        } else {
            MaintenanceTarget::Table
        },
        expected_table_id: Some(target.table_id),
        // Only inline recluster bounds the write's latency. Queued rounds use the
        // normal Fuse scan range and yield after each stable range.
        limit: if background {
            None
        } else {
            Some(ctx.get_settings().get_auto_compaction_segments_limit()? as usize)
        },
        selection: None,
        is_final: background,
    })
}

struct ReclusterProgress {
    job: ReclusterJob,
    slice_elapsed: Duration,
    rounds: usize,
    rescan_requested: bool,
}

/// Keep only lightweight job/scan/claim progress, never a frozen input generation.
/// Each round replans on the latest table and releases candidates and decode workers
/// before yielding its scheduler slot, including on a retryable conflict.
pub(crate) struct ReclusterTask {
    progress: Option<ReclusterProgress>,
}

impl ReclusterTask {
    pub(crate) fn new() -> Self {
        Self { progress: None }
    }

    pub(crate) fn merge(&mut self) {
        if let Some(progress) = &mut self.progress {
            // Do not rewind an unfinished scan on every write. Coalesce demand for
            // another pass so a hot table can still reach its later ranges.
            progress.rescan_requested = true;
        }
    }

    pub(crate) fn reset_progress(&mut self) {
        self.progress = None;
    }

    pub(crate) async fn run(
        &mut self,
        ctx: Arc<QueryContext>,
        target: &TableHookTarget,
    ) -> Result<MaintenanceOutcome> {
        ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let Some((target, table)) = target.resolve_background(&ctx, TaskKind::Recluster).await?
        else {
            return Ok(MaintenanceOutcome::Completed);
        };
        if table.cluster_key_meta().is_none() {
            return Ok(MaintenanceOutcome::Completed);
        }
        if self.progress.is_none() {
            let plan = recluster_plan(&ctx, &target, table.as_ref(), true)?;
            self.progress = Some(ReclusterProgress {
                job: ReclusterJob::background(plan, true),
                slice_elapsed: Duration::ZERO,
                rounds: 0,
                rescan_requested: false,
            });
        }
        let state = self.progress.as_mut().expect("recluster job initialized");
        state.job.set_target_name(target.database, target.table);
        let timeout = Duration::from_secs(ctx.get_settings().get_recluster_timeout_secs()?.max(1));
        if state.slice_elapsed >= timeout {
            state.slice_elapsed = Duration::ZERO;
            return Ok(MaintenanceOutcome::Deferred(Duration::from_secs(5)));
        }
        state.rounds += 1;
        let started = Instant::now();
        let result = state.job.run_round(&ctx).await;
        state.slice_elapsed += started.elapsed();
        let outcome = result?;
        info!(event = "recluster.background_round", table_id = target.table_id,
            round = state.rounds, outcome :? = outcome;
            "Background recluster yielded its execution slot");
        match outcome {
            ReclusterRoundOutcome::Committed => {
                // A successful background commit already restarts scanning at the
                // latest head, covering notifications merged before this round.
                state.rescan_requested = false;
                Ok(MaintenanceOutcome::Progress(Duration::from_millis(100)))
            }
            ReclusterRoundOutcome::ScanProgress => Ok(MaintenanceOutcome::ScanProgress),
            ReclusterRoundOutcome::NoParts if std::mem::take(&mut state.rescan_requested) => {
                state.job.revisit_head();
                // A requested rescan is not a commit: preserve the no-progress budget
                // and return to the scheduler before acquiring a fresh execution grant.
                Ok(MaintenanceOutcome::ScanProgress)
            }
            ReclusterRoundOutcome::NoParts => {
                state.job.finish(&ctx).await;
                Ok(MaintenanceOutcome::Completed)
            }
            ReclusterRoundOutcome::ClaimRetriesExhausted => Err(ErrorCode::TableAlreadyLocked(
                "Background recluster will revisit ranges held by segment claim owners",
            )),
            ReclusterRoundOutcome::Conflict(error) => Err(error),
        }
    }
}
