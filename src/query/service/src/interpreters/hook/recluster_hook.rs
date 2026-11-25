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
use databend_common_storages_fuse::operations::VectorFinalPlan;
use log::info;

use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::interpreter_table_recluster::ReclusterJob;
use crate::interpreters::interpreter_table_recluster::ReclusterRoundOutcome;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::table_maintenance::scheduler::TaskKind;
use crate::table_maintenance::task::MaintenanceOutcome;

fn recluster_plan(target: &TableHookTarget, table: &dyn Table) -> ReclusterPlan {
    ReclusterPlan {
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
        limit: None,
        selection: None,
        is_final: true,
    }
}

/// State retained between background rounds, without an execution context or grant.
pub(crate) struct ReclusterProgress {
    job: ReclusterJob,
    slice_elapsed: Duration,
    rounds: usize,
}

impl ReclusterProgress {
    pub(crate) fn reset_candidates(&mut self) {
        self.job.reset_candidates();
    }

    pub(crate) fn retained_full_pass_bytes(&self) -> u64 {
        self.job.retained_full_pass_bytes()
    }

    /// Replan on the latest table each round. A vector FINAL retains its starting
    /// block locations across rounds to delimit the full pass.
    pub(crate) async fn run(
        progress: &mut Option<Self>,
        ctx: Arc<QueryContext>,
        target: &TableHookTarget,
        cluster_key_id: u32,
    ) -> Result<MaintenanceOutcome> {
        ctx.set_cache_policy(ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        let Some((target, table)) = target.resolve_background(&ctx, TaskKind::Recluster).await?
        else {
            return Ok(MaintenanceOutcome::Completed);
        };
        let current_key_id = table.get_table_info().meta.cluster_key_id();
        if current_key_id != Some(cluster_key_id) {
            return Ok(MaintenanceOutcome::Completed);
        }
        let state = if let Some(state) = progress {
            state
        } else {
            let plan = recluster_plan(&target, table.as_ref());
            let mut job = ReclusterJob::background(plan, cluster_key_id);
            let started = Instant::now();
            if job.prepare_final(&ctx).await? == VectorFinalPlan::Skip {
                return Ok(MaintenanceOutcome::Completed);
            }
            progress.insert(Self {
                job,
                slice_elapsed: started.elapsed(),
                rounds: 0,
            })
        };
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
                Ok(MaintenanceOutcome::Progress(Duration::from_millis(100)))
            }
            ReclusterRoundOutcome::ScanProgress => Ok(MaintenanceOutcome::ScanProgress),
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
