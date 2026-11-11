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

// Logs from this module will show up as "[COMPACT-HOOK] ...".
databend_common_tracing::register_module_tag!("[COMPACT-HOOK]");

use std::time::Instant;

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_meta_app::schema::is_materialized_view_engine;
use databend_common_sql::plans::MaintenanceTarget;
use databend_common_sql::plans::ReclusterPlan;
use log::info;

use crate::interpreters::Interpreter;
use crate::interpreters::ReclusterTableInterpreter;
use crate::interpreters::hook::table_hook_task::TableHookTarget;
use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::interpreters::hook::table_hook_task::keep_progress;
use crate::interpreters::hook::table_id_matches_target;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

impl TableHookTask {
    /// Run one round of recluster on a table that the compact stage has just compacted.
    ///
    /// `target` is the target the compact stage resolved. Tables without cluster keys are
    /// skipped.
    pub(super) async fn execute_recluster(&self, target: &TableHookTarget) -> Result<()> {
        let ctx = &self.ctx;
        let table = ctx
            .get_table(&target.catalog, &target.database, &target.table)
            .await?;
        if !table_id_matches_target(
            "compact",
            Some(target.table_id),
            table.get_id(),
            &target.catalog,
            &target.database,
            &target.table,
        ) {
            return Ok(());
        }
        if table.cluster_key_meta().is_none() {
            return Ok(());
        }

        let recluster_start = Instant::now();
        let recluster_result: Result<()> = keep_progress(ctx, async {
            // evict the table from cache
            ctx.evict_table_from_cache(&target.catalog, &target.database, &target.table)?;
            // Spill is disabled to keep this synchronous hook fast (spilling adds
            // disk IO). Admission must then keep tasks inside the memory budget.
            // TODO: remove once the async hook is enabled.
            ctx.set_enable_sort_spill(false);
            let maintenance_target = if is_materialized_view_engine(table.engine()) {
                MaintenanceTarget::MaterializedView {
                    table_id: table.get_id(),
                }
            } else {
                MaintenanceTarget::Table
            };
            let recluster = ReclusterPlan {
                catalog: target.catalog.clone(),
                database: target.database.clone(),
                table: target.table.clone(),
                target: maintenance_target,
                expected_table_id: Some(target.table_id),
                limit: Some(ctx.get_settings().get_auto_compaction_segments_limit()? as usize),
                selection: None,
                is_final: false,
            };
            let allow_segment_claims = self.lock_opt != LockTableOption::NoLock;
            let recluster_interpreter = ReclusterTableInterpreter::try_create(
                ctx.clone(),
                recluster,
                allow_segment_claims,
            )?;
            // Recluster will be done in `ReclusterTableInterpreter::execute2` directly,
            // we do not need to use `PipelineCompleteExecutor` to execute it.
            let build_res = recluster_interpreter.execute2().await?;
            debug_assert!(build_res.main_pipeline.is_empty());
            Ok(())
        })
        .await;
        info!(
            event = "compact_hook.stage_finished",
            stage = "recluster",
            catalog = target.catalog.as_str(),
            database = target.database.as_str(),
            table = target.table.as_str(),
            elapsed_ms = recluster_start.elapsed().as_millis() as u64,
            success = recluster_result.is_ok(),
            error :? = recluster_result.as_ref().err();
            "Compact hook stage finished"
        );
        recluster_result
    }
}
