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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;

use crate::interpreters::common::log_query_finished;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::sessions::QueryContext;

fn run_hooks(query_ctx: Arc<QueryContext>) -> Result<()> {
    hook_clear_m_cte_temp_table(&query_ctx)?;
    hook_vacuum_temp_files(&query_ctx)?;
    hook_disk_temp_dir(&query_ctx)
}

/// Controls which post-execution actions are performed when a pipeline finishes.
///
/// Use [`QueryFinishHooks::top_level`] for normal user-facing queries,
/// [`QueryFinishHooks::nested_with_hooks`] for internal sub-executions that may
/// create temporary artifacts (e.g. EXPLAIN ANALYZE, EXPLAIN PERF inner pipelines),
/// and [`QueryFinishHooks::nested`] for lightweight internal pipelines that don't
/// need cleanup (e.g. recursive CTE inner pipeline).
pub struct QueryFinishHooks {
    /// Collect pipeline execution profiles into the query context.
    pub collect_profiles: bool,
    /// Run post-query cleanup hooks (CTE temp tables, spill files, disk temp dirs).
    pub run_hooks: bool,
    /// Emit the query-finish log, metrics, and profile JSON.
    pub log_finished: bool,
}

impl QueryFinishHooks {
    /// All three actions enabled. Use for top-level user queries.
    pub fn top_level() -> Self {
        Self {
            collect_profiles: true,
            run_hooks: true,
            log_finished: true,
        }
    }

    /// Profiles only — no hooks, no logging. Use for nested/internal pipeline
    /// executions where the outer query owns the lifecycle (e.g. recursive CTE
    /// inner pipeline).
    pub fn nested() -> Self {
        Self {
            collect_profiles: true,
            run_hooks: false,
            log_finished: false,
        }
    }

    /// Profiles and cleanup hooks, but no logging. Use for nested pipelines
    /// that may create temporary artifacts (spill files, CTE temp tables) and
    /// need cleanup even on failure, while the outer query owns the log
    /// lifecycle (e.g. EXPLAIN ANALYZE, EXPLAIN PERF inner pipelines).
    pub fn nested_with_hooks() -> Self {
        Self {
            collect_profiles: true,
            run_hooks: true,
            log_finished: false,
        }
    }

    /// Convert into a closure suitable for [`Pipeline::set_on_finished`].
    pub fn into_callback(
        self,
        ctx: Arc<QueryContext>,
    ) -> impl Fn(&ExecutionInfo) -> Result<()> + Send + Sync + 'static {
        move |info: &ExecutionInfo| {
            if self.collect_profiles {
                ctx.add_query_profiles(&info.profiling);
            }
            let hooks_res = if self.run_hooks {
                run_hooks(ctx.clone())
            } else {
                Ok(())
            };
            if self.log_finished {
                log_query_finished(&ctx, info.res.clone().err());
            }
            info.res.clone().and(hooks_res)
        }
    }
}
