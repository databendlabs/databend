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

// Logs from this module will show up as "[REFRESH-HOOK] ...".
databend_common_tracing::register_module_tag!("[REFRESH-HOOK]");

use std::sync::Arc;

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::always_callback;
use databend_common_storages_fuse::FuseTable;
use log::info;

use crate::interpreters::hook::table_hook_task::TableHookTask;
use crate::interpreters::hook::table_id_matches_target;
use crate::interpreters::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextCachePolicy;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;

impl TableHookTask {
    pub(super) async fn execute_refresh(&self) -> Result<()> {
        // Maintenance after the write: `CachePolicy::MAINTENANCE` for its reads, on the context
        // of the write, whose own reads are done by now.
        self.ctx
            .set_cache_policy(self.ctx.cache_policy().at_most(CachePolicy::MAINTENANCE));
        info!("Table hook starting refresh job");
        self.do_refresh(self.ctx.clone()).await
    }

    async fn do_refresh(&self, ctx: Arc<QueryContext>) -> Result<()> {
        let Some(desc) = self.target.resolve(&ctx, "refresh").await? else {
            return Ok(());
        };

        // Target resolution refreshed the table cache and checked its ID. Reuse that
        // current table to filter hints against the post-maintenance snapshot.
        let table = ctx
            .get_table(&desc.catalog, &desc.database, &desc.table)
            .await?;
        if !table_id_matches_target(
            "refresh",
            Some(desc.table_id),
            table.get_id(),
            &desc.catalog,
            &desc.database,
            &desc.table,
        ) {
            return Ok(());
        }
        let names = table
            .get_table_info()
            .meta
            .indexes
            .values()
            .filter(|index| !index.sync_creation)
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        if names.is_empty() {
            return Ok(());
        }
        let mut pipeline = Pipeline::create();
        FuseTable::try_from_table(table.as_ref())?
            .do_refresh_indexes(
                ctx.clone(),
                &names,
                Some(ctx.written_segment_locations().list()),
                &mut pipeline,
            )
            .await?;
        if pipeline.is_empty() {
            return Ok(());
        }

        // The shared builder has per-index worker budgets; automatic maintenance uses
        // one total pipeline budget rather than multiplying it by the number of indexes.
        pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
        let query_ctx = ctx.clone();
        pipeline.set_on_finished(always_callback(move |_info: &ExecutionInfo| {
            hook_clear_m_cte_temp_table(&query_ctx)?;
            hook_vacuum_temp_files(&query_ctx)?;
            hook_disk_temp_dir(&query_ctx)?;
            Ok(())
        }));
        let executor = PipelineCompleteExecutor::try_create(
            pipeline,
            ExecutorSettings::try_create(ctx.clone())?,
        )?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute().await
    }
}
