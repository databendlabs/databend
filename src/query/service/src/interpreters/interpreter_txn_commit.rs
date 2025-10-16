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
use std::time::Instant;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::StageInfo;
use databend_common_metrics::storage::metrics_inc_copy_purge_files_cost_milliseconds;
use databend_common_metrics::storage::metrics_inc_copy_purge_files_counter;
use databend_common_storage::init_stage_operator;
use databend_common_storages_fuse::commit_with_backoff;
use databend_common_storages_fuse::operations::vacuum_tables_from_info;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
use databend_storages_common_io::Files;
use databend_storages_common_session::TxnManagerRef;
use log::error;
use log::info;
use log::warn;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CommitInterpreter {
    ctx: Arc<QueryContext>,
}

impl CommitInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(Self { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for CommitInterpreter {
    fn name(&self) -> &str {
        "CommitInterpreter"
    }

    fn is_txn_command(&self) -> bool {
        true
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        execute_commit_statement(self.ctx.clone()).await?;
        Ok(PipelineBuildResult::create())
    }
}

struct ClearTxnManagerGuard(TxnManagerRef);

impl Drop for ClearTxnManagerGuard {
    fn drop(&mut self) {
        self.0.lock().clear();
    }
}

pub async fn execute_commit_statement(ctx: Arc<dyn TableContext>) -> Result<()> {
    // After commit statement, current session should be in auto commit mode, no matter update meta success or not.
    // Use this guard to clear txn manager before return.
    let _guard = ClearTxnManagerGuard(ctx.txn_mgr().clone());
    let is_active = ctx.txn_mgr().lock().is_active();
    if is_active {
        ctx.txn_mgr().lock().set_auto_commit();
        let req = ctx.txn_mgr().lock().req();
        commit_with_backoff(ctx.clone(), req).await?;
        let need_purge_files = ctx.txn_mgr().lock().need_purge_files();
        for (stage_info, files) in need_purge_files {
            try_purge_files(ctx.clone(), &stage_info, &files).await;
        }

        let tables_need_purge = { ctx.txn_mgr().lock().table_need_purge() };

        if !tables_need_purge.is_empty() {
            if LicenseManagerSwitch::instance()
                .check_enterprise_enabled(ctx.get_license_key(), Vacuum)
                .is_ok()
            {
                let handler: Arc<VacuumHandlerWrapper> = GlobalInstance::get();
                let num_tables = tables_need_purge.len();
                info!("Vacuuming {num_tables} tables after transaction commit");
                if let Err(e) =
                    vacuum_tables_from_info(tables_need_purge, ctx.clone(), handler).await
                {
                    warn!( "Failed to vacuum tables after transaction commit (best-effort operation): {e}");
                } else {
                    info!( "{num_tables} tables vacuumed after transaction commit in a best-effort manner" );
                }
            } else {
                warn!("EE feature is not enabled, vacuum after transaction commit is skipped");
            }
        }
    }
    Ok(())
}

#[async_backtrace::framed]
async fn try_purge_files(ctx: Arc<dyn TableContext>, stage_info: &StageInfo, files: &[String]) {
    let start = Instant::now();
    let op = init_stage_operator(stage_info);

    match op {
        Ok(op) => {
            let file_op = Files::create(ctx, op);
            if let Err(e) = file_op.remove_file_in_batch(files).await {
                error!("Failed to delete files: {:?}, error: {}", files, e);
            }
        }
        Err(e) => {
            error!("Failed to initialize stage operator, error: {}", e);
        }
    }

    let elapsed = start.elapsed();
    info!("Purged {} files, operation took {:?}", files.len(), elapsed);

    // Perf.
    {
        metrics_inc_copy_purge_files_counter(files.len() as u32);
        metrics_inc_copy_purge_files_cost_milliseconds(elapsed.as_millis() as u32);
    }
}
