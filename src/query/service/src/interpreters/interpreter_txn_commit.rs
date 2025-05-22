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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_metrics::storage::metrics_inc_copy_purge_files_cost_milliseconds;
use databend_common_metrics::storage::metrics_inc_copy_purge_files_counter;
use databend_common_storage::init_stage_operator;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_io::Files;
use databend_storages_common_session::TxnManagerRef;
use log::error;
use log::info;

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
    async fn execute2(&self) -> Result<PipelineBuildResult> {
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
        let catalog = ctx.get_default_catalog()?;

        let req = ctx.txn_mgr().lock().req();

        let update_summary = {
            let table_descriptions = req
                .update_table_metas
                .iter()
                .map(|(req, _)| (req.table_id, req.seq, req.new_table_meta.engine.clone()))
                .collect::<Vec<_>>();
            let stream_descriptions = req
                .update_stream_metas
                .iter()
                .map(|s| (s.stream_id, s.seq, "stream"))
                .collect::<Vec<_>>();
            (table_descriptions, stream_descriptions)
        };

        let mismatched_tids = {
            ctx.txn_mgr().lock().set_auto_commit();
            let ret = catalog.retryable_update_multi_table_meta(req).await;
            if let Err(ref e) = ret {
                // other errors may occur, especially the version mismatch of streams,
                // let's log it here for the convenience of diagnostics
                error!(
                    "Non-recoverable fault occurred during table metadata update: {}",
                    e
                );
            }
            ret?
        };

        match &mismatched_tids {
            Ok(_) => {
                info!(
                    "Transaction committed successfully, updated targets: {:?}",
                    update_summary
                );
            }
            Err(e) => {
                let err_msg = format!(
                    "Due to concurrent transactions, explicit transaction commit failed. Conflicting table IDs: {:?}",
                    e.iter().map(|(tid, _, _)| tid).collect::<Vec<_>>()
                );
                info!(
                    "Transaction commit failed due to concurrent modifications. Conflicting table IDs: {:?}",
                    e
                );
                return Err(ErrorCode::TableVersionMismatched(format!("{}", err_msg)));
            }
        }
        let need_purge_files = ctx.txn_mgr().lock().need_purge_files();
        for (stage_info, files) in need_purge_files {
            try_purge_files(ctx.clone(), &stage_info, &files).await;
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
