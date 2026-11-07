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
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
use databend_storages_common_io::Files;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_session::TxnTableHook;
use log::error;
use log::info;
use log::warn;

use crate::interpreters::Interpreter;
use crate::interpreters::hook::execute_deferred_table_hooks;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextSession;
use crate::sessions::TableContextTableAccess;

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
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            execute_commit_statement(self.ctx.clone()).await?;
            Ok(PipelineBuildResult::create())
        })
    }
}

struct ClearTxnManagerGuard(TxnManagerRef);

impl Drop for ClearTxnManagerGuard {
    fn drop(&mut self) {
        self.0.lock().clear();
    }
}

/// Automatic maintenance for a committed transaction. Dispatch only after releasing any
/// enclosing lifecycle locks; each maintenance operation acquires its own locks.
#[must_use]
pub struct PostCommitHooks {
    ctx: Arc<QueryContext>,
    table_hooks: Vec<TxnTableHook>,
    txn_id: String,
}

impl PostCommitHooks {
    pub async fn execute(self) {
        // Maintenance builds and executes SQL pipelines. Keep its large future off the
        // caller's stack, including internal transactions nested in materialized-view refresh.
        Box::pin(execute_deferred_table_hooks(
            self.ctx,
            self.table_hooks,
            &self.txn_id,
        ))
        .await;
    }
}

pub async fn execute_commit_statement(ctx: Arc<QueryContext>) -> Result<()> {
    if let Some(hooks) = commit_transaction(ctx).await? {
        hooks.execute().await;
    }
    Ok(())
}

/// Commit and clear the transaction, returning automatic maintenance separately so callers
/// with an enclosing lifecycle lock can release it before dispatching the hooks.
pub async fn commit_transaction(ctx: Arc<QueryContext>) -> Result<Option<PostCommitHooks>> {
    // After commit statement, current session should be in auto commit mode, no matter update meta success or not.
    // Use this guard to clear txn manager before return.
    let guard = ClearTxnManagerGuard(ctx.txn_mgr());
    let is_active = ctx.txn_mgr().lock().is_active();
    if is_active {
        ctx.txn_mgr().lock().set_auto_commit();
        let (tenant, req) = {
            let txn_mgr = ctx.txn_mgr();
            let txn_mgr = txn_mgr.lock();
            (
                txn_mgr.tenant().unwrap_or_else(|| ctx.get_tenant()),
                txn_mgr.req(),
            )
        };
        commit_with_backoff(ctx.clone(), &tenant, req).await?;
        let (need_purge_files, tables_need_purge, table_hooks, txn_id) = {
            let txn_mgr = ctx.txn_mgr();
            let mut txn_mgr = txn_mgr.lock();
            (
                txn_mgr.need_purge_files(),
                txn_mgr.table_need_purge(),
                txn_mgr.take_table_hooks(),
                txn_mgr.txn_id().to_string(),
            )
        };
        // set_auto_commit only changed the state. Clear the actual buffer before any
        // post-commit work, and finish the guard before handing tasks to another context.
        drop(guard);
        for (stage_info, files) in need_purge_files {
            try_purge_files(ctx.clone(), &stage_info, &files).await;
        }

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
                    warn!(
                        "Failed to vacuum tables after transaction commit (best-effort operation): {e}"
                    );
                } else {
                    info!(
                        "{num_tables} tables vacuumed after transaction commit in a best-effort manner"
                    );
                }
            } else {
                warn!("EE feature is not enabled, vacuum after transaction commit is skipped");
            }
        }
        return Ok(Some(PostCommitHooks {
            ctx,
            table_hooks,
            txn_id,
        }));
    }
    Ok(None)
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
