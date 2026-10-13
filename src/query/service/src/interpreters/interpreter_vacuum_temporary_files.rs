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
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::VacuumTemporaryFilesPlan;
use databend_common_storages_fuse::table_functions::vacuum_inactive_temp_tables;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use parking_lot::Mutex;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextQueryState;
use crate::sessions::TableContextTelemetry;

/// `set_status_info` logs on every call; update the processlist status at this cadence.
const STATUS_INTERVAL: Duration = Duration::from_secs(2);

pub struct VacuumTemporaryFilesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumTemporaryFilesPlan,
}

impl VacuumTemporaryFilesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumTemporaryFilesPlan) -> Result<Self> {
        Ok(VacuumTemporaryFilesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumTemporaryFilesInterpreter {
    fn name(&self) -> &str {
        "VacuumTemporaryFiles"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

            let handler = get_vacuum_handler();

            let temporary_files_prefix = self.ctx.query_tenant_spill_prefix();
            // Surface progress in `system.processes`: `scan_progress_read_rows` on every report,
            // `status` at most every few seconds because `set_status_info` also logs.
            let report_ctx = self.ctx.clone();
            let last_status = Mutex::new(Instant::now());
            let report = Arc::new(move |removed: usize| {
                report_ctx.get_scan_progress().set(&ProgressValues {
                    rows: removed,
                    bytes: 0,
                });
                let mut last = last_status.lock();
                if last.elapsed() >= STATUS_INTERVAL {
                    *last = Instant::now();
                    report_ctx
                        .set_status_info(&format!("[VACUUM] removed {removed} temporary files"));
                }
            });
            // The share of LIMIT the file vacuum used (not only what it knows it removed).
            let used_limit = handler
                .do_vacuum_temporary_files(
                    self.ctx.clone().get_abort_checker(),
                    temporary_files_prefix,
                    &VacuumTempOptions::VacuumCommand(self.plan.retain, report),
                    self.plan.limit.map(|x| x as usize).unwrap_or(usize::MAX),
                )
                .await?;

            let table_ctx: Arc<dyn TableContext> = self.ctx.clone();
            let session_limit = self
                .plan
                .limit
                .map(|limit| limit.saturating_sub(used_limit as u64));
            vacuum_inactive_temp_tables(&table_ctx, session_limit).await?;

            Ok(PipelineBuildResult::create())
        })
    }
}
