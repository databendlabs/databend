// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropTablesResult;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use databend_enterprise_vacuum_handler::VacuumHandler;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;

use crate::storages::fuse::do_vacuum;
use crate::storages::fuse::operations::vacuum_table_v2::do_vacuum2;
use crate::storages::fuse::operations::vacuum_temporary_files::do_vacuum_temporary_files;
use crate::storages::fuse::vacuum_drop_tables;
pub struct RealVacuumHandler {}

#[async_trait::async_trait]
impl VacuumHandler for RealVacuumHandler {
    async fn do_vacuum(
        &self,
        table: &dyn Table,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        do_vacuum(table, ctx, retention_time, dry_run).await
    }

    async fn do_vacuum2(
        &self,
        table: &dyn Table,
        ctx: Arc<dyn TableContext>,
        respect_flash_back: bool,
    ) -> Result<Vec<String>> {
        do_vacuum2(table, ctx, respect_flash_back).await
    }

    async fn do_vacuum_drop_tables(
        &self,
        threads_nums: usize,
        tables: Vec<Arc<dyn Table>>,
        dry_run_limit: Option<usize>,
    ) -> VacuumDropTablesResult {
        vacuum_drop_tables(threads_nums, tables, dry_run_limit).await
    }

    async fn do_vacuum_temporary_files(
        &self,
        abort_checker: AbortChecker,
        temporary_dir: String,
        options: &VacuumTempOptions,
        vacuum_limit: usize,
    ) -> Result<usize> {
        do_vacuum_temporary_files(abort_checker, temporary_dir, options, vacuum_limit).await
    }
}

impl RealVacuumHandler {
    pub fn init() -> Result<()> {
        let rm = RealVacuumHandler {};
        let wrapper = VacuumHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
