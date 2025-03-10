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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
// (TableName, file, file size)
pub type VacuumDropFileInfo = (String, String, u64);

// (drop_files, failed_tables)
pub type VacuumDropTablesResult = Result<(Option<Vec<VacuumDropFileInfo>>, HashSet<u64>)>;

#[async_trait::async_trait]
pub trait VacuumHandler: Sync + Send {
    async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>>;

    async fn do_vacuum2(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        respect_flash_back: bool,
    ) -> Result<Vec<String>>;

    async fn do_vacuum_drop_tables(
        &self,
        threads_nums: usize,
        tables: Vec<Arc<dyn Table>>,
        dry_run_limit: Option<usize>,
    ) -> VacuumDropTablesResult;

    async fn do_vacuum_temporary_files(
        &self,
        abort_checker: AbortChecker,
        temporary_dir: String,
        options: &VacuumTempOptions,
        vacuum_limit: usize,
    ) -> Result<usize>;
}

#[derive(Debug, Clone)]
pub enum VacuumTempOptions {
    // nodes, query_id
    QueryHook(Vec<usize>, String),
    VacuumCommand(Option<Duration>),
}

pub struct VacuumHandlerWrapper {
    handler: Box<dyn VacuumHandler>,
}

impl VacuumHandlerWrapper {
    pub fn new(handler: Box<dyn VacuumHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        self.handler
            .do_vacuum(fuse_table, ctx, retention_time, dry_run)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum2(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        respect_flash_back: bool,
    ) -> Result<Vec<String>> {
        self.handler
            .do_vacuum2(fuse_table, ctx, respect_flash_back)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum_drop_tables(
        &self,
        threads_nums: usize,
        tables: Vec<Arc<dyn Table>>,
        dry_run_limit: Option<usize>,
    ) -> VacuumDropTablesResult {
        self.handler
            .do_vacuum_drop_tables(threads_nums, tables, dry_run_limit)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum_temporary_files(
        &self,
        abort_checker: AbortChecker,
        temporary_dir: String,
        options: &VacuumTempOptions,
        vacuum_limit: usize,
    ) -> Result<usize> {
        self.handler
            .do_vacuum_temporary_files(abort_checker, temporary_dir, options, vacuum_limit)
            .await
    }
}

pub fn get_vacuum_handler() -> Arc<VacuumHandlerWrapper> {
    GlobalInstance::get()
}
