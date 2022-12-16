// Copyright 2021 Datafuse Labs.
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

use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::SystemTime;

use chrono_tz::Tz;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_base::runtime::TrySpawn;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Partitions;
use common_catalog::plan::StageTableInfo;
use common_catalog::table_context::StageAttachment;
use common_config::DATABEND_COMMIT_VERSION;
use common_datablocks::DataBlock;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_io::prelude::FormatSettings;
use common_meta_app::schema::TableInfo;
use common_meta_types::RoleInfo;
use common_meta_types::UserInfo;
use common_settings::Settings;
use common_storage::DataOperator;
use common_storage::StorageMetrics;
use common_storages_stage::StageTable;
use parking_lot::RwLock;
use tracing::debug;

use crate::api::DataExchangeManager;
use crate::auth::AuthMgr;
use crate::catalogs::Catalog;
use crate::clusters::Cluster;
use crate::pipelines::executor::PipelineExecutor;
use crate::servers::http::v1::HttpQueryHandle;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::ProcessInfo;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::TableContext;
use crate::storages::Table;

#[derive(Clone)]
pub struct QueryContext {
    version: String,
    partition_queue: Arc<RwLock<VecDeque<PartInfoPtr>>>,
    shared: Arc<QueryContextShared>,
    fragment_id: Arc<AtomicUsize>,
}

impl QueryContext {
    pub fn create_from(other: Arc<QueryContext>) -> Arc<QueryContext> {
        QueryContext::create_from_shared(other.shared.clone())
    }

    pub fn create_from_shared(shared: Arc<QueryContextShared>) -> Arc<QueryContext> {
        debug!("Create QueryContext");

        Arc::new(QueryContext {
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!("DatabendQuery {}", *DATABEND_COMMIT_VERSION),
            shared,
            fragment_id: Arc::new(AtomicUsize::new(0)),
        })
    }

    // Build fuse/system normal table by table info.
    fn build_table_by_table_info(
        &self,
        catalog_name: &str,
        table_info: &TableInfo,
        table_args: Option<Vec<DataValue>>,
    ) -> Result<Arc<dyn Table>> {
        let catalog = self.get_catalog(catalog_name)?;
        if table_args.is_none() {
            catalog.get_table_by_info(table_info)
        } else {
            Ok(catalog
                .get_table_function(&table_info.name, table_args)?
                .as_table())
        }
    }

    // Build external table by stage info, this is used in:
    // COPY INTO t1 FROM 's3://'
    // 's3://' here is a s3 external stage, and build it to the external table.
    fn build_external_by_table_info(
        &self,
        _catalog: &str,
        table_info: &StageTableInfo,
        _table_args: Option<Vec<DataValue>>,
    ) -> Result<Arc<dyn Table>> {
        StageTable::try_create(table_info.clone())
    }

    pub async fn set_current_database(&self, new_database_name: String) -> Result<()> {
        let tenant_id = self.get_tenant();
        let catalog = self.get_catalog(self.get_current_catalog().as_str())?;
        match catalog
            .get_database(tenant_id.as_str(), &new_database_name)
            .await
        {
            Ok(_) => self.shared.set_current_database(new_database_name),
            Err(_) => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Cannot USE '{}', because the '{}' doesn't exist",
                    new_database_name, new_database_name
                )));
            }
        };

        Ok(())
    }

    pub fn get_exchange_manager(&self) -> Arc<DataExchangeManager> {
        DataExchangeManager::instance()
    }

    pub fn attach_http_query(&self, handle: HttpQueryHandle) {
        self.shared.attach_http_query_handle(handle);
    }

    pub fn get_http_query(&self) -> Option<HttpQueryHandle> {
        self.shared.get_http_query()
    }

    pub fn get_auth_manager(&self) -> Arc<AuthMgr> {
        self.shared.get_auth_manager()
    }

    // Get the current session.
    pub fn get_current_session(&self) -> Arc<Session> {
        self.shared.session.clone()
    }

    // Get one session by session id.
    pub fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<Arc<Session>> {
        SessionManager::instance().get_session_by_id(id)
    }

    // Get session id by mysql connection id.
    pub fn get_id_by_mysql_conn_id(self: &Arc<Self>, conn_id: &Option<u32>) -> Option<String> {
        SessionManager::instance().get_id_by_mysql_conn_id(conn_id)
    }

    // Get all the processes list info.
    pub fn get_processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    /// Get the client socket address.
    pub fn get_client_address(&self) -> Option<SocketAddr> {
        self.shared.session.session_ctx.get_client_host()
    }

    pub fn get_affect(self: &Arc<Self>) -> Option<QueryAffect> {
        self.shared.get_affect()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        self.shared.get_data_metrics()
    }

    pub fn set_affect(self: &Arc<Self>, affect: QueryAffect) {
        self.shared.set_affect(affect)
    }

    pub fn set_executor(&self, weak_ptr: Weak<PipelineExecutor>) {
        self.shared.set_executor(weak_ptr)
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        self.shared.attach_stage(attachment);
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.shared.created_time
    }
}

#[async_trait::async_trait]
impl TableContext for QueryContext {
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        match &plan.source_info {
            DataSourceInfo::TableSource(table_info) => {
                self.build_table_by_table_info(&plan.catalog, table_info, plan.tbl_args.clone())
            }
            DataSourceInfo::StageSource(stage_info) => {
                self.build_external_by_table_info(&plan.catalog, stage_info, plan.tbl_args.clone())
            }
        }
    }
    fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }
    fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }
    fn get_write_progress(&self) -> Arc<Progress> {
        self.shared.write_progress.clone()
    }
    fn get_write_progress_value(&self) -> ProgressValues {
        self.shared.write_progress.as_ref().get_values()
    }
    fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }
    fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }

    fn try_get_part(&self) -> Option<PartInfoPtr> {
        self.partition_queue.write().pop_front()
    }

    // Update the context partition pool from the pipeline builder.
    fn try_set_partitions(&self, partitions: Partitions) -> Result<()> {
        let mut partition_queue = self.partition_queue.write();

        partition_queue.clear();
        for part in partitions.partitions {
            partition_queue.push_back(part);
        }
        Ok(())
    }
    fn attach_query_str(&self, kind: String, query: &str) {
        self.shared.attach_query_str(kind, query);
    }

    fn get_fragment_id(&self) -> usize {
        self.fragment_id.fetch_add(1, Ordering::Release)
    }

    fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_catalog(catalog_name.as_ref())
    }

    fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }
    fn get_current_catalog(&self) -> String {
        self.shared.get_current_catalog()
    }

    fn get_aborting(&self) -> Arc<AtomicBool> {
        self.shared.get_aborting()
    }

    fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }
    fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }
    fn get_current_role(&self) -> Option<RoleInfo> {
        self.shared.get_current_role()
    }
    fn get_fuse_version(&self) -> String {
        self.version.clone()
    }
    fn get_changed_settings(&self) -> Arc<Settings> {
        self.shared.get_changed_settings()
    }
    fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        self.shared.apply_changed_settings(changed_settings)
    }

    fn get_format_settings(&self) -> Result<FormatSettings> {
        self.shared.session.get_format_settings()
    }
    fn get_tenant(&self) -> String {
        self.shared.get_tenant()
    }

    /// Get the session running query.
    fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    fn get_query_kind(&self) -> String {
        self.shared.get_query_kind()
    }

    // Get the storage data accessor operator from the session manager.
    fn get_data_operator(&self) -> Result<DataOperator> {
        Ok(self.shared.data_operator.clone())
    }
    fn push_precommit_block(&self, block: DataBlock) {
        self.shared.push_precommit_block(block)
    }
    fn consume_precommit_blocks(&self) -> Vec<DataBlock> {
        self.shared.consume_precommit_blocks()
    }
    fn try_get_function_context(&self) -> Result<FunctionContext> {
        let tz = self.get_settings().get_timezone()?;
        let tz = tz.parse::<Tz>().map_err(|_| {
            ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
        })?;
        Ok(FunctionContext { tz })
    }
    fn get_connection_id(&self) -> String {
        self.shared.get_connection_id()
    }
    fn get_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    /// Fetch a Table by db and table name.
    ///
    /// It guaranteed to return a consistent result for multiple calls, in a same query.
    /// E.g.:
    /// ```sql
    /// SELECT * FROM (SELECT * FROM db.table_name) as subquery_1, (SELECT * FROM db.table_name) AS subquery_2
    /// ```
    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        self.shared.get_table(catalog, database, table).await
    }

    // Get all the processes list info.
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    // Get Stage Attachment.
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.shared.get_stage_attachment()
    }
}

impl TrySpawn for QueryContext {
    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.shared.try_get_runtime()?.spawn(task))
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_current_user())
    }
}
