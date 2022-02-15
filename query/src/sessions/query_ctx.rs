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
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use common_base::tokio::task::JoinHandle;
use common_base::Progress;
use common_base::ProgressValues;
use common_base::TrySpawn;
use common_dal_context::DalContext;
use common_dal_context::DalMetrics;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::UserInfo;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::AbortStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use opendal::Operator;

use crate::catalogs::Catalog;
use crate::catalogs::DatabaseCatalog;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::servers::http::v1::HttpQueryHandle;
use crate::sessions::ProcessInfo;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionRef;
use crate::sessions::Settings;
use crate::storages::cache::CacheManager;
use crate::storages::Table;
use crate::users::UserApiProvider;

pub struct QueryContext {
    version: String,
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Part>>>,
    shared: Arc<QueryContextShared>,
}

impl QueryContext {
    pub fn create_from(other: Arc<QueryContext>) -> Arc<QueryContext> {
        QueryContext::create_from_shared(other.shared.clone())
    }

    pub fn create_from_shared(shared: Arc<QueryContextShared>) -> Arc<QueryContext> {
        shared.increment_ref_count();

        tracing::debug!("Create QueryContext");

        Arc::new(QueryContext {
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!(
                "DatabendQuery v-{}",
                *crate::configs::DATABEND_COMMIT_VERSION
            ),
            shared,
        })
    }

    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    pub fn build_table_from_source_plan(
        &self,
        plan: &ReadDataSourcePlan,
    ) -> Result<Arc<dyn Table>> {
        let catalog = self.get_catalog();

        if plan.tbl_args.is_none() {
            catalog.get_table_by_info(&plan.table_info)
        } else {
            Ok(catalog
                .get_table_function(&plan.table_info.name, plan.tbl_args.clone())?
                .as_table())
        }
    }

    pub fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }

    pub fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }

    pub fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }

    pub fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }

    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    pub fn try_get_partitions(&self, num: u64) -> Result<Partitions> {
        let mut partitions = vec![];
        for _ in 0..num {
            match self.partition_queue.write().pop_back() {
                None => break,
                Some(partition) => {
                    partitions.push(partition);
                }
            }
        }
        Ok(partitions)
    }

    // Update the context partition pool from the pipeline builder.
    pub fn try_set_partitions(&self, partitions: Partitions) -> Result<()> {
        for part in partitions {
            self.partition_queue.write().push_back(part);
        }
        Ok(())
    }

    pub fn try_get_statistics(&self) -> Result<Statistics> {
        let statistics = self.statistics.read();
        Ok((*statistics).clone())
    }

    pub fn try_set_statistics(&self, val: &Statistics) -> Result<()> {
        *self.statistics.write() = val.clone();
        Ok(())
    }

    pub fn attach_http_query(&self, handle: HttpQueryHandle) {
        self.shared.attach_http_query_handle(handle);
    }

    pub fn attach_query_str(&self, query: &str) {
        self.shared.attach_query_str(query);
    }

    pub fn attach_query_plan(&self, query_plan: &PlanNode) {
        self.shared.attach_query_plan(query_plan);
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    pub fn get_catalog(&self) -> Arc<DatabaseCatalog> {
        self.shared.get_catalog()
    }

    /// Fetch a Table by db and table name.
    ///
    /// It guaranteed to return a consistent result for multiple calls, in a same query.
    /// E.g.:
    /// ```sql
    /// SELECT * FROM (SELECT * FROM db.table_name) as subquery_1, (SELECT * FROM db.table_name) AS subquery_2
    /// ```
    pub async fn get_table(&self, database: &str, table: &str) -> Result<Arc<dyn Table>> {
        self.shared.get_table(database, table).await
    }

    pub fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }

    pub fn try_create_abortable(&self, input: SendableDataBlockStream) -> Result<AbortStream> {
        let (abort_handle, abort_stream) = AbortStream::try_create(input)?;
        self.shared.add_source_abort_handle(abort_handle);
        Ok(abort_stream)
    }

    pub fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }

    pub async fn set_current_database(&self, new_database_name: String) -> Result<()> {
        let tenant_id = self.get_tenant();
        let catalog = self.get_catalog();
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

    pub async fn set_current_tenant(&self, tenant: String) -> Result<()> {
        self.shared.set_current_tenant(tenant);

        Ok(())
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }

    pub fn get_fuse_version(&self) -> String {
        self.version.clone()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    pub fn get_config(&self) -> Config {
        self.shared.conf.clone()
    }

    pub fn get_tenant(&self) -> String {
        self.shared.get_tenant()
    }

    pub fn get_subquery_name(&self, _query: &PlanNode) -> String {
        let index = self.shared.subquery_index.fetch_add(1, Ordering::Relaxed);
        format!("_subquery_{}", index)
    }

    // Get user manager api.
    pub fn get_user_manager(self: &Arc<Self>) -> Arc<UserApiProvider> {
        self.shared.session.get_session_manager().get_user_manager()
    }

    // Get the current session.
    pub fn get_current_session(self: &Arc<Self>) -> Arc<Session> {
        self.shared.session.clone()
    }

    // Get one session by session id.
    pub fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<SessionRef> {
        self.shared
            .session
            .get_session_manager()
            .get_session_by_id(id)
    }

    // Get all the processes list info.
    pub fn get_processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        self.shared.session.get_session_manager().processes_info()
    }

    /// Get the data accessor metrics.
    pub fn get_dal_metrics(&self) -> DalMetrics {
        self.shared.dal_ctx.get_metrics()
    }

    /// Get the session running query.
    pub fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    /// Get the client socket address.
    pub fn get_client_address(&self) -> Option<SocketAddr> {
        self.shared.session.session_ctx.get_client_host()
    }

    /// Get the storage cache manager
    pub fn get_storage_cache_manager(&self) -> &CacheManager {
        self.shared.session.session_mgr.get_storage_cache_manager()
    }

    // Get the storage data accessor operator from the session manager.
    pub async fn get_storage_operator(&self) -> Result<Operator> {
        let operator = self.shared.session.get_storage_operator();
        Ok(operator.layer(self.shared.dal_ctx.clone()))
    }

    pub fn get_dal_context(&self) -> &DalContext {
        self.shared.dal_ctx.as_ref()
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

impl Drop for QueryContext {
    fn drop(&mut self) {
        self.shared.destroy_context_ref()
    }
}

impl QueryContextShared {
    pub(in crate::sessions) fn destroy_context_ref(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Acquire);
            tracing::debug!("Destroy QueryContext");
            self.session.destroy_context_shared();
        }
    }

    pub(in crate::sessions) fn increment_ref_count(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
