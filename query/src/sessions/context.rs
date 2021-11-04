// Copyright 2020 Datafuse Labs.
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
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;
use std::time::Duration;

use common_base::tokio::task::JoinHandle;
use common_base::BlockingWait;
use common_base::ProgressCallback;
use common_base::ProgressValues;
use common_base::Runtime;
use common_base::TrySpawn;
use common_context::TableIOContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::NodeInfo;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::AbortStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::impls::DatabaseCatalog;
use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::common::ContextDalBuilder;
use crate::sessions::context_shared::DatabendQueryContextShared;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;

pub struct DatabendQueryContext {
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Part>>>,
    version: String,
    shared: Arc<DatabendQueryContextShared>,
}

pub type DatabendQueryContextRef = Arc<DatabendQueryContext>;

impl DatabendQueryContext {
    pub fn new(other: DatabendQueryContextRef) -> DatabendQueryContextRef {
        DatabendQueryContext::from_shared(other.shared.clone())
    }

    pub fn from_shared(shared: Arc<DatabendQueryContextShared>) -> DatabendQueryContextRef {
        shared.increment_ref_count();

        log::info!("Create DatabendQueryContext");

        Arc::new(DatabendQueryContext {
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!(
                "DatabendQuery v-{}",
                *crate::configs::DATABEND_COMMIT_VERSION
            ),
            shared,
        })
    }

    /// Set progress callback to context.
    /// By default, it is called for leaf sources, after each block
    /// Note that the callback can be called from different threads.
    pub fn progress_callback(&self) -> Result<ProgressCallback> {
        let current_progress = self.shared.progress.clone();
        Ok(Box::new(move |value: &ProgressValues| {
            current_progress.incr(value);
        }))
    }

    pub fn get_progress_value(&self) -> ProgressValues {
        self.shared.progress.as_ref().get_values()
    }

    pub fn get_and_reset_progress_value(&self) -> ProgressValues {
        self.shared.progress.as_ref().get_and_reset()
    }

    // Some table can estimate the approx total rows, such as NumbersTable
    pub fn add_total_rows_approx(&self, total_rows: usize) {
        self.shared
            .progress
            .as_ref()
            .add_total_rows_approx(total_rows);
    }

    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    pub fn try_get_partitions(&self, num: usize) -> Result<Partitions> {
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

    pub fn get_cluster(&self) -> ClusterRef {
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
    pub fn get_table(&self, database: &str, table: &str) -> Result<Arc<dyn Table>> {
        self.shared.get_table(database, table)
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

        let t = if plan.tbl_args.is_none() {
            catalog.build_table(&plan.table_info)?
        } else {
            catalog
                .get_table_function(&plan.table_info.name, plan.tbl_args.clone())?
                .as_table()
        };

        Ok(t)
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

    pub fn set_current_database(&self, new_database_name: String) -> Result<()> {
        let rt = self.shared.try_get_runtime()?;
        let cata = self.get_catalog();
        let db_name = new_database_name.clone();

        let res = (async move { cata.get_database(&db_name).await })
            .wait_in(&rt, Some(Duration::from_millis(5000)))?;

        match res {
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

    pub fn get_fuse_version(&self) -> String {
        self.version.clone()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    pub fn get_config(&self) -> Config {
        self.shared.conf.clone()
    }

    pub fn get_subquery_name(&self, _query: &PlanNode) -> String {
        let index = self.shared.subquery_index.fetch_add(1, Ordering::Relaxed);
        format!("_subquery_{}", index)
    }

    pub fn attach_query_str(&self, query: &str) {
        self.shared.attach_query_str(query);
    }

    pub fn attach_query_plan(&self, query_plan: &PlanNode) {
        self.shared.attach_query_plan(query_plan);
    }

    pub fn get_sessions_manager(self: &Arc<Self>) -> SessionManagerRef {
        self.shared.session.get_sessions_manager()
    }

    pub fn get_shared_runtime(&self) -> Result<Arc<Runtime>> {
        self.shared.try_get_runtime()
    }

    /// Build a TableIOContext for single node service.
    pub fn get_single_node_table_io_context(self: &Arc<Self>) -> Result<TableIOContext> {
        let nodes = vec![Arc::new(NodeInfo {
            id: self.get_cluster().local_id(),
            ..Default::default()
        })];

        let settings = self.get_settings();
        let max_threads = settings.get_max_threads()? as usize;

        Ok(TableIOContext::new(
            self.get_shared_runtime()?,
            Arc::new(ContextDalBuilder::new(self.get_config().storage)),
            max_threads,
            nodes,
            Some(self.clone()),
        ))
    }

    /// Build a TableIOContext that contains cluster information so that one using it could distributed data evenly in the cluster.
    pub fn get_cluster_table_io_context(self: &Arc<Self>) -> Result<TableIOContext> {
        let cluster = self.get_cluster();
        let nodes = cluster.get_nodes();
        let settings = self.get_settings();
        let max_threads = settings.get_max_threads()? as usize;

        Ok(TableIOContext::new(
            self.get_shared_runtime()?,
            Arc::new(ContextDalBuilder::new(self.get_config().storage)),
            max_threads,
            nodes,
            Some(self.clone()),
        ))
    }
}

impl TrySpawn for DatabendQueryContext {
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

impl std::fmt::Debug for DatabendQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_settings())
    }
}

impl Drop for DatabendQueryContext {
    fn drop(&mut self) {
        self.shared.destroy_context_ref()
    }
}

impl DatabendQueryContextShared {
    pub(in crate::sessions) fn destroy_context_ref(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Acquire);
            log::info!("Destroy DatabendQueryContext");
            self.session.destroy_context_shared();
        }
    }

    pub(in crate::sessions) fn increment_ref_count(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
