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

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_metatypes::MetaId;
use common_metatypes::MetaVersion;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::Statistics;
use common_progress::ProgressCallback;
use common_progress::ProgressValues;
use common_runtime::tokio::task::JoinHandle;
use common_streams::AbortStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::catalog::Catalog;
use crate::catalogs::utils::TableFunctionMeta;
use crate::catalogs::utils::TableMeta;
use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::datasources::DatabaseCatalog;
use crate::sessions::context_shared::DatafuseQueryContextShared;
use crate::sessions::SessionManagerRef;
use crate::sessions::Settings;

pub struct DatafuseQueryContext {
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Part>>>,
    version: String,
    shared: Arc<DatafuseQueryContextShared>,
}

pub type DatafuseQueryContextRef = Arc<DatafuseQueryContext>;

impl DatafuseQueryContext {
    pub fn new(other: DatafuseQueryContextRef) -> DatafuseQueryContextRef {
        DatafuseQueryContext::from_shared(other.shared.clone())
    }

    pub fn from_shared(shared: Arc<DatafuseQueryContextShared>) -> DatafuseQueryContextRef {
        shared.increment_ref_count();

        log::info!("Create DatafuseQueryContext");

        Arc::new(DatafuseQueryContext {
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!(
                "DatafuseQuery v-{}",
                *crate::configs::config::FUSE_COMMIT_VERSION
            ),
            shared,
        })
    }

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    pub fn execute_task<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.shared.try_get_runtime()?.spawn(task))
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

    pub fn try_get_cluster(&self) -> Result<ClusterRef> {
        self.shared.try_get_cluster()
    }

    pub fn get_catalog(&self) -> Arc<DatabaseCatalog> {
        self.shared.get_catalog()
    }

    pub fn get_table(&self, database: &str, table: &str) -> Result<Arc<TableMeta>> {
        self.get_catalog().get_table(database, table)
    }

    pub async fn get_table_by_id(
        &self,
        database: &str,
        table_id: MetaId,
        table_ver: Option<MetaVersion>,
    ) -> Result<Arc<TableMeta>> {
        self.get_catalog()
            .get_table_by_id(database, table_id, table_ver)
    }

    pub fn get_table_function(&self, function_name: &str) -> Result<Arc<TableFunctionMeta>> {
        self.get_catalog().get_table_function(function_name)
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
        match self.get_catalog().get_database(new_database_name.as_str()) {
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
}

impl std::fmt::Debug for DatafuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_settings())
    }
}

impl Drop for DatafuseQueryContext {
    fn drop(&mut self) {
        self.shared.destroy_context_ref()
    }
}

impl DatafuseQueryContextShared {
    pub(in crate::sessions) fn destroy_context_ref(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Acquire);
            log::info!("Destroy DatafuseQueryContext");
            self.session.destroy_context_shared();
        }
    }

    pub(in crate::sessions) fn increment_ref_count(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }
}
