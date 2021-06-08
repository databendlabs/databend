// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_infallible::RwLock;
use common_planners::Partition;
use common_planners::Partitions;
use common_planners::Statistics;
use common_progress::Progress;
use common_progress::ProgressCallback;
use common_progress::ProgressValues;
use common_runtime::Runtime;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::clusters::Cluster;
use crate::clusters::ClusterRef;
use crate::datasources::DataSource;
use crate::datasources::IDataSource;
use crate::datasources::ITable;
use crate::datasources::ITableFunction;
use crate::sessions::Settings;

#[derive(Clone)]
pub struct FuseQueryContext {
    uuid: Arc<RwLock<String>>,
    settings: Settings,
    cluster: Arc<RwLock<ClusterRef>>,
    datasource: Arc<dyn IDataSource>,
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Partition>>>,
    current_database: Arc<RwLock<String>>,
    progress: Arc<Progress>,
    runtime: Arc<RwLock<Runtime>>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create() -> Result<FuseQueryContextRef> {
        let cpus = num_cpus::get();
        let settings = Settings::create();
        let ctx = FuseQueryContext {
            uuid: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            settings,
            cluster: Arc::new(RwLock::new(Cluster::empty())),
            datasource: Arc::new(DataSource::try_create()?),
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            current_database: Arc::new(RwLock::new(String::from("default"))),
            progress: Arc::new(Progress::create()),
            runtime: Arc::new(RwLock::new(Runtime::with_worker_threads(cpus)?)),
        };
        // Default settings.
        ctx.initial_settings()?;
        // Customize settings.
        ctx.settings.try_set_u64("max_threads", cpus as u64, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string())?;

        Ok(Arc::new(ctx))
    }

    pub fn with_cluster(&self, cluster: ClusterRef) -> Result<FuseQueryContextRef> {
        *self.cluster.write() = cluster;
        Ok(Arc::new(self.clone()))
    }

    pub fn with_id(&self, uuid: &str) -> Result<FuseQueryContextRef> {
        *self.uuid.write() = uuid.to_string();
        Ok(Arc::new(self.clone()))
    }

    /// ctx.reset will reset the necessary variables in the session
    pub fn reset(&self) -> Result<()> {
        self.progress.reset();
        self.statistics.write().clear();
        self.partition_queue.write().clear();
        Ok(())
    }

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    pub fn execute_task<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.runtime.read().spawn(task)
    }

    /// Set progress callback to context.
    /// By default, it is called for leaf sources, after each block
    /// Note that the callback can be called from different threads.
    pub fn progress_callback(&self) -> Result<ProgressCallback> {
        let current_progress = self.progress.clone();
        Ok(Box::new(move |value: &ProgressValues| {
            current_progress.incr(value);
        }))
    }

    pub fn get_progress_value(&self) -> ProgressValues {
        self.progress.as_ref().get_values()
    }

    pub fn get_and_reset_progress_value(&self) -> ProgressValues {
        self.progress.as_ref().get_and_reset()
    }

    // Some table can estimate the approx total rows, such as NumbersTable
    pub fn add_total_rows_approx(&self, total_rows: usize) {
        self.progress.as_ref().add_total_rows_approx(total_rows);
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
        Ok(Statistics {
            read_rows: statistics.read_rows,
            read_bytes: statistics.read_bytes,
        })
    }

    pub fn try_set_statistics(&self, val: &Statistics) -> Result<()> {
        *self.statistics.write() = val.clone();
        Ok(())
    }

    pub fn try_get_cluster(&self) -> Result<ClusterRef> {
        let cluster = self.cluster.read();
        Ok(cluster.clone())
    }

    pub fn get_datasource(&self) -> Arc<dyn IDataSource> {
        self.datasource.clone()
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn ITable>> {
        self.datasource.get_table(db_name, table_name)
    }

    // This is an adhoc solution for the metadata syncing problem, far from elegant. let's tweak this later.
    //
    // The reason of not extending IDataSource::get_table (e.g. by adding a remote_hint parameter):
    // Implementation of fetching remote table involves async operations which is not
    // straight forward (but not infeasible) to do in a non-async method.
    pub async fn get_remote_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn ITable>> {
        self.datasource.get_remote_table(db_name, table_name).await
    }

    pub fn get_table_function(&self, function_name: &str) -> Result<Arc<dyn ITableFunction>> {
        self.datasource.get_table_function(function_name)
    }

    pub fn get_settings(&self) -> Result<Vec<DataValue>> {
        self.settings.get_settings()
    }

    pub fn get_id(&self) -> Result<String> {
        Ok(self.uuid.as_ref().read().clone())
    }

    pub fn get_current_database(&self) -> String {
        self.current_database.as_ref().read().clone()
    }

    pub fn set_current_database(&self, new_database_name: String) -> Result<()> {
        self.datasource
            .get_database(new_database_name.as_str())
            .map(|_| {
                *self.current_database.write() = new_database_name.to_string();
            })
            .map_err(|_| {
                ErrorCodes::UnknownDatabase(format!(
                    "Database {}  doesn't exist.",
                    new_database_name
                ))
            })
    }

    pub fn get_max_threads(&self) -> Result<u64> {
        self.settings.try_get_u64("max_threads")
    }

    pub fn set_max_threads(&self, threads: u64) -> Result<()> {
        *self.runtime.write() = Runtime::with_worker_threads(threads as usize)?;
        self.settings.try_update_u64("max_threads", threads)
    }

    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("max_block_size", u64, 10000, "Maximum block size for reading".to_string()),
        ("flight_client_timeout", u64, 60, "Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds".to_string()),
        ("min_distributed_rows", u64, 100000000, "Minimum distributed read rows. In cluster mode, when read rows exceeds this value, the local table converted to distributed query.".to_string()),
        ("min_distributed_bytes", u64, 500 * 1024 * 1024, "Minimum distributed read bytes. In cluster mode, when read bytes exceeds this value, the local table converted to distributed query.".to_string())
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
