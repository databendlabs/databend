// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::VecDeque;
use std::sync::Arc;

use common_exception::Result;
use common_datavalues::DataValue;
use common_infallible::RwLock;
use common_planners::Partition;
use common_planners::Partitions;
use common_planners::Statistics;
use common_progress::ProgressCallback;
use uuid::Uuid;

use crate::clusters::Cluster;
use crate::clusters::ClusterRef;
use crate::datasources::DataSource;
use crate::datasources::IDataSource;
use crate::datasources::ITable;
use crate::datasources::ITableFunction;
use crate::sessions::Settings;
use common_exception::ErrorCodes;

#[derive(Clone)]
pub struct FuseQueryContext {
    uuid: Arc<RwLock<String>>,
    settings: Settings,
    cluster: Arc<RwLock<ClusterRef>>,
    datasource: Arc<dyn IDataSource>,
    statistics: Arc<RwLock<Statistics>>,
    partition_queue: Arc<RwLock<VecDeque<Partition>>>,
    progress_callback: Arc<RwLock<Option<ProgressCallback>>>,
    current_database: Arc<RwLock<String>>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create() -> Result<FuseQueryContextRef> {
        let settings = Settings::create();
        let ctx = FuseQueryContext {
            uuid: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            settings,
            cluster: Arc::new(RwLock::new(Cluster::empty())),
            datasource: Arc::new(DataSource::try_create()?),
            statistics: Arc::new(RwLock::new(Statistics::default())),
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            progress_callback: Arc::new(RwLock::new(None)),
            current_database: Arc::new(RwLock::new(String::from("default"))),
        };

        ctx.initial_settings()?;
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
        self.statistics.write().clear();
        self.partition_queue.write().clear();
        Ok(())
    }

    /// Set progress callback to context.
    /// By default, it is called for leaf sources, after each block
    /// Note that the callback can be called from different threads.
    pub fn set_progress_callback(&self, callback: ProgressCallback) {
        *self.progress_callback.write() = Some(callback);
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
            read_bytes: statistics.read_bytes
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

    pub fn get_table_function(&self, function_name: &str) -> Result<Arc<dyn ITableFunction>> {
        self.datasource.get_table_function(function_name)
    }

    pub fn get_settings(&self) -> Result<Vec<DataValue>> {
        self.settings.get_settings().map_err(ErrorCodes::from_anyhow)
    }

    pub fn get_id(&self) -> Result<String> {
        Ok(self.uuid.as_ref().read().clone())
    }


    pub fn get_current_database(&self) -> String {
        self.current_database.as_ref().read().clone()
    }

    pub fn set_current_database(&self, new_database_name: String) -> Result<()> {
        self.datasource.get_database(new_database_name.as_str())
            .map(|_| -> (){ *self.current_database.write() = new_database_name.to_string(); })
            .map_err(|_| ErrorCodes::UnknownDatabase(format!("Database {}  doesn't exist.", new_database_name)))
    }

    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("max_threads", u64, num_cpus::get() as u64, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string()),
        ("max_block_size", u64, 10000, "Maximum block size for reading".to_string())
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
