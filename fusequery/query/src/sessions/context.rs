// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use common_datavalues::DataValue;
use common_planners::{Partition, Partitions, Statistics};
use uuid::Uuid;

use crate::clusters::{Cluster, ClusterRef};
use crate::datasources::{DataSource, IDataSource, ITable};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::sessions::Settings;

#[derive(Clone)]
pub struct FuseQueryContext {
    uuid: Arc<Mutex<String>>,
    settings: Settings,
    cluster: Arc<Mutex<ClusterRef>>,
    datasource: Arc<Mutex<dyn IDataSource>>,
    statistics: Arc<Mutex<Statistics>>,
    partition_queue: Arc<Mutex<VecDeque<Partition>>>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create() -> FuseQueryResult<FuseQueryContextRef> {
        let settings = Settings::create();
        let ctx = FuseQueryContext {
            uuid: Arc::new(Mutex::new(Uuid::new_v4().to_string())),
            settings,
            cluster: Arc::new(Mutex::new(Cluster::empty())),
            datasource: Arc::new(Mutex::new(DataSource::try_create()?)),
            statistics: Arc::new(Mutex::new(Statistics::default())),
            partition_queue: Arc::new(Mutex::new(VecDeque::new())),
        };

        ctx.initial_settings()?;
        Ok(Arc::new(ctx))
    }

    pub fn with_cluster(&self, cluster: ClusterRef) -> FuseQueryResult<FuseQueryContextRef> {
        *self.cluster.lock()? = cluster;
        Ok(Arc::new(self.clone()))
    }

    pub fn with_id(&self, uuid: &str) -> FuseQueryResult<FuseQueryContextRef> {
        *self.uuid.lock()? = uuid.to_string();
        Ok(Arc::new(self.clone()))
    }

    // ctx.reset will reset the necessary variables in the session
    pub fn reset(&self) -> FuseQueryResult<()> {
        self.statistics.lock()?.clear();
        self.partition_queue.lock()?.clear();
        Ok(())
    }

    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    pub fn try_get_partitions(&self, num: usize) -> FuseQueryResult<Partitions> {
        let mut partitions = vec![];
        for _ in 0..num {
            match self.partition_queue.lock()?.pop_back() {
                None => break,
                Some(partition) => {
                    partitions.push(partition);
                }
            }
        }
        Ok(partitions)
    }

    // Update the context partition pool from the pipeline builder.
    pub fn try_set_partitions(&self, partitions: Partitions) -> FuseQueryResult<()> {
        for part in partitions {
            self.partition_queue.lock()?.push_back(part);
        }
        Ok(())
    }

    pub fn try_get_statistics(&self) -> FuseQueryResult<Statistics> {
        let statistics = self.statistics.lock()?;
        Ok(Statistics {
            read_rows: statistics.read_rows,
            read_bytes: statistics.read_bytes,
        })
    }

    pub fn try_set_statistics(&self, val: &Statistics) -> FuseQueryResult<()> {
        *self.statistics.lock()? = val.clone();
        Ok(())
    }

    pub fn try_get_cluster(&self) -> FuseQueryResult<ClusterRef> {
        let cluster = self.cluster.lock()?;
        Ok(cluster.clone())
    }

    pub fn get_datasource(&self) -> Arc<Mutex<dyn IDataSource>> {
        self.datasource.clone()
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.datasource.lock()?.get_table(db_name, table_name)
    }

    pub fn get_settings(&self) -> FuseQueryResult<Vec<DataValue>> {
        self.settings.get_settings()
    }

    pub fn get_id(&self) -> FuseQueryResult<String> {
        Ok(self.uuid.as_ref().lock()?.clone())
    }

    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("max_threads", u64, num_cpus::get() as u64, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string()),
        ("max_block_size", u64, 10000, "Maximum block size for reading".to_string()),
        ("default_db", String, "default".to_string(), "the default database for current session".to_string())
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
