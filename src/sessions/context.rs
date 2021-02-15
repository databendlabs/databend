// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::datasources::{DataSource, IDataSource, ITable, Partition, Partitions, Statistics};
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::sessions::Settings;

pub struct FuseQueryContext {
    id: String,
    settings: Settings,
    datasource: Arc<Mutex<dyn IDataSource>>,
    statistics: Mutex<Statistics>,
    partition_queue: Mutex<VecDeque<Partition>>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create_ctx() -> FuseQueryResult<Arc<Self>> {
        let settings = Settings::create();
        let ctx = FuseQueryContext {
            id: Uuid::new_v4().to_string(),
            settings,
            datasource: Arc::new(Mutex::new(DataSource::try_create()?)),
            statistics: Mutex::new(Statistics::default()),
            partition_queue: Mutex::new(VecDeque::new()),
        };

        ctx.initial_settings()?;
        Ok(Arc::new(ctx))
    }

    // ctx.reset will reset the necessary variables in the session
    pub fn reset(&self) -> FuseQueryResult<()> {
        self.statistics.lock()?.clear();
        self.partition_queue.lock()?.clear();
        Ok(())
    }

    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    pub fn try_fetch_partitions(&self, num: usize) -> FuseQueryResult<Partitions> {
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
    pub fn try_update_partitions(&self, partitions: Partitions) -> FuseQueryResult<()> {
        for part in partitions {
            self.partition_queue.lock()?.push_back(part);
        }
        Ok(())
    }

    pub fn get_settings(&self) -> FuseQueryResult<Vec<DataValue>> {
        self.settings.get_settings()
    }

    pub fn set_statistics(&self, val: &Statistics) -> FuseQueryResult<()> {
        *self.statistics.lock()? = val.clone();
        Ok(())
    }

    pub fn get_statistics(&self) -> FuseQueryResult<Statistics> {
        let statistics = self.statistics.lock()?;
        Ok(Statistics {
            read_rows: statistics.read_rows,
            read_bytes: statistics.read_bytes,
        })
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.datasource.lock()?.get_table(db_name, table_name)
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("max_threads", u64, 8, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string()),
        ("max_block_size", u64, 10000, "Maximum block size for reading".to_string()),
        ("default_db", String, "default".to_string(), "the default database for current session".to_string())
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
