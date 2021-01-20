// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::contexts::Settings;
use crate::datasources::{IDataSource, ITable, Statistics};
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};

pub struct FuseQueryContext {
    datasource: Arc<Mutex<dyn IDataSource>>,
    statistics: Mutex<Statistics>,
    settings: Settings,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create_ctx(datasource: Arc<Mutex<dyn IDataSource>>) -> FuseQueryResult<Arc<Self>> {
        let settings = Settings::create();
        let ctx = FuseQueryContext {
            settings,
            datasource,
            statistics: Mutex::new(Statistics::default()),
        };

        ctx.initial_settings()?;
        Ok(Arc::new(ctx))
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
