// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::contexts::SettingMap;
use crate::datasources::{IDataSource, ITable};
use crate::error::FuseQueryResult;

pub struct FuseQueryContext {
    datasource: Arc<Mutex<dyn IDataSource>>,
    settings: SettingMap,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn try_create_ctx(datasource: Arc<Mutex<dyn IDataSource>>) -> FuseQueryResult<Arc<Self>> {
        let settings = SettingMap::create();
        settings.try_set_u64("max_threads", 8)?;
        settings.try_set_u64("max_block_size", 10000)?;
        settings.try_set_string("default_db", "default".to_string())?;

        Ok(Arc::new(FuseQueryContext {
            datasource,
            settings,
        }))
    }

    pub fn get_current_database(&self) -> FuseQueryResult<String> {
        let key = "default_db";
        self.settings.try_get_string(key)
    }

    pub fn set_current_database(&self, val: &str) -> FuseQueryResult<()> {
        let key = "default_db";
        self.datasource.lock()?.check_database(val)?;
        self.settings.try_set_string(key, val.to_string())
    }

    pub fn get_max_block_size(&self) -> FuseQueryResult<u64> {
        let key = "max_block_size";
        self.settings.try_get_u64(key)
    }

    pub fn set_max_block_size(&self, val: u64) -> FuseQueryResult<()> {
        let key = "max_block_size";
        self.settings.try_set_u64(key, val)
    }

    pub fn get_max_threads(&self) -> FuseQueryResult<u64> {
        let key = "max_threads";
        self.settings.try_get_u64(key)
    }

    pub fn set_max_threads(&self, val: u64) -> FuseQueryResult<()> {
        let key = "max_threads";
        self.settings.try_set_u64(key, val)
    }

    pub fn get_settings(&self) -> FuseQueryResult<(Vec<String>, Vec<String>)> {
        self.settings.get_settings()
    }

    pub fn get_table(&self, db_name: &str, table_name: &str) -> FuseQueryResult<Arc<dyn ITable>> {
        self.datasource.lock()?.get_table(db_name, table_name)
    }
}

impl std::fmt::Debug for FuseQueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.settings)
    }
}
