// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::{Arc, Mutex};

use crate::datasources::{IDataSource, ITable};
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};
use std::collections::HashMap;

pub struct FuseQueryContext {
    datasource: Arc<Mutex<dyn IDataSource>>,
    settings: Mutex<HashMap<&'static str, DataValue>>,
}

pub type FuseQueryContextRef = Arc<FuseQueryContext>;

impl FuseQueryContext {
    pub fn create_ctx(datasource: Arc<Mutex<dyn IDataSource>>) -> Self {
        let mut settings = HashMap::default();
        settings.insert("max_threads", DataValue::UInt64(Some(8)));
        settings.insert("max_block_size", DataValue::UInt64(Some(10000)));
        settings.insert("default_db", DataValue::String(Some("default".to_string())));
        FuseQueryContext {
            datasource,
            settings: Mutex::new(settings),
        }
    }

    fn try_set_u64(&self, key: &'static str, val: u64) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        settings.insert(key, DataValue::UInt64(Some(val)));
        Ok(())
    }

    fn try_get_u64(&self, key: &str) -> FuseQueryResult<u64> {
        let settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::UInt64(Some(result)) = val.clone() {
            Ok(result)
        } else {
            Err(FuseQueryError::Internal(format!(
                "Cannot find the setting: {}",
                key
            )))
        }
    }

    fn try_set_string(&self, key: &'static str, val: String) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        settings.insert(key, DataValue::String(Some(val)));
        Ok(())
    }

    fn try_get_string(&self, key: &str) -> FuseQueryResult<String> {
        let settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::String(Some(result)) = val.clone() {
            Ok(result)
        } else {
            Err(FuseQueryError::Internal(format!(
                "Cannot find the setting: {}",
                key
            )))
        }
    }

    pub fn get_current_database(&self) -> FuseQueryResult<String> {
        let key = "default_db";
        self.try_get_string(key)
    }

    pub fn set_current_database(&self, val: &str) -> FuseQueryResult<()> {
        let key = "default_db";
        self.try_set_string(key, val.to_string())
    }

    pub fn get_max_block_size(&self) -> FuseQueryResult<u64> {
        let key = "max_block_size";
        self.try_get_u64(key)
    }

    pub fn set_max_block_size(&self, val: u64) -> FuseQueryResult<()> {
        let key = "max_block_size";
        self.try_set_u64(key, val)
    }

    pub fn get_max_threads(&self) -> FuseQueryResult<u64> {
        let key = "max_threads";
        self.try_get_u64(key)
    }

    pub fn set_max_threads(&self, val: u64) -> FuseQueryResult<()> {
        let key = "max_threads";
        self.try_set_u64(key, val)
    }

    pub fn get_settings(&self) -> FuseQueryResult<(Vec<String>, Vec<String>)> {
        let settings = self.settings.lock()?;
        let names: Vec<String> = settings.keys().into_iter().map(|x| x.to_string()).collect();
        let values: Vec<String> = settings
            .values()
            .into_iter()
            .map(|x| format!("{}", x))
            .collect();
        Ok((names, values))
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
