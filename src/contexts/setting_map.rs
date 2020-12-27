// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};

#[derive(Debug, Clone)]
pub struct SettingMap {
    settings: Arc<Mutex<HashMap<&'static str, DataValue>>>,
}

impl SettingMap {
    pub fn create() -> Self {
        SettingMap {
            settings: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub fn try_set_u64(&self, key: &'static str, val: u64) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        settings.insert(key, DataValue::UInt64(Some(val)));
        Ok(())
    }

    pub fn try_get_u64(&self, key: &str) -> FuseQueryResult<u64> {
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

    pub fn try_set_string(&self, key: &'static str, val: String) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        settings.insert(key, DataValue::String(Some(val)));
        Ok(())
    }

    pub fn try_get_string(&self, key: &str) -> FuseQueryResult<String> {
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
}
