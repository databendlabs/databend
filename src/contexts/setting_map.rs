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

    pub fn try_set_u64(&self, key: &'static str, val: u64, desc: &str) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        let v = DataValue::Struct(vec![
            DataValue::UInt64(Some(val)),
            DataValue::String(Some(desc.to_string())),
        ]);
        settings.insert(key, v);
        Ok(())
    }

    pub fn try_update_u64(&self, key: &'static str, v: u64) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::Struct(values) = val {
            let v = DataValue::Struct(vec![DataValue::UInt64(Some(v)), values[1].clone()]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_u64(&self, key: &str) -> FuseQueryResult<u64> {
        let settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::Struct(values) = val.clone() {
            if let DataValue::UInt64(Some(result)) = values[0] {
                Ok(result)
            } else {
                Err(FuseQueryError::Internal(format!(
                    "Cannot find the setting: {}",
                    key
                )))
            }
        } else {
            Err(FuseQueryError::Internal(format!(
                "Cannot find the setting: {}",
                key
            )))
        }
    }

    pub fn try_set_string(
        &self,
        key: &'static str,
        val: String,
        desc: &str,
    ) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        let v = DataValue::Struct(vec![
            DataValue::String(Some(val)),
            DataValue::String(Some(desc.to_string())),
        ]);
        settings.insert(key, v);
        Ok(())
    }

    pub fn try_update_string(&self, key: &'static str, v: String) -> FuseQueryResult<()> {
        let mut settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::Struct(values) = val {
            let v = DataValue::Struct(vec![DataValue::String(Some(v)), values[1].clone()]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_string(&self, key: &str) -> FuseQueryResult<String> {
        let settings = self.settings.lock()?;
        let val = settings
            .get(key)
            .ok_or_else(|| FuseQueryError::Internal(format!("Cannot find the setting: {}", key)))?;

        if let DataValue::Struct(values) = val.clone() {
            if let DataValue::String(Some(result)) = values[0].clone() {
                Ok(result)
            } else {
                Err(FuseQueryError::Internal(format!(
                    "Cannot find the setting: {}",
                    key
                )))
            }
        } else {
            Err(FuseQueryError::Internal(format!(
                "Cannot find the setting: {}",
                key
            )))
        }
    }

    pub fn get_settings(&self) -> FuseQueryResult<Vec<DataValue>> {
        let settings = self.settings.lock()?;

        let mut result = vec![];
        for (k, v) in settings.iter() {
            if let DataValue::Struct(values) = v {
                let res = DataValue::Struct(vec![
                    DataValue::String(Some(k.to_string())),
                    values[0].clone(),
                    values[1].clone(),
                ]);
                result.push(res);
            }
        }
        Ok(result)
    }
}
