// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;

#[derive(Debug, Clone)]
pub struct Settings {
    // DataValue is of DataValue::Struct([name, value, default_value, description])
    settings: Arc<RwLock<HashMap<&'static str, DataValue>>>,
}

impl Settings {
    pub fn create() -> Self {
        Settings {
            settings: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    // TODO, to use macro generate this codes
    pub fn try_set_u64(&self, key: &'static str, val: u64, desc: String) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = DataValue::Struct(vec![
            DataValue::UInt64(Some(val)),
            DataValue::UInt64(Some(val)),
            DataValue::Utf8(Some(desc)),
        ]);
        settings.insert(key, setting_val);
        Ok(())
    }

    pub fn try_update_u64(&self, key: &'static str, val: u64) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            let v = DataValue::Struct(vec![
                DataValue::UInt64(Some(val)),
                values[1].clone(),
                values[2].clone(),
            ]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_u64(&self, key: &str) -> Result<u64> {
        let settings = self.settings.read();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            if let DataValue::UInt64(Some(result)) = values[0].clone() {
                return Ok(result);
            }
        }

        Result::Err(ErrorCode::UnknownVariable(format!(
            "Unknown variable: {:?}",
            key
        )))
    }

    pub fn try_set_i64(&self, key: &'static str, val: i64, desc: String) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = DataValue::Struct(vec![
            DataValue::Int64(Some(val)),
            DataValue::Int64(Some(val)),
            DataValue::Utf8(Some(desc)),
        ]);
        settings.insert(key, setting_val);
        Ok(())
    }

    pub fn try_update_i64(&self, key: &'static str, val: i64) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            let v = DataValue::Struct(vec![
                DataValue::Int64(Some(val)),
                values[1].clone(),
                values[2].clone(),
            ]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_i64(&self, key: &str) -> Result<i64> {
        let settings = self.settings.read();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            if let DataValue::Int64(Some(result)) = values[0].clone() {
                return Ok(result);
            }
        }

        Result::Err(ErrorCode::UnknownVariable(format!(
            "Unknown variable: {:?}",
            key
        )))
    }

    pub fn try_set_f64(&self, key: &'static str, val: f64, desc: String) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = DataValue::Struct(vec![
            DataValue::Float64(Some(val)),
            DataValue::Float64(Some(val)),
            DataValue::Utf8(Some(desc)),
        ]);
        settings.insert(key, setting_val);
        Ok(())
    }

    pub fn try_update_f64(&self, key: &'static str, val: f64) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            let v = DataValue::Struct(vec![
                DataValue::Float64(Some(val)),
                values[1].clone(),
                values[2].clone(),
            ]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_f64(&self, key: &str) -> Result<f64> {
        let settings = self.settings.read();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            if let DataValue::Float64(Some(result)) = values[0].clone() {
                return Ok(result);
            }
        }

        Result::Err(ErrorCode::UnknownVariable(format!(
            "Unknown variable: {:?}",
            key
        )))
    }

    pub fn try_set_string(&self, key: &'static str, val: String, desc: String) -> Result<()> {
        let mut settings = self.settings.write();
        let default_value = val.clone();
        let setting_val = DataValue::Struct(vec![
            DataValue::Utf8(Some(val)),
            DataValue::Utf8(Some(default_value)),
            DataValue::Utf8(Some(desc)),
        ]);
        settings.insert(key, setting_val);
        Ok(())
    }

    pub fn try_update_string(&self, key: &'static str, val: String) -> Result<()> {
        let mut settings = self.settings.write();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            let v = DataValue::Struct(vec![
                DataValue::Utf8(Some(val)),
                values[1].clone(),
                values[2].clone(),
            ]);
            settings.insert(key, v);
        }
        Ok(())
    }

    pub fn try_get_string(&self, key: &str) -> Result<String> {
        let settings = self.settings.read();
        let setting_val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if let DataValue::Struct(values) = setting_val {
            if let DataValue::Utf8(Some(result)) = values[0].clone() {
                return Ok(result);
            }
        }

        Result::Err(ErrorCode::UnknownVariable(format!(
            "Unknown variable: {:?}",
            key
        )))
    }

    pub fn get_settings(&self) -> Result<Vec<DataValue>> {
        let settings = self.settings.read();

        let mut result = vec![];
        for (k, v) in settings.iter() {
            if let DataValue::Struct(values) = v {
                let res = DataValue::Struct(vec![
                    DataValue::Utf8(Some(k.to_string())),
                    values[0].clone(),
                    values[1].clone(),
                    values[2].clone(),
                ]);
                result.push(res);
            }
        }
        Ok(result)
    }
}
