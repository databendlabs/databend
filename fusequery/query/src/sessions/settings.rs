// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;

#[derive(Debug)]
pub struct Settings {
    inner: SettingsBase,
}

impl Settings {
    apply_macros! { apply_getter_setter_settings, apply_initial_settings, apply_update_settings,
        ("max_block_size", u64, 10000, "Maximum block size for reading".to_string()),
        ("max_threads", u64, 16, "The maximum number of threads to execute the request. By default, it is determined automatically.".to_string()),
        ("flight_client_timeout", u64, 60, "Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds".to_string()),
        ("min_distributed_rows", u64, 100000000, "Minimum distributed read rows. In cluster mode, when read rows exceeds this value, the local table converted to distributed query.".to_string()),
        ("min_distributed_bytes", u64, 500 * 1024 * 1024, "Minimum distributed read bytes. In cluster mode, when read bytes exceeds this value, the local table converted to distributed query.".to_string())
    }

    pub fn try_create() -> Result<Arc<Settings>> {
        let settings = Arc::new(Settings {
            inner: SettingsBase::create(),
        });

        settings.initial_settings()?;
        settings.set_max_threads(num_cpus::get() as u64)?;

        Ok(settings)
    }

    pub fn iter(&self) -> SettingsIterator {
        SettingsIterator {
            settings: self.inner.get_settings(),
            index: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SettingsBase {
    // DataValue is of DataValue::Struct([name, value, default_value, description])
    settings: Arc<RwLock<HashMap<&'static str, DataValue>>>,
}

impl SettingsBase {
    pub fn create() -> Self {
        SettingsBase {
            settings: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    // TODO, to use macro generate this codes
    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    #[allow(unused)]
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

    pub fn get_settings(&self) -> Vec<DataValue> {
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
        result
    }
}

pub struct SettingsIterator {
    settings: Vec<DataValue>,
    index: usize,
}

impl Iterator for SettingsIterator {
    type Item = DataValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.settings.len() {
            None
        } else {
            let setting = self.settings[self.index].clone();
            self.index += 1;
            Some(setting)
        }
    }
}
