// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;

#[derive(Clone, Debug)]
pub struct SettingValue {
    value: DataValue,
    desc: &'static str,
}

#[derive(Clone, Debug)]
pub struct Settings {
    settings: Arc<RwLock<HashMap<&'static str, SettingValue>>>,
}

impl Settings {
    pub fn try_create() -> Result<Self> {
        let map = Arc::new(RwLock::new(HashMap::default()));
        let mut settings = map.write();

        // max_block_size.
        settings.insert("max_block_size", SettingValue {
            value: DataValue::UInt64(Some(10000)),
            desc: "Maximum block size for reading",
        });
        // max_threads.
        settings.insert("max_threads", SettingValue {
            value: DataValue::UInt64(Some(16)),
            desc: "The maximum number of threads to execute the request. By default, it is determined automatically.",
        });

        Ok(Settings {
            settings: map.clone(),
        })
    }

    // Get max_block_size.
    pub fn get_max_block_size(&self) -> Result<u64> {
        let key = "max_block_size";
        self.try_get_u64(key)
    }

    // Set max_block_size.
    pub fn set_max_block_size(&self, val: u64, global: bool) -> Result<()> {
        let key = "max_block_size";
        self.try_set_u64(key, val, global)
    }

    // Get max_threads.
    pub fn get_max_threads(&self) -> Result<u64> {
        let key = "max_threads";
        self.try_get_u64(key)
    }

    // Set max_threads.
    pub fn set_max_threads(&self, val: u64, global: bool) -> Result<()> {
        let key = "max_threads";
        self.try_set_u64(key, val, global)
    }

    // Get u64 value from settings map.
    fn try_get_u64(&self, key: &str) -> Result<u64> {
        let settings = self.settings.read();
        let setting = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        setting.value.as_u64()
    }

    // Set u64 value to settings map, if global also write to meta.
    fn try_set_u64(&self, key: &str, val: u64, global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.value = DataValue::UInt64(Some(val));

        if global {
            // TODO(bohu): Write value to meta service.
        }
        Ok(())
    }
}
