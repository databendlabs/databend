// Copyright 2021 Datafuse Labs.
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
use common_macros::MallocSizeOf;

#[derive(Clone, Debug, MallocSizeOf)]
pub struct SettingItem {
    value: DataValue,
    desc: &'static str,
}

#[derive(Clone, Debug, MallocSizeOf)]
pub struct Settings {
    // DataValue is of DataValue::Struct([name, value, default_value, description])
    settings: Arc<RwLock<HashMap<&'static str, SettingItem>>>,
}

impl Settings {
    pub fn create() -> Result<Self> {
        let settings = Arc::new(RwLock::new(HashMap::default()));
        let mut map = settings.write();

        // max_block_size.
        map.insert("max_block_size", SettingItem {
            value: DataValue::Int64(Some(10000)),
            desc: "Maximum block size for reading",
        });
        // max_threads.
        map.insert("max_threads", SettingItem {
            value: DataValue::Int64(Some(16)),
            desc: "The maximum number of threads to execute the request. By default, it is determined automatically.",
        });

        Ok(Settings {
            settings: settings.clone(),
        })
    }

    pub fn get_max_block_size(&self) -> Result<i64> {
        let key = "max_block_size";
        self.try_get_int(key)
    }

    pub fn set_max_block_size(&self, val: i64, global: bool) -> Result<()> {
        let key = "max_block_size";
        self.try_set_int(key, val, global)
    }

    // Get int value from settings map.
    fn try_get_int(&self, key: &str) -> Result<i64> {
        let settings = self.settings.read();
        let val = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        val.value.as_i64()
    }

    // Set int value to settings map, if global also write to meta.
    fn try_set_int(&self, key: &str, val: i64, global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut item = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        item.value = DataValue::Int64(Some(val));

        if global {
            // TODO(bohu): Write value to meta service.
        }
        Ok(())
    }
}
