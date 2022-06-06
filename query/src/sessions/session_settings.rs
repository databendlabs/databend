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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_base::infallible::RwLock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::Itertools;

use crate::Config;

#[derive(Clone)]
enum ScopeLevel {
    #[allow(dead_code)]
    Global,
    Session,
}

impl Debug for ScopeLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            ScopeLevel::Global => {
                write!(f, "GLOBAL")
            }
            ScopeLevel::Session => {
                write!(f, "SESSION")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SettingValue {
    // Default value of this setting.
    default_value: DataValue,
    user_setting: UserSetting,
    level: ScopeLevel,
    desc: &'static str,
}

#[derive(Clone)]
pub struct Settings {
    settings: Arc<RwLock<HashMap<String, SettingValue>>>,
}

impl Settings {
    pub fn try_create(conf: &Config) -> Result<Settings> {
        let values = vec![
            // max_block_size
            SettingValue {
                default_value: DataValue::UInt64(10000),
                user_setting: UserSetting::create("max_block_size", DataValue::UInt64(10000)),
                level: ScopeLevel::Session,
                desc: "Maximum block size for reading",
            },

            // max_threads
            SettingValue {
                default_value: DataValue::UInt64(16),
                user_setting: UserSetting::create("max_threads", DataValue::UInt64(16)),
                level: ScopeLevel::Session,
                desc: "The maximum number of threads to execute the request. By default, it is determined automatically.",
            },

            // flight_client_timeout
            SettingValue {
                default_value: DataValue::UInt64(60),
                user_setting: UserSetting::create("flight_client_timeout", DataValue::UInt64(60)),
                level: ScopeLevel::Session,
                desc: "Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds",
            },

            // storage_read_buffer_size
            SettingValue {
                default_value: DataValue::UInt64(1024 * 1024),
                user_setting: UserSetting::create("storage_read_buffer_size", DataValue::UInt64(1024 * 1024)),
                level: ScopeLevel::Session,
                desc: "The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.",
            },

            // enable_new_processor_framework
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("enable_new_processor_framework", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "Enable new processor framework if value != 0, default value: 1",
            },
            // enable_planner_v2
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("enable_planner_v2", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Enable planner v2 by setting this variable to 1, default value: 0",
            },
            SettingValue {
                default_value: DataValue::String("\n".as_bytes().to_vec()),
                user_setting: UserSetting::create("record_delimiter", DataValue::String("\n".as_bytes().to_vec())),
                level: ScopeLevel::Session,
                desc: "Format record_delimiter, default value: \n",
            },
            SettingValue {
                default_value: DataValue::String(",".as_bytes().to_vec()),
                user_setting: UserSetting::create("field_delimiter", DataValue::String(",".as_bytes().to_vec())),
                level: ScopeLevel::Session,
                desc: "Format field delimiter, default value: ,",
            },
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("empty_as_default", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "Format empty_as_default, default value: 1",
            },
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("skip_header", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Whether to skip the input header, default value: 0",
            },
            SettingValue {
                default_value: DataValue::String("None".as_bytes().to_vec()),
                user_setting: UserSetting::create("compression", DataValue::String("None".as_bytes().to_vec())),
                level: ScopeLevel::Session,
                desc: "Format compression, default value: None",
            },
            SettingValue {
                default_value: DataValue::String("UTC".as_bytes().to_vec()),
                user_setting: UserSetting::create("timezone", DataValue::String("UTC".as_bytes().to_vec())),
                level: ScopeLevel::Session,
                desc: "Timezone, default value: UTC,",
            },
            SettingValue {
                default_value: DataValue::UInt64(10000),
                user_setting: UserSetting::create("group_by_two_level_threshold", DataValue::UInt64(10000)),
                level: ScopeLevel::Session,
                desc: "The threshold of keys to open two-level aggregation, default value: 10000",
            },
        ];

        let settings = Arc::new(RwLock::new(HashMap::default()));

        // Initial settings.
        {
            let mut settings_mut = settings.write();
            for value in values {
                let name = value.user_setting.name.clone();
                settings_mut.insert(name, value);
            }
        }

        let ret = Settings { settings };

        // Overwrite settings from conf.
        {
            // Set max threads.
            let cpus = if conf.query.num_cpus == 0 {
                num_cpus::get() as u64
            } else {
                conf.query.num_cpus
            };
            ret.set_max_threads(cpus)?;
        }

        Ok(ret)
    }

    // Get max_block_size.
    pub fn get_max_block_size(&self) -> Result<u64> {
        let key = "max_block_size";
        self.try_get_u64(key)
    }

    // Get max_threads.
    pub fn get_max_threads(&self) -> Result<u64> {
        let key = "max_threads";
        self.try_get_u64(key)
    }

    // Set max_threads.
    pub fn set_max_threads(&self, val: u64) -> Result<()> {
        let key = "max_threads";
        self.try_set_u64(key, val, false)
    }

    // Get flight client timeout.
    pub fn get_flight_client_timeout(&self) -> Result<u64> {
        let key = "flight_client_timeout";
        self.try_get_u64(key)
    }

    // Get storage read buffer size.
    pub fn get_storage_read_buffer_size(&self) -> Result<u64> {
        let key = "storage_read_buffer_size";
        self.try_get_u64(key)
    }

    pub fn get_enable_new_processor_framework(&self) -> Result<u64> {
        let key = "enable_new_processor_framework";
        self.try_get_u64(key)
    }

    pub fn get_enable_planner_v2(&self) -> Result<u64> {
        static KEY: &str = "enable_planner_v2";
        self.try_get_u64(KEY)
    }

    pub fn get_field_delimiter(&self) -> Result<Vec<u8>> {
        let key = "field_delimiter";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    pub fn get_record_delimiter(&self) -> Result<Vec<u8>> {
        let key = "record_delimiter";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    pub fn get_compression(&self) -> Result<Vec<u8>> {
        let key = "compression";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    pub fn get_empty_as_default(&self) -> Result<u64> {
        let key = "empty_as_default";
        self.try_get_u64(key)
    }

    pub fn get_skip_header(&self) -> Result<u64> {
        let key = "skip_header";
        self.try_get_u64(key)
    }

    pub fn get_timezone(&self) -> Result<Vec<u8>> {
        let key = "timezone";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    // Get group by two level threshold
    pub fn get_group_by_two_level_threshold(&self) -> Result<u64> {
        let key = "group_by_two_level_threshold";
        self.try_get_u64(key)
    }

    // Set group by two level threshold
    pub fn set_group_by_two_level_threshold(&self, val: u64) -> Result<()> {
        let key = "group_by_two_level_threshold";
        self.try_set_u64(key, val, false)
    }

    pub fn has_setting(&self, key: &str) -> bool {
        let settings = self.settings.read();
        settings.get(key).is_some()
    }

    fn check_and_get_setting_value(&self, key: &str) -> Result<SettingValue> {
        let settings = self.settings.read();
        let setting = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        Ok(setting.clone())
    }

    // Get u64 value, we don't get from the metasrv.
    fn try_get_u64(&self, key: &str) -> Result<u64> {
        let setting = self.check_and_get_setting_value(key)?;
        setting.user_setting.value.as_u64()
    }

    // Set u64 value to settings map, if is_global will write to metasrv.
    fn try_set_u64(&self, key: &str, val: u64, _is_global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::UInt64(val);

        Ok(())
    }

    fn try_set_string(&self, key: &str, val: Vec<u8>, _is_global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::String(val);

        Ok(())
    }

    pub fn get_setting_values(&self) -> Vec<DataValue> {
        let settings = self.settings.read();

        let mut result = vec![];
        for (k, v) in settings.iter().sorted_by_key(|&(k, _)| k) {
            let res = DataValue::Struct(vec![
                // Name.
                DataValue::String(k.as_bytes().to_vec()),
                // Value.
                v.user_setting.value.clone(),
                // Default Value.
                v.default_value.clone(),
                // Scope level.
                DataValue::String(format!("{:?}", v.level).into_bytes()),
                // Desc.
                DataValue::String(v.desc.as_bytes().to_vec()),
            ]);
            result.push(res);
        }
        result
    }

    pub fn get_changed_settings(&self) -> Settings {
        let settings = self.settings.read();
        let mut values = vec![];
        for (_k, v) in settings.iter().sorted_by_key(|&(k, _)| k) {
            if v.user_setting.value != v.default_value {
                values.push(v.clone());
            }
        }
        let new_settings = Arc::new(RwLock::new(HashMap::default()));
        {
            let mut new_settings_mut = new_settings.write();
            for value in values {
                let name = value.user_setting.name.clone();
                new_settings_mut.insert(name, value.clone());
            }
        }
        Settings {
            settings: new_settings,
        }
    }

    pub fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        let mut settings = self.settings.write();
        let values = changed_settings.get_setting_values();
        for value in values.into_iter() {
            if let DataValue::Struct(vals) = value {
                let key = vals[0].to_string();
                let mut val = settings.get_mut(&key).ok_or_else(|| {
                    ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key))
                })?;
                val.user_setting.value = vals[2].clone();
            }
        }
        Ok(())
    }

    pub fn get_setting_values_short(&self) -> BTreeMap<String, DataValue> {
        let settings = self.settings.read();

        let mut result = BTreeMap::new();
        for (k, v) in settings.iter().sorted_by_key(|&(k, _)| k) {
            result.insert(k.clone(), v.user_setting.value.clone());
        }
        result
    }

    pub fn set_settings(&self, key: String, val: String, is_global: bool) -> Result<()> {
        let setting = self.check_and_get_setting_value(&key)?;

        match setting.user_setting.value.max_data_type().data_type_id() {
            TypeID::UInt64 => {
                let u64_val = val.parse::<u64>()?;
                self.try_set_u64(&key, u64_val, is_global)?;
            }
            TypeID::String => {
                self.try_set_string(&key, val.into_bytes(), is_global)?;
            }

            v => {
                return Err(ErrorCode::UnknownVariable(format!(
                    "Unsupported variable:{:?} type:{:?} when set_settings().",
                    key, v
                )));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct UserSetting {
    // The name of the setting.
    pub name: String,

    // The value of the setting.
    pub value: DataValue,
}

impl UserSetting {
    pub fn create(name: &str, value: DataValue) -> UserSetting {
        UserSetting {
            name: name.to_string(),
            value,
        }
    }
}
