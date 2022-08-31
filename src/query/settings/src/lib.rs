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

#![deny(unused_crate_dependencies)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str;
use std::sync::Arc;

use common_ast::Dialect;
use common_config::Config;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserSetting;
use common_users::UserApiProvider;
use itertools::Itertools;
use parking_lot::RwLock;

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
    possible_values: Option<Vec<&'static str>>,
}

#[derive(Clone)]
pub struct Settings {
    settings: Arc<RwLock<HashMap<String, SettingValue>>>,
    // TODO verify this, will tenant change during the lifetime of a given session?
    //#[allow(dead_code)]
    // session_ctx: Arc<T>,
    tenant: String,
}

impl Settings {
    pub async fn try_create(
        conf: &Config,
        user_api: Arc<UserApiProvider>,
        tenant: String,
    ) -> Result<Arc<Settings>> {
        let settings = Self::default_settings(&tenant);

        let ret = {
            // Overwrite settings from metasrv
            let global_settings = user_api
                .get_setting_api_client(&tenant)?
                .get_settings()
                .await?;

            for global_setting in global_settings {
                let name = global_setting.name;
                let val = String::from_utf8(global_setting.value.as_string()?).unwrap();
                settings.set_settings(name, val, true)?;
            }
            settings
        };

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

    pub fn default_settings(tenant: &str) -> Arc<Settings> {
        let values = vec![
            // max_block_size
            SettingValue {
                default_value: DataValue::UInt64(10000),
                user_setting: UserSetting::create("max_block_size", DataValue::UInt64(10000)),
                level: ScopeLevel::Session,
                desc: "Maximum block size for reading",
                possible_values: None,
            },
            // max_threads
            SettingValue {
                default_value: DataValue::UInt64(16),
                user_setting: UserSetting::create("max_threads", DataValue::UInt64(16)),
                level: ScopeLevel::Session,
                desc: "The maximum number of threads to execute the request. By default, it is determined automatically.",
                possible_values: None,
            },
            // flight_client_timeout
            SettingValue {
                default_value: DataValue::UInt64(60),
                user_setting: UserSetting::create("flight_client_timeout", DataValue::UInt64(60)),
                level: ScopeLevel::Session,
                desc: "Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds",
                possible_values: None,
            },
            // storage_read_buffer_size
            SettingValue {
                default_value: DataValue::UInt64(1024 * 1024),
                user_setting: UserSetting::create(
                    "storage_read_buffer_size",
                    DataValue::UInt64(1024 * 1024),
                ),
                level: ScopeLevel::Session,
                desc: "The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.",
                possible_values: None,
            },
            // enable_new_processor_framework
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create(
                    "enable_new_processor_framework",
                    DataValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "Enable new processor framework if value != 0, default value: 1",
                possible_values: None,
            },
            // enable_planner_v2
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("enable_planner_v2", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "Enable planner v2 by setting this variable to 1, default value: 1",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::String("\n".as_bytes().to_vec()),
                user_setting: UserSetting::create(
                    "record_delimiter",
                    DataValue::String("\n".as_bytes().to_vec()),
                ),
                level: ScopeLevel::Session,
                desc: "Format record_delimiter, default value: \"\\n\"",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::String(",".as_bytes().to_vec()),
                user_setting: UserSetting::create(
                    "field_delimiter",
                    DataValue::String(",".as_bytes().to_vec()),
                ),
                level: ScopeLevel::Session,
                desc: "Format field delimiter, default value: ,",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("empty_as_default", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "Format empty_as_default, default value: 1",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("skip_header", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Whether to skip the input header, default value: 0",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::String("None".as_bytes().to_vec()),
                user_setting: UserSetting::create(
                    "compression",
                    DataValue::String("None".as_bytes().to_vec()),
                ),
                level: ScopeLevel::Session,
                desc: "Format compression, default value: None",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::String("UTC".as_bytes().to_vec()),
                user_setting: UserSetting::create(
                    "timezone",
                    DataValue::String("UTC".as_bytes().to_vec()),
                ),
                level: ScopeLevel::Session,
                desc: "Timezone, default value: UTC,",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(10000),
                user_setting: UserSetting::create(
                    "group_by_two_level_threshold",
                    DataValue::UInt64(10000),
                ),
                level: ScopeLevel::Session,
                desc: "The threshold of keys to open two-level aggregation, default value: 10000",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("enable_async_insert", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Whether the client open async insert mode, default value: 0",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("wait_for_async_insert", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "Whether the client wait for the reply of async insert, default value: 1",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(100),
                user_setting: UserSetting::create(
                    "wait_for_async_insert_timeout",
                    DataValue::UInt64(100),
                ),
                level: ScopeLevel::Session,
                desc: "The timeout in seconds for waiting for processing of async insert, default value: 100",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create(
                    "unquoted_ident_case_sensitive",
                    DataValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "Case sensitivity of unquoted identifiers, default value: 0 (aka case-insensitive)",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create(
                    "quoted_ident_case_sensitive",
                    DataValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "Case sensitivity of quoted identifiers, default value: 1 (aka case-sensitive)",
                possible_values: None,
            },
            SettingValue {
                default_value: DataValue::String("PostgreSQL".as_bytes().to_vec()),
                user_setting: UserSetting::create(
                    "sql_dialect",
                    DataValue::String("PostgreSQL".as_bytes().to_vec()),
                ),
                level: ScopeLevel::Session,
                desc: "SQL dialect, support \"PostgreSQL\" and \"MySQL\", default value: \"PostgreSQL\"",
                possible_values: Some(vec!["PostgreSQL", "MySQL"]),
            },
            // max_execute_time
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("max_execute_time", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "The maximum query execution time. it means no limit if the value is zero. default value: 0",
                possible_values: None,
            },
        ];

        let settings: Arc<RwLock<HashMap<String, SettingValue>>> =
            Arc::new(RwLock::new(HashMap::default()));

        // Initial settings.
        {
            let mut settings_mut = settings.write();
            for value in values {
                let name = value.user_setting.name.clone();
                settings_mut.insert(name, value);
            }
        }

        Arc::new(Settings {
            tenant: tenant.to_string(),
            settings,
        })
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

    // Get max_execute_time.
    pub fn get_max_execute_time(&self) -> Result<u64> {
        self.try_get_u64("max_execute_time")
    }

    // Set max_execute_time.
    pub fn set_max_execute_time(&self, val: u64) -> Result<()> {
        self.try_set_u64("max_execute_time", val, false)
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

    pub fn get_enable_async_insert(&self) -> Result<u64> {
        let key = "enable_async_insert";
        self.try_get_u64(key)
    }

    pub fn set_enable_async_insert(&self, val: u64) -> Result<()> {
        let key = "enable_async_insert";
        self.try_set_u64(key, val, false)
    }

    pub fn get_wait_for_async_insert(&self) -> Result<u64> {
        let key = "wait_for_async_insert";
        self.try_get_u64(key)
    }

    pub fn set_wait_for_async_insert(&self, val: u64) -> Result<()> {
        let key = "wait_for_async_insert";
        self.try_set_u64(key, val, false)
    }

    pub fn get_wait_for_async_insert_timeout(&self) -> Result<u64> {
        let key = "wait_for_async_insert_timeout";
        self.try_get_u64(key)
    }

    pub fn set_wait_for_async_insert_timeout(&self, val: u64) -> Result<()> {
        let key = "wait_for_async_insert_timeout";
        self.try_set_u64(key, val, false)
    }

    pub fn get_unquoted_ident_case_sensitive(&self) -> Result<bool> {
        static KEY: &str = "unquoted_ident_case_sensitive";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_unquoted_ident_case_sensitive(&self, val: bool) -> Result<()> {
        static KEY: &str = "unquoted_ident_case_sensitive";
        let v = if val { 1 } else { 0 };
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_quoted_ident_case_sensitive(&self) -> Result<bool> {
        static KEY: &str = "quoted_ident_case_sensitive";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_quoted_ident_case_sensitive(&self, val: bool) -> Result<()> {
        static KEY: &str = "quoted_ident_case_sensitive";
        let v = if val { 1 } else { 0 };
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_sql_dialect(&self) -> Result<Dialect> {
        let key = "sql_dialect";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
            .map(|v| {
                if v == b"MySQL" {
                    Dialect::MySQL
                } else {
                    Dialect::PostgreSQL
                }
            })
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

    fn check_possible_values(&self, setting: &SettingValue, val: String) -> Result<String> {
        if let Some(possible_values) = &setting.possible_values {
            for possible_value in possible_values {
                if possible_value.to_lowercase() == val.to_lowercase() {
                    return Ok(possible_value.to_string());
                }
            }
            return Err(ErrorCode::WrongValueForVariable(format!(
                "Variable {:?} can't be set to the value of {:?}",
                setting.user_setting.name, val
            )));
        }
        Ok(val)
    }

    // Get u64 value, we don't get from the metasrv.
    fn try_get_u64(&self, key: &str) -> Result<u64> {
        let setting = self.check_and_get_setting_value(key)?;
        setting.user_setting.value.as_u64()
    }

    // Set u64 value to settings map, if is_global will write to metasrv.
    fn try_set_u64(&self, key: &str, val: u64, is_global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::UInt64(val);

        if is_global {
            let tenant = self.tenant.as_str();
            let _ = futures::executor::block_on(
                UserApiProvider::instance()
                    .get_setting_api_client(tenant)?
                    .set_setting(setting.user_setting.clone()),
            )?;
            setting.level = ScopeLevel::Global;
        }

        Ok(())
    }

    fn try_set_string(&self, key: &str, val: Vec<u8>, is_global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::String(val);

        if is_global {
            let _ = futures::executor::block_on(
                UserApiProvider::instance()
                    .get_setting_api_client(&self.tenant)?
                    .set_setting(setting.user_setting.clone()),
            )?;
            setting.level = ScopeLevel::Global;
        }

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
            tenant: self.tenant.clone(),
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
        let val = self.check_possible_values(&setting, val)?;

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

    pub fn set_batch_settings(
        &self,
        settings: &HashMap<String, String>,
        is_global: bool,
    ) -> Result<()> {
        for (k, v) in settings.iter() {
            if self.has_setting(k.as_str()) {
                self.set_settings(k.to_string(), v.to_string(), is_global)?
            }
        }

        Ok(())
    }
}
