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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::UserSetting;

use crate::configs::Config;
use crate::sessions::SessionContext;
use crate::users::UserApiProvider;

#[derive(Clone)]
enum ScopeLevel {
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
    // The scope of the setting is GLOBAL(metasrv) or SESSION.
    level: ScopeLevel,
    desc: &'static str,
}

#[derive(Clone)]
pub struct Settings {
    settings: Arc<RwLock<HashMap<String, SettingValue>>>,
    #[allow(dead_code)]
    user_api: Arc<UserApiProvider>,
    #[allow(dead_code)]
    session_ctx: Arc<SessionContext>,
}

impl Settings {
    pub fn try_create(
        conf: &Config,
        session_ctx: Arc<SessionContext>,
        user_api: Arc<UserApiProvider>,
    ) -> Result<Settings> {
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

            // parallel_read_threads
            SettingValue {
                default_value: DataValue::UInt64(1),
                user_setting: UserSetting::create("parallel_read_threads", DataValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "The maximum number of parallelism for reading data. By default, it is 1.",
            },

            // storage_read_buffer_size
            SettingValue {
                default_value: DataValue::UInt64(1024 * 1024),
                user_setting: UserSetting::create("storage_read_buffer_size", DataValue::UInt64(1024 * 1024)),
                level: ScopeLevel::Session,
                desc: "The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.",
            },

            // storage_backoff_init_delay_ms
            SettingValue {
                default_value: DataValue::UInt64(5),
                user_setting: UserSetting::create("storage_occ_backoff_init_delay_ms", DataValue::UInt64(5)),
                level: ScopeLevel::Session,
                desc: "The initial retry delay in millisecond. By default, it is 5 ms.",
            },

            // storage_occ_backoff_max_delay_ms
            SettingValue {
                default_value: DataValue::UInt64(20 * 1000),
                user_setting: UserSetting::create("storage_occ_backoff_max_delay_ms", DataValue::UInt64(20 * 1000)),
                level: ScopeLevel::Session,
                desc: "The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.",
            },

            // storage_occ_backoff_max_elapsed_ms
            SettingValue {
                default_value: DataValue::UInt64(120 * 1000),
                user_setting: UserSetting::create("storage_occ_backoff_max_elapsed_ms", DataValue::UInt64(120 * 1000)),
                level: ScopeLevel::Session,
                desc: "The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.",
            },

            // enable_new_processor_framework
            SettingValue {
                default_value: DataValue::UInt64(0),
                user_setting: UserSetting::create("enable_new_processor_framework", DataValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Enable new processor framework if value != 0, default value: 0",
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

        let ret = Settings {
            settings,
            user_api,
            session_ctx,
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

    // Get parallel read threads.
    pub fn get_parallel_read_threads(&self) -> Result<u64> {
        let key = "parallel_read_threads";
        self.try_get_u64(key)
    }

    // Get storage read buffer size.
    pub fn get_storage_read_buffer_size(&self) -> Result<u64> {
        let key = "storage_read_buffer_size";
        self.try_get_u64(key)
    }

    // Get storage occ backoff init delay in ms.
    pub fn get_storage_occ_backoff_init_delay_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_init_delay_ms";
        self.try_get_u64(key)
    }

    // Get storage occ backoff max delay in ms.
    pub fn get_storage_occ_backoff_max_delay_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_max_delay_ms";
        self.try_get_u64(key)
    }

    // Get storage occ backoff max elapsed in ms.
    pub fn get_storage_occ_backoff_max_elapsed_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_max_elapsed_ms";
        self.try_get_u64(key)
    }

    pub fn get_enable_new_processor_framework(&self) -> Result<u64> {
        let key = "enable_new_processor_framework";
        self.try_get_u64(key)
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
    fn try_set_u64(&self, key: &str, val: u64, is_global: bool) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::UInt64(val);

        if is_global {
            let tenant = self.session_ctx.get_current_tenant();
            let _ = futures::executor::block_on(
                self.user_api
                    .get_setting_api_client(&tenant)?
                    .set_setting(setting.user_setting.clone()),
            )?;
            setting.level = ScopeLevel::Global;
        }

        Ok(())
    }

    pub fn get_setting_values(&self) -> Vec<DataValue> {
        let settings = self.settings.read();

        let mut result = vec![];
        for (k, v) in settings.iter() {
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

    pub fn set_settings(&self, key: String, val: String, is_global: bool) -> Result<()> {
        let setting = self.check_and_get_setting_value(&key)?;

        match setting.user_setting.value.max_data_type().data_type_id() {
            TypeID::UInt64 => {
                let u64_val = val.parse::<u64>()?;
                self.try_set_u64(&key, u64_val, is_global)?;
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
