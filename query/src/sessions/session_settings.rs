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

use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::UserSetting;

use crate::configs::Config;
use crate::sessions::SessionContext;
use crate::users::UserApiProvider;

#[derive(Clone, Debug)]
pub struct SettingValue {
    default_value: DataValue,
    user_setting: UserSetting,
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
        let mut values = vec![
            // max_block_size
            SettingValue {
                default_value:DataValue::UInt64(Some(10000)),
                user_setting: UserSetting::create("max_block_size", DataValue::UInt64(Some(10000))),
                desc: "Maximum block size for reading",
            },

            // max_threads
            SettingValue {
                default_value:DataValue::UInt64(Some(16)),
                user_setting: UserSetting::create("max_threads", DataValue::UInt64(Some(16))),
                desc: "The maximum number of threads to execute the request. By default, it is determined automatically.",
            },

            // flight_client_timeout
            SettingValue {
                default_value:DataValue::UInt64(Some(60)),
                user_setting: UserSetting::create("flight_client_timeout", DataValue::UInt64(Some(60))),
                desc:"Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds",
            },

            // parallel_read_threads
            SettingValue {
                default_value: DataValue::UInt64(Some(1)),
                user_setting: UserSetting::create("parallel_read_threads", DataValue::UInt64(Some(1))),
                desc:"The maximum number of parallelism for reading data. By default, it is 1.",
            },

            // storage_read_buffer_size
            SettingValue {
                default_value: DataValue::UInt64(Some(1024*1024)),
                user_setting: UserSetting::create("storage_read_buffer_size", DataValue::UInt64(Some(1024*1024))),
                desc:"The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.",
            },

            // storage_backoff_init_delay_ms
            SettingValue {
                default_value: DataValue::UInt64(Some(5)),
                user_setting: UserSetting::create("storage_occ_backoff_init_delay_ms", DataValue::UInt64(Some(5))),
                desc:"The initial retry delay in millisecond. By default,  it is 5 ms.",
            },

            // storage_occ_backoff_max_delay_ms
            SettingValue {
                default_value:DataValue::UInt64(Some(20*1000)),
                user_setting: UserSetting::create("storage_occ_backoff_max_delay_ms", DataValue::UInt64(Some(20*1000))),
                desc:"The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing. By default, it is 20 seconds.",
            },

            // storage_occ_backoff_max_elapsed_ms
            SettingValue {
                default_value:DataValue::UInt64(Some(120*1000)),
                user_setting: UserSetting::create("storage_occ_backoff_max_elapsed_ms", DataValue::UInt64(Some(120*1000))),
                desc:"The maximum elapsed time after the occ starts, beyond which there will be no more retries. By default, it is 2 minutes.",
            },
        ];

        let settings = Arc::new(RwLock::new(HashMap::default()));

        // Initial settings.
        // Note: Use code block here to drop the write lock at the end.
        {
            // Get settings from metasrv.
            let global_settings = futures::executor::block_on(
                user_api.get_settings(&session_ctx.get_current_tenant()),
            )?;

            for global in &global_settings {
                for vx in values.iter_mut() {
                    if global.name == vx.user_setting.name {
                        vx.default_value = global.value.clone();
                    }
                }
            }

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

        // Overwrite settings.
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

    // Set max_block_size.
    pub fn set_max_block_size(&self, val: u64) -> Result<()> {
        let key = "max_block_size";
        self.try_set_u64(key, val)
    }

    // Get max_threads.
    pub fn get_max_threads(&self) -> Result<u64> {
        let key = "max_threads";
        self.try_get_u64(key)
    }

    // Set max_threads.
    pub fn set_max_threads(&self, val: u64) -> Result<()> {
        let key = "max_threads";
        self.try_set_u64(key, val)
    }

    // Get flight client timeout.
    pub fn get_flight_client_timeout(&self) -> Result<u64> {
        let key = "flight_client_timeout";
        self.try_get_u64(key)
    }

    // Set flight client timeout.
    pub fn set_flight_client_timeout(&self, val: u64) -> Result<()> {
        let key = "flight_client_timeout";
        self.try_set_u64(key, val)
    }

    // Get parallel read threads.
    pub fn get_parallel_read_threads(&self) -> Result<u64> {
        let key = "parallel_read_threads";
        self.try_get_u64(key)
    }

    // Set parallel read threads.
    pub fn set_parallel_read_threads(&self, val: u64) -> Result<()> {
        let key = "parallel_read_threads";
        self.try_set_u64(key, val)
    }

    // Get storage read buffer size.
    pub fn get_storage_read_buffer_size(&self) -> Result<u64> {
        let key = "storage_read_buffer_size";
        self.try_get_u64(key)
    }

    // Set storage read buffer size.
    pub fn set_storage_read_buffer_size(&self, val: u64) -> Result<()> {
        let key = "storage_read_buffer_size";
        self.try_set_u64(key, val)
    }

    // Get storage occ backoff init delay in ms.
    pub fn get_storage_occ_backoff_init_delay_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_init_delay_ms";
        self.try_get_u64(key)
    }

    // Set storage occ backoff init delay in ms.
    pub fn set_storage_occ_backoff_init_delay_ms(&self, val: u64) -> Result<()> {
        let key = "storage_occ_backoff_init_delay_ms";
        self.try_set_u64(key, val)
    }

    // Get storage occ backoff max delay in ms.
    pub fn get_storage_occ_backoff_max_delay_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_max_delay_ms";
        self.try_get_u64(key)
    }

    // Set storage occ backoff max delay in ms.
    pub fn set_storage_occ_backoff_max_delay_ms(&self, val: u64) -> Result<()> {
        let key = "storage_occ_backoff_max_delay_ms";
        self.try_set_u64(key, val)
    }

    // Get storage occ backoff max elapsed in ms.
    pub fn get_storage_occ_backoff_max_elapsed_ms(&self) -> Result<u64> {
        let key = "storage_occ_backoff_max_elapsed_ms";
        self.try_get_u64(key)
    }

    // Set storage occ backoff max elapsed in ms.
    pub fn set_storage_occ_backoff_max_elapsed_ms(&self, val: u64) -> Result<()> {
        let key = "storage_occ_backoff_max_elapsed_ms";
        self.try_set_u64(key, val)
    }

    fn check_and_get_setting_value(&self, key: &str) -> Result<SettingValue> {
        let settings = self.settings.read();
        let setting = settings
            .get(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        Ok(setting.clone())
    }

    // Get u64 value from settings map.
    fn try_get_u64(&self, key: &str) -> Result<u64> {
        let setting = self.check_and_get_setting_value(key)?;
        setting.user_setting.value.as_u64()
    }

    // Set u64 value to settings map, if global(TODO) also write to meta.
    fn try_set_u64(&self, key: &str, val: u64) -> Result<()> {
        let mut settings = self.settings.write();
        let mut setting = settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = DataValue::UInt64(Some(val));

        Ok(())
    }

    pub fn get_setting_values(&self) -> Vec<DataValue> {
        let settings = self.settings.read();

        let mut result = vec![];
        for (k, v) in settings.iter() {
            let res = DataValue::Struct(vec![
                // Name.
                DataValue::String(Some(k.as_bytes().to_vec())),
                // Value.
                v.user_setting.value.clone(),
                // Default Value.
                v.default_value.clone(),
                // Desc.
                DataValue::String(Some(v.desc.as_bytes().to_vec())),
            ]);
            result.push(res);
        }
        result
    }

    pub fn set_settings(&self, key: String, val: String) -> Result<()> {
        let setting = self.check_and_get_setting_value(&key)?;

        match setting.user_setting.value.data_type() {
            DataType::UInt64 => {
                let u64_val = val.parse::<u64>()?;
                self.try_set_u64(&key, u64_val)?;
            }
            v => {
                return Err(ErrorCode::UnknownVariable(format!(
                    "Unsupported variable:{:?} type:{:?} when set_settings().",
                    key, v
                )))
            }
        }

        Ok(())
    }
}
