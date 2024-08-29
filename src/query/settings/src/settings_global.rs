// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MatchSeq;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::settings::ChangeValue;
use crate::settings::Settings;
use crate::settings_default::DefaultSettings;
use crate::ScopeLevel;

impl Settings {
    #[async_backtrace::framed]
    pub async fn load_settings(
        user_api: Arc<UserApiProvider>,
        tenant: &Tenant,
    ) -> Result<Vec<UserSetting>> {
        user_api.setting_api(tenant).get_settings().await
    }

    #[async_backtrace::framed]
    pub async fn try_drop_global_setting(&self, key: &str) -> Result<()> {
        self.changes.remove(key);

        UserApiProvider::instance()
            .setting_api(&self.tenant)
            .try_drop_setting(key, MatchSeq::GE(1))
            .await
    }

    #[async_backtrace::framed]
    pub async fn set_global_setting(&self, k: String, v: String) -> Result<()> {
        let (key, value) = DefaultSettings::convert_value(k.clone(), v)?;
        self.changes.insert(key.clone(), ChangeValue {
            value: value.clone(),
            level: ScopeLevel::Global,
        });

        UserApiProvider::instance()
            .set_setting(&self.tenant, UserSetting { name: key, value })
            .await?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn load_changes(&self) -> Result<()> {
        self.load_config_changes()?;
        self.load_global_changes().await
    }

    fn load_config_changes(&self) -> Result<()> {
        let query_config = &GlobalConfig::instance().query;
        if let Some(parquet_fast_read_bytes) = query_config.parquet_fast_read_bytes {
            self.set_parquet_fast_read_bytes(parquet_fast_read_bytes)?;
        }

        if let Some(max_storage_io_requests) = query_config.max_storage_io_requests {
            self.set_max_storage_io_requests(max_storage_io_requests)?;
        }

        if let Some(enterprise_license_key) = query_config.databend_enterprise_license.clone() {
            unsafe {
                self.set_enterprise_license(enterprise_license_key)?;
            }
        }
        Ok(())
    }

    async fn load_global_changes(&self) -> Result<()> {
        let default_settings = DefaultSettings::instance()?;

        let api = UserApiProvider::instance();
        let global_settings = Settings::load_settings(api, &self.tenant).await?;

        for global_setting in global_settings {
            let name = global_setting.name;
            let val = global_setting.value.as_string();

            self.changes
                .insert(name.clone(), match default_settings.settings.get(&name) {
                    None => {
                        // the settings may be deprecated
                        warn!("Ignore deprecated global setting {} = {}", name, val);
                        continue;
                    }
                    Some(default_setting_value) => match &default_setting_value.value {
                        UserSettingValue::UInt64(_) => ChangeValue {
                            level: ScopeLevel::Global,
                            value: UserSettingValue::UInt64(val.parse::<u64>()?),
                        },
                        UserSettingValue::String(_) => ChangeValue {
                            level: ScopeLevel::Global,
                            value: UserSettingValue::String(val.clone()),
                        },
                    },
                });
        }

        Ok(())
    }
}
