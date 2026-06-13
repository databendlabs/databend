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
use databend_common_users::UserApiProvider;
use log::warn;

use crate::ScopeLevel;
use crate::SettingScope;
use crate::settings::ChangeValue;
use crate::settings::Settings;
use crate::settings_default::DefaultSettings;

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
            .try_drop_setting(key)
            .await
    }

    #[async_backtrace::framed]
    pub async fn set_global_setting(&self, k: String, v: String) -> Result<()> {
        DefaultSettings::check_setting_scope(&k, SettingScope::Global)?;
        let (key, value) = DefaultSettings::convert_value(k.clone(), v)?;
        self.changes.insert(key.clone(), ChangeValue {
            value: value.clone(),
            level: ScopeLevel::Global,
        });

        UserApiProvider::instance()
            .setting_api(&self.tenant)
            .set_setting(UserSetting { name: key, value })
            .await?;
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn load_changes(&self) -> Result<()> {
        self.load_config_changes()?;
        self.load_global_changes().await
    }

    fn apply_local_config(&self, k: &str, v: String) -> Result<()> {
        let (key, value) = DefaultSettings::convert_value(k.to_string(), v)?;
        self.changes.insert(key.clone(), ChangeValue {
            value: value.clone(),
            level: ScopeLevel::Local,
        });
        Ok(())
    }

    fn load_config_changes(&self) -> Result<()> {
        let query_config = &GlobalConfig::instance().query;
        if let Some(val) = query_config.common.parquet_fast_read_bytes {
            self.apply_local_config("parquet_fast_read_bytes", val.to_string())?;
        }

        if let Some(val) = query_config.common.max_storage_io_requests {
            self.apply_local_config("max_storage_io_requests", val.to_string())?;
        }

        if let Some(val) = query_config.common.databend_enterprise_license.clone() {
            self.apply_local_config("enterprise_license", val)?;
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
                        warn!(
                            "[SETTINGS] Ignoring deprecated global setting: {} = {}",
                            name, val
                        );
                        continue;
                    }
                    Some(default_setting_value) => {
                        if DefaultSettings::check_setting_scope(&name, SettingScope::Global)
                            .is_err()
                        {
                            // the setting is session only, ignore the global setting
                            warn!(
                                "[SETTINGS] Ignoring session-only setting at global scope: {} = {}",
                                name, val
                            );
                            continue;
                        }
                        match &default_setting_value.value {
                            UserSettingValue::UInt64(_) => ChangeValue {
                                level: ScopeLevel::Global,
                                value: UserSettingValue::UInt64(val.parse::<u64>()?),
                            },
                            UserSettingValue::String(_) => ChangeValue {
                                level: ScopeLevel::Global,
                                value: UserSettingValue::String(val.clone()),
                            },
                        }
                    }
                });
        }

        Ok(())
    }
}
