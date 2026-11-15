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
use std::time::Duration;

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

/// The global setting that turns the meta read cache on and gives its TTL.
const META_READ_CACHE_TTL_SECS: &str = "meta_read_cache_ttl_secs";

/// The TTL given by the global value of [`META_READ_CACHE_TTL_SECS`]; absent or unreadable
/// means off.
fn meta_read_cache_ttl(value: Option<&UserSettingValue>) -> Duration {
    let secs = value.and_then(|v| v.as_string().parse::<u64>().ok());
    Duration::from_secs(secs.unwrap_or(0))
}

impl Settings {
    #[async_backtrace::framed]
    pub async fn load_settings(
        user_api: Arc<UserApiProvider>,
        tenant: &Tenant,
    ) -> Result<Vec<UserSetting>> {
        // A plain read of the global settings of every new session; may come from the meta read
        // cache.
        let epoch = user_api.meta_read_cache_ttl_epoch();
        let settings = user_api.cached_setting_api(tenant).get_settings().await?;

        // The cache is controlled by one of the settings just read. While it is off, every
        // session reads meta-service, so turning it on is seen at once; while it is on, a
        // change of the TTL is seen when the cached settings expire. A `SET GLOBAL` or
        // `UNSET GLOBAL` of it on this node during the read wins over what the read returned.
        let ttl = settings
            .iter()
            .find(|s| s.name == META_READ_CACHE_TTL_SECS)
            .map(|s| &s.value);
        user_api.set_meta_read_cache_ttl_as_read(tenant, epoch, meta_read_cache_ttl(ttl));
        Ok(settings)
    }

    #[async_backtrace::framed]
    pub async fn try_drop_global_setting(&self, key: &str) -> Result<()> {
        self.changes.remove(key);

        let user_api = UserApiProvider::instance();
        user_api
            .setting_api(&self.tenant)
            .try_drop_setting(key)
            .await?;
        if key == META_READ_CACHE_TTL_SECS {
            user_api.set_meta_read_cache_ttl(&self.tenant, meta_read_cache_ttl(None));
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn set_global_setting(&self, k: String, v: String) -> Result<()> {
        DefaultSettings::check_setting_scope(&k, SettingScope::Global)?;
        let (key, value) = DefaultSettings::convert_value(k.clone(), v)?;
        self.changes.insert(key.clone(), ChangeValue {
            value: value.clone(),
            level: ScopeLevel::Global,
        });

        let user_api = UserApiProvider::instance();
        user_api
            .setting_api(&self.tenant)
            .set_setting(UserSetting {
                name: key.clone(),
                value: value.clone(),
            })
            .await?;
        // On this node at once; other nodes follow as described in `load_settings`.
        if key == META_READ_CACHE_TTL_SECS {
            user_api.set_meta_read_cache_ttl(&self.tenant, meta_read_cache_ttl(Some(&value)));
        }
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
