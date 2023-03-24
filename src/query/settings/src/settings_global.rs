use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSetting;
use common_meta_app::principal::UserSettingValue;
use common_meta_types::MatchSeq;
use common_users::UserApiProvider;

use crate::settings::ChangeValue;
use crate::settings::NewSettings;
use crate::settings_default::DefaultSettings;
use crate::ScopeLevel;

impl NewSettings {
    pub async fn load_settings(
        user_api: Arc<UserApiProvider>,
        tenant: String,
    ) -> Result<Vec<UserSetting>> {
        user_api
            .get_setting_api_client(&tenant)?
            .get_settings()
            .await
    }

    pub async fn try_drop_global_setting(&self, key: &str) -> Result<()> {
        self.changes.remove(key);

        if !DefaultSettings::has_setting(key) {
            return Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            )));
        }

        UserApiProvider::instance()
            .get_setting_api_client(&self.tenant)?
            .drop_setting(key, MatchSeq::GE(1))
            .await
    }

    pub async fn set_global_setting(&self, k: String, v: String) -> Result<()> {
        if let (key, Some(value)) = DefaultSettings::convert_value(k, v)? {
            self.changes.insert(key.clone(), ChangeValue {
                value: value.clone(),
                level: ScopeLevel::Global,
            });

            UserApiProvider::instance()
                .set_setting(&self.tenant, UserSetting { name: key, value })
                .await?;
        }

        Ok(())
    }

    pub async fn load_global_changes(&self) -> Result<()> {
        let default_settings = DefaultSettings::instance();

        let api = UserApiProvider::instance();
        let global_settings = NewSettings::load_settings(api, self.tenant.clone()).await?;

        for global_setting in global_settings {
            let name = global_setting.name;
            let val = global_setting.value.as_string()?;

            self.changes
                .insert(name.clone(), match default_settings.settings.get(&name) {
                    None => {
                        // the settings may be deprecated
                        tracing::warn!("Ignore deprecated global setting {} = {}", name, val);
                        continue;
                    }
                    Some(default_setting_value) => match &default_setting_value.value {
                        UserSettingValue::UInt64(_) => ChangeValue {
                            level: ScopeLevel::Global,
                            value: UserSettingValue::UInt64(val.parse::<u64>()?),
                        },
                        UserSettingValue::String(v) => ChangeValue {
                            level: ScopeLevel::Global,
                            value: UserSettingValue::String(v.clone()),
                        },
                    },
                });
        }

        Ok(())
    }
}
