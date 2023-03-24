use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSetting;
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

    pub fn set_global_setting(&self, k: String, v: String) -> Result<()> {
        if let (key, Some(value)) = DefaultSettings::convert_value(k, v)? {
            self.changes.insert(key.clone(), ChangeValue {
                value: value.clone(),
                level: ScopeLevel::Global,
            });

            UserApiProvider::instance().set_setting(&self.tenant, UserSetting { name: key, value })
        }

        Ok(())
    }
}
