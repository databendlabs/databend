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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MatchSeq;

use crate::UserApiProvider;

impl UserApiProvider {
    // Set a setting.
    #[async_backtrace::framed]
    pub async fn set_setting(&self, tenant: &Tenant, setting: UserSetting) -> Result<u64> {
        let setting_api_provider = self.setting_api(tenant);
        setting_api_provider.set_setting(setting).await
    }

    // Get all settings list.
    #[async_backtrace::framed]
    pub async fn get_settings(&self, tenant: &Tenant) -> Result<Vec<UserSetting>> {
        let setting_api_provider = self.setting_api(tenant);
        setting_api_provider.get_settings().await
    }

    // Drop a setting by name.
    #[async_backtrace::framed]
    pub async fn drop_setting(&self, tenant: &Tenant, name: &str) -> Result<()> {
        let setting_api_provider = self.setting_api(tenant);
        setting_api_provider
            .try_drop_setting(name, MatchSeq::GE(1))
            .await
    }
}
