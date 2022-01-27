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

use common_exception::Result;
use common_meta_types::UserSetting;

use crate::users::UserApiProvider;

impl UserApiProvider {
    pub async fn set_setting(
        &self,
        tenant: &str,
        cluster: &str,
        setting: UserSetting,
    ) -> Result<u64> {
        let setting_api_provider = self.get_setting_api_client(tenant, cluster);
        setting_api_provider.set_setting(setting).await
    }

    // Get the tenant/cluster all settings list.
    pub async fn get_settings(&self, tenant: &str, cluster: &str) -> Result<Vec<UserSetting>> {
        let setting_api_provider = self.get_setting_api_client(tenant, cluster);
        let get_settings = setting_api_provider.get_settings();

        match get_settings.await {
            Err(e) => Err(e.add_message_back("(while get settings).")),
            Ok(seq_settings) => Ok(seq_settings),
        }
    }

    // Drop a settings by name.
    pub async fn drop_setting(&self, tenant: &str, cluster: &str, name: &str) -> Result<()> {
        let setting_api_provider = self.get_setting_api_client(tenant, cluster);
        setting_api_provider.drop_setting(name, None).await
    }
}
