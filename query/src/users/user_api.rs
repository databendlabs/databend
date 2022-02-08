// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_management::RoleApi;
use common_management::RoleMgr;
use common_management::SettingApi;
use common_management::SettingMgr;
use common_management::StageApi;
use common_management::StageMgr;
use common_management::UdfApi;
use common_management::UdfMgr;
use common_management::UserApi;
use common_management::UserMgr;
use common_meta_api::KVApi;

use crate::common::MetaClientProvider;
use crate::configs::Config;

pub struct UserApiProvider {
    client: Arc<dyn KVApi>,
}

impl UserApiProvider {
    pub async fn create_global(conf: Config) -> Result<Arc<UserApiProvider>> {
        let client = MetaClientProvider::new(conf.meta.to_grpc_client_config())
            .try_get_kv_client()
            .await?;

        Ok(Arc::new(UserApiProvider { client }))
    }

    pub fn get_user_api_client(&self, tenant: &str) -> Result<Arc<dyn UserApi>> {
        Ok(Arc::new(UserMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_role_api_client(&self, tenant: &str) -> Result<Arc<dyn RoleApi>> {
        Ok(Arc::new(RoleMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_stage_api_client(&self, tenant: &str) -> Result<Arc<dyn StageApi>> {
        Ok(Arc::new(StageMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_udf_api_client(&self, tenant: &str) -> Result<Arc<dyn UdfApi>> {
        Ok(Arc::new(UdfMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_setting_api_client(&self, tenant: &str) -> Result<Arc<dyn SettingApi>> {
        Ok(Arc::new(SettingMgr::create(self.client.clone(), tenant)?))
    }
}
