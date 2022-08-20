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

use common_base::base::Singleton;
use common_exception::Result;
use common_grpc::RpcClientConf;
use common_management::QuotaApi;
use common_management::QuotaMgr;
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
use common_meta_store::MetaStore;
use common_meta_store::MetaStoreProvider;
use once_cell::sync::OnceCell;

pub struct UserApiProvider {
    meta: MetaStore,
    client: Arc<dyn KVApi>,
}

static USER_API_PROVIDER: OnceCell<Singleton<Arc<UserApiProvider>>> = OnceCell::new();

impl UserApiProvider {
    pub async fn init(
        conf: RpcClientConf,
        v: Singleton<Arc<UserApiProvider>>,
    ) -> Result<()> {
        v.init(Self::try_create(conf).await?)?;

        USER_API_PROVIDER.set(v).ok();
        Ok(())
    }

    pub async fn try_create(conf: RpcClientConf) -> Result<Arc<UserApiProvider>> {
        let client = MetaStoreProvider::new(conf).try_get_meta_store().await?;
        Ok(Arc::new(UserApiProvider {
            meta: client.clone(),
            client: client.arc(),
        }))
    }

    pub fn instance() -> Arc<UserApiProvider> {
        match USER_API_PROVIDER.get() {
            None => panic!("UserApiProvider is not init"),
            Some(user_api_provider) => user_api_provider.get(),
        }
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

    pub fn get_tenant_quota_api_client(&self, tenant: &str) -> Result<Arc<dyn QuotaApi>> {
        Ok(Arc::new(QuotaMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_setting_api_client(&self, tenant: &str) -> Result<Arc<dyn SettingApi>> {
        Ok(Arc::new(SettingMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_meta_store_client(&self) -> Arc<MetaStore> {
        Arc::new(self.meta.clone())
    }
}
