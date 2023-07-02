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

use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_exception::Result;
use common_grpc::RpcClientConf;
use common_management::FileFormatApi;
use common_management::FileFormatMgr;
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
use common_meta_app::principal::AuthInfo;
use common_meta_app::tenant::TenantQuota;
use common_meta_kvapi::kvapi;
use common_meta_store::MetaStore;
use common_meta_store::MetaStoreProvider;
use common_meta_types::MatchSeq;
use common_meta_types::MetaError;

use crate::idm_config::IDMConfig;

pub struct UserApiProvider {
    meta: MetaStore,
    client: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    idm_config: IDMConfig,
}

impl UserApiProvider {
    #[async_backtrace::framed]
    pub async fn init(
        conf: RpcClientConf,
        idm_config: IDMConfig,
        tenant: &str,
        quota: Option<TenantQuota>,
    ) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf, idm_config).await?);
        if let Some(q) = quota {
            let i = UserApiProvider::instance().get_tenant_quota_api_client(tenant)?;
            let res = i.get_quota(MatchSeq::GE(0)).await?;
            i.set_quota(&q, MatchSeq::Exact(res.seq)).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        conf: RpcClientConf,
        idm_config: IDMConfig,
    ) -> Result<Arc<UserApiProvider>> {
        let client = MetaStoreProvider::new(conf).create_meta_store().await?;
        Ok(Arc::new(UserApiProvider {
            meta: client.clone(),
            client: client.arc(),
            idm_config,
        }))
    }

    #[async_backtrace::framed]
    pub async fn try_create_simple(conf: RpcClientConf) -> Result<Arc<UserApiProvider>> {
        Self::try_create(conf, IDMConfig::default()).await
    }

    pub fn instance() -> Arc<UserApiProvider> {
        GlobalInstance::get()
    }

    pub fn get_user_api_client(&self, tenant: &str) -> Result<Arc<impl UserApi>> {
        Ok(Arc::new(UserMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_role_api_client(&self, tenant: &str) -> Result<Arc<impl RoleApi>> {
        Ok(Arc::new(RoleMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_stage_api_client(&self, tenant: &str) -> Result<Arc<dyn StageApi>> {
        Ok(Arc::new(StageMgr::create(self.client.clone(), tenant)?))
    }

    pub fn get_file_format_api_client(&self, tenant: &str) -> Result<Arc<dyn FileFormatApi>> {
        Ok(Arc::new(FileFormatMgr::create(
            self.client.clone(),
            tenant,
        )?))
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

    pub fn get_configured_user(&self, user_name: &str) -> Option<&AuthInfo> {
        self.idm_config.users.get(user_name)
    }

    pub fn get_configured_users(&self) -> HashMap<String, AuthInfo> {
        self.idm_config.users.clone()
    }
}
