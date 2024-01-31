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

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_management::ConnectionApi;
use databend_common_management::ConnectionMgr;
use databend_common_management::FileFormatApi;
use databend_common_management::FileFormatMgr;
use databend_common_management::NetworkPolicyApi;
use databend_common_management::NetworkPolicyMgr;
use databend_common_management::PasswordPolicyApi;
use databend_common_management::PasswordPolicyMgr;
use databend_common_management::QuotaApi;
use databend_common_management::QuotaMgr;
use databend_common_management::RoleApi;
use databend_common_management::RoleMgr;
use databend_common_management::SettingApi;
use databend_common_management::SettingMgr;
use databend_common_management::StageApi;
use databend_common_management::StageMgr;
use databend_common_management::UdfApi;
use databend_common_management::UdfMgr;
use databend_common_management::UserApi;
use databend_common_management::UserMgr;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;

use crate::idm_config::IDMConfig;
use crate::BUILTIN_ROLE_PUBLIC;

pub struct UserApiProvider {
    meta: MetaStore,
    client: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
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
        GlobalInstance::set(Self::try_create(conf, idm_config, tenant).await?);
        let user_mgr = UserApiProvider::instance();
        if let Some(q) = quota {
            let i = user_mgr.get_tenant_quota_api_client(tenant)?;
            let res = i.get_quota(MatchSeq::GE(0)).await?;
            i.set_quota(&q, MatchSeq::Exact(res.seq)).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        conf: RpcClientConf,
        idm_config: IDMConfig,
        tenant: &str,
    ) -> Result<Arc<UserApiProvider>> {
        let client = MetaStoreProvider::new(conf).create_meta_store().await?;
        let user_mgr = UserApiProvider {
            meta: client.clone(),
            client: client.arc(),
            idm_config,
        };

        // init built-in role
        // Currently we have two builtin roles:
        // 1. ACCOUNT_ADMIN, which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`,
        //    it also contains all roles. ACCOUNT_ADMIN can access the data objects which owned by any role.
        // 2. PUBLIC, on the other side only includes the public accessible privileges, but every role
        //    contains the PUBLIC role. The data objects which owned by PUBLIC can be accessed by any role.
        // But we only can set PUBLIC role into meta.
        // Because the previous deserialization using from_bits caused forward compatibility issues after adding new privilege type
        // Until we can confirm all product use https://github.com/datafuselabs/databend/releases/tag/v1.2.321-nightly or later,
        // We can add account_admin into meta.
        {
            let public = RoleInfo::new(BUILTIN_ROLE_PUBLIC);
            user_mgr.add_role(tenant, public, true).await?;
        }

        Ok(Arc::new(user_mgr))
    }

    #[async_backtrace::framed]
    pub async fn try_create_simple(
        conf: RpcClientConf,
        tenant: &str,
    ) -> Result<Arc<UserApiProvider>> {
        Self::try_create(conf, IDMConfig::default(), tenant).await
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

    pub fn get_connection_api_client(&self, tenant: &str) -> Result<Arc<dyn ConnectionApi>> {
        Ok(Arc::new(ConnectionMgr::create(
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

    pub fn get_network_policy_api_client(
        &self,
        tenant: &str,
    ) -> Result<Arc<impl NetworkPolicyApi>> {
        Ok(Arc::new(NetworkPolicyMgr::create(
            self.client.clone(),
            tenant,
        )?))
    }

    pub fn get_password_policy_api_client(
        &self,
        tenant: &str,
    ) -> Result<Arc<impl PasswordPolicyApi>> {
        Ok(Arc::new(PasswordPolicyMgr::create(
            self.client.clone(),
            tenant,
        )?))
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
