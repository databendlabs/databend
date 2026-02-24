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
use databend_common_config::CacheConfig;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::ClientSessionMgr;
use databend_common_management::ConnectionMgr;
use databend_common_management::FileFormatMgr;
use databend_common_management::NetworkPolicyMgr;
use databend_common_management::PasswordPolicyMgr;
use databend_common_management::ProcedureMgr;
use databend_common_management::QuotaApi;
use databend_common_management::QuotaMgr;
use databend_common_management::RoleMgr;
use databend_common_management::SettingMgr;
use databend_common_management::StageApi;
use databend_common_management::StageMgr;
use databend_common_management::UserApi;
use databend_common_management::UserMgr;
use databend_common_management::task::TaskMgr;
use databend_common_management::udf::UdfMgr;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_meta_client::RpcClientConf;
use databend_meta_kvapi::kvapi;
use databend_meta_plugin_cache::Cache;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use log::debug;
use tokio::sync::Mutex;

use crate::BUILTIN_ROLE_PUBLIC;
use crate::builtin::BuiltIn;

pub struct UserApiProvider {
    meta: MetaStore,
    client: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,

    /// Optional cache for ownership information for all keys [`TenantOwnershipObjectIdent`] in meta-service.
    ///
    /// All the key-values under "__fd_object_owners/" will be cached.
    /// This instance is shared between all `role_api()` return values.
    ///
    /// When UserApiProvider is created, the cache will be created and initialized.
    /// The `Cache` instance internally stores the status about if databend-meta service supports cache.
    /// If not, every access returns [`Unsupported`] error.
    ownership_cache: Option<Arc<Mutex<Cache>>>,

    builtin: BuiltIn,
}

impl UserApiProvider {
    #[async_backtrace::framed]
    pub async fn init(
        conf: RpcClientConf,
        cache_config: &CacheConfig,
        builtin: BuiltIn,
        tenant: &Tenant,
        quota: Option<TenantQuota>,
    ) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf, cache_config, builtin, tenant).await?);
        let user_mgr = UserApiProvider::instance();

        if let Some(q) = quota {
            let i = user_mgr.tenant_quota_api(tenant);
            let res = i.get_quota(MatchSeq::GE(0)).await?;
            i.set_quota(&q, MatchSeq::Exact(res.seq)).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        conf: RpcClientConf,
        cache_config: &CacheConfig,
        builtin: BuiltIn,
        tenant: &Tenant,
    ) -> Result<Arc<UserApiProvider>> {
        let meta_store = MetaStoreProvider::new(conf)
            .create_meta_store::<DatabendRuntime>()
            .await
            .map_err(|e| {
                ErrorCode::MetaServiceError(e.to_string())
                    .add_message_back("(while create meta store)")
            })?;

        let client = meta_store.inner().clone();

        let cache = if cache_config.meta_service_ownership_cache {
            let cache = RoleMgr::new_cache(client.clone()).await;
            let cache = Arc::new(Mutex::new(cache));
            Some(cache)
        } else {
            None
        };

        let user_mgr = UserApiProvider {
            meta: meta_store.clone(),
            client: meta_store.arc(),
            ownership_cache: cache,
            builtin,
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
            let public = RoleInfo::new(BUILTIN_ROLE_PUBLIC, None);
            user_mgr
                .add_role(tenant, public, &CreateOption::CreateIfNotExists)
                .await?;
        }

        Ok(Arc::new(user_mgr))
    }

    #[async_backtrace::framed]
    pub async fn try_create_simple(
        conf: RpcClientConf,
        tenant: &Tenant,
    ) -> Result<Arc<UserApiProvider>> {
        Self::try_create(conf, &CacheConfig::default(), BuiltIn::default(), tenant).await
    }

    pub fn instance() -> Arc<UserApiProvider> {
        GlobalInstance::get()
    }

    pub fn udf_api(&self, tenant: &Tenant) -> UdfMgr {
        UdfMgr::create(self.client.clone(), tenant)
    }

    pub fn task_api(&self, tenant: &Tenant) -> TaskMgr {
        TaskMgr::create(self.client.clone(), tenant)
    }

    pub fn user_api(&self, tenant: &Tenant) -> Arc<impl UserApi> {
        let user_mgr = UserMgr::create(self.client.clone(), tenant);
        Arc::new(user_mgr)
    }

    pub fn role_api(&self, tenant: &Tenant) -> RoleMgr {
        let role_mgr = RoleMgr::create(
            self.client.clone(),
            tenant,
            GlobalConfig::instance().query.upgrade_to_pb,
            self.ownership_cache.clone(),
        );
        debug!("RoleMgr created");
        role_mgr
    }

    pub fn stage_api(&self, tenant: &Tenant) -> Arc<dyn StageApi> {
        Arc::new(StageMgr::create(self.client.clone(), tenant))
    }

    pub fn file_format_api(&self, tenant: &Tenant) -> FileFormatMgr {
        FileFormatMgr::create(self.client.clone(), tenant)
    }

    pub fn connection_api(&self, tenant: &Tenant) -> ConnectionMgr {
        ConnectionMgr::create(self.client.clone(), tenant)
    }

    pub fn tenant_quota_api(&self, tenant: &Tenant) -> Arc<dyn QuotaApi> {
        const WRITE_PB: bool = false;
        Arc::new(QuotaMgr::<WRITE_PB>::create(self.client.clone(), tenant))
    }

    pub fn setting_api(&self, tenant: &Tenant) -> SettingMgr {
        SettingMgr::create(self.client.clone(), tenant)
    }
    pub fn procedure_api(&self, tenant: &Tenant) -> ProcedureMgr {
        ProcedureMgr::create(self.client.clone(), tenant)
    }

    pub fn network_policy_api(&self, tenant: &Tenant) -> NetworkPolicyMgr {
        NetworkPolicyMgr::create(self.client.clone(), tenant)
    }

    pub fn password_policy_api(&self, tenant: &Tenant) -> PasswordPolicyMgr {
        PasswordPolicyMgr::create(self.client.clone(), tenant)
    }

    pub fn client_session_api(&self, tenant: &Tenant) -> ClientSessionMgr {
        ClientSessionMgr::create(self.client.clone(), tenant)
    }

    pub fn get_meta_store_client(&self) -> Arc<MetaStore> {
        Arc::new(self.meta.clone())
    }

    pub fn get_configured_user(&self, user_name: &str) -> Option<&AuthInfo> {
        self.builtin.users.get(user_name)
    }

    pub fn get_configured_users(&self) -> HashMap<String, AuthInfo> {
        self.builtin.users.clone()
    }

    pub fn get_configured_udf(&self, udf_name: &str) -> Option<UserDefinedFunction> {
        self.builtin.udfs.get(udf_name).cloned()
    }

    pub fn get_configured_udfs(&self) -> HashMap<String, UserDefinedFunction> {
        self.builtin.udfs.clone()
    }
}
