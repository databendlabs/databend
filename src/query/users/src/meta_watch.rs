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

//! Watched copies of the metadata that every request reads, by `cache.meta_service_watch_cache`:
//! the users, for authentication, and the global settings, for new sessions. Like every
//! [`WatchedPrefix`], they lag a change by the watch delay: only those reads use them.

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::SettingMgr;
use databend_common_management::errors::meta_service_error;
use databend_common_meta_api::kv_pb_api::decode_seqv;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::SettingIdent;
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::ClientHandle;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_plugin_cache::WatchedPrefix;
use databend_meta_runtime::DatabendRuntime;

type KvApi = dyn kvapi::KVApi<Error = MetaError> + Send + Sync;

/// The watched metadata of one tenant: the tenant of this node.
pub(crate) struct MetaWatches {
    tenant: Tenant,
    users: WatchedPrefix,
    settings: WatchedPrefix,
}

impl MetaWatches {
    pub(crate) async fn new(client: Arc<ClientHandle<DatabendRuntime>>, tenant: &Tenant) -> Self {
        let users = TenantUserIdent::new_user_host(tenant.clone(), "", "").tenant_prefix();
        let settings = SettingIdent::new(tenant.clone(), "").to_string_key();
        MetaWatches {
            tenant: tenant.clone(),
            users: WatchedPrefix::new(client.clone(), &users, "users").await,
            settings: WatchedPrefix::new(client, &settings, "global settings").await,
        }
    }

    /// The watches of `tenant`, if it is the tenant they watch.
    pub(crate) fn of<'a>(watches: &'a Option<Self>, tenant: &Tenant) -> Option<&'a Self> {
        watches.as_ref().filter(|w| &w.tenant == tenant)
    }

    pub(crate) async fn get_user(&self, kv: &KvApi, user: &UserIdentity) -> Result<UserInfo> {
        let ident =
            TenantUserIdent::new_user_host(self.tenant.clone(), &user.username, &user.hostname);
        let key = ident.to_string_key();
        let value = self
            .users
            .mget_kv(kv, std::slice::from_ref(&key))
            .await
            .map_err(meta_service_error)?
            .pop()
            .flatten()
            .ok_or_else(|| {
                ErrorCode::UnknownUser(format!("User {} does not exist.", user.display()))
            })?;
        let user = decode_seqv::<UserInfo>(value, || format!("decode value of {key}"))
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;
        Ok(user.data)
    }

    pub(crate) async fn get_settings(&self, kv: &KvApi) -> Result<Vec<UserSetting>> {
        self.settings
            .list_kv(kv, self.settings.prefix())
            .await
            .map_err(meta_service_error)?
            .into_iter()
            .map(|(_, value)| SettingMgr::decode_setting(&value.data))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use databend_common_base::base::GlobalInstance;
    use databend_common_config::CacheConfig;
    use databend_common_config::GlobalConfig;
    use databend_common_config::InnerConfig;
    use databend_common_meta_app::principal::AuthInfo;
    use databend_common_meta_app::principal::UserSettingValue;
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_version::BUILD_INFO;
    use databend_meta_client::RpcClientConf;
    use databend_meta_client::kvapi::KVStream;
    use databend_meta_client::kvapi::ListOptions;
    use databend_meta_client::types::Change;
    use databend_meta_client::types::TxnReply;
    use databend_meta_client::types::TxnRequest;
    use databend_meta_client::types::UpsertKV;
    use databend_meta_client::types::protobuf::StreamItem;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use futures::stream;
    use futures::stream::BoxStream;

    use super::*;
    use crate::UserApiProvider;
    use crate::builtin::BuiltIn;

    /// A meta-service without keys: a read it answers finds nothing.
    struct EmptyKv;

    #[async_trait]
    impl kvapi::KVApi for EmptyKv {
        type Error = MetaError;

        async fn upsert_kv(
            &self,
            _req: UpsertKV,
        ) -> std::result::Result<Change<Vec<u8>>, MetaError> {
            unreachable!("watched reads do not write")
        }

        async fn get_many_kv(
            &self,
            keys: BoxStream<'static, std::result::Result<String, MetaError>>,
        ) -> std::result::Result<KVStream<MetaError>, MetaError> {
            Ok(keys.map_ok(|key| StreamItem::new(key, None)).boxed())
        }

        async fn list_kv(
            &self,
            _opts: ListOptions<'_, str>,
        ) -> std::result::Result<KVStream<MetaError>, MetaError> {
            Ok(stream::empty().boxed())
        }

        async fn transaction(&self, _txn: TxnRequest) -> std::result::Result<TxnReply, MetaError> {
            unreachable!("watched reads do not write")
        }
    }

    /// Reads that find the written user and setting with `EmptyKv` were answered by the copies:
    /// their prefixes and keys match those of `UserMgr` and `SettingMgr`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_watched_users_and_settings() -> anyhow::Result<()> {
        let thread_name = std::thread::current().name().unwrap().to_string();
        GlobalInstance::init_testing(&thread_name);
        GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
        let tenant = Tenant::new_literal("test");
        let cache = CacheConfig {
            meta_service_watch_cache: true,
            ..Default::default()
        };
        let provider = UserApiProvider::try_create(
            RpcClientConf::empty(),
            &cache,
            BuiltIn::default(),
            &tenant,
        )
        .await?;
        let user = UserInfo::new("u1", "%", AuthInfo::None);
        provider
            .create_user(&tenant, user.clone(), &CreateOption::Create)
            .await?;
        let setting = UserSetting::create("max_threads", UserSettingValue::UInt64(3));
        provider
            .setting_api(&tenant)
            .set_setting(setting.clone())
            .await?;

        assert!(MetaWatches::of(&provider.meta_watches, &Tenant::new_literal("other")).is_none());
        let watches = MetaWatches::of(&provider.meta_watches, &tenant).unwrap();
        let mut answered = false;
        for _ in 0..100 {
            let got_user = watches.get_user(&EmptyKv, &user.identity()).await;
            let got_settings = watches.get_settings(&EmptyKv).await?;
            if let Ok(got_user) = got_user {
                if got_settings == vec![setting.clone()] {
                    assert_eq!(got_user, user);
                    answered = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(answered, "the watched copies did not answer");

        let missing = watches
            .get_user(&EmptyKv, &UserIdentity::new("u2", "%"))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), ErrorCode::UNKNOWN_USER);

        // Both entry points agree with the direct reads.
        let login = provider
            .get_login_user_with_client_ip(&tenant, user.identity(), None)
            .await?;
        assert_eq!(login, provider.get_user(&tenant, user.identity()).await?);
        assert_eq!(
            provider.get_session_settings(&tenant).await?,
            provider.setting_api(&tenant).get_settings().await?
        );
        Ok(())
    }
}
