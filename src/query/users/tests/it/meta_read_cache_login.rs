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

//! The password retry after a rejection served from a lagging meta read cache.

use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_management::UserApi;
use databend_common_management::UserMgr;
use databend_common_management::meta_read_cache::CachedPrefix;
use databend_common_management::meta_read_cache::MetaReadCache;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_users::UserApiProvider;
use databend_common_users::builtin::BuiltIn;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::MetaError;
use databend_meta_runtime::DatabendRuntime;

const CONFIGURED: &str = "configured";

fn password(p: &str) -> AuthInfo {
    AuthInfo::Password {
        hash_value: PasswordHashMethod::Sha256.hash(p.as_bytes()),
        hash_method: PasswordHashMethod::Sha256,
        need_change: false,
    }
}

fn user(name: &str, p: &str, option: UserOption) -> UserInfo {
    let mut user = UserInfo::new(name, "%", password(p));
    user.update_auth_option(None, Some(option));
    user
}

fn user_mgr(meta: MetaStore, tenant: &Tenant) -> UserMgr {
    let kv: Arc<dyn KVApi<Error = MetaError>> = Arc::new(meta);
    UserMgr::create(kv, tenant)
}

struct Lagging {
    api: Arc<UserApiProvider>,
    tenant: Tenant,
    /// What the meta read cache sees: a meta-service the watch stream has stopped following.
    cached: UserMgr,
    /// The current state in meta-service, written without fences, as by another node.
    current: UserMgr,
}

/// A [`UserApiProvider`] whose meta read cache watches a separate store, so a user written
/// there differs from the one in meta-service until the test writes both.
async fn lagging() -> anyhow::Result<Lagging> {
    let thread_name = std::thread::current().name().unwrap().to_string();
    GlobalInstance::init_testing(&thread_name);
    GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
    let tenant = Tenant::new_literal("test");

    let watched = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let cache = MetaReadCache::create(
        watched.clone(),
        &tenant,
        &[CachedPrefix::User],
        Duration::from_secs(3600),
    )
    .await?;
    GlobalInstance::set(cache);

    let mut builtin = BuiltIn::default();
    builtin
        .users
        .insert(CONFIGURED.to_string(), password("configured-pwd"));
    let api = UserApiProvider::try_create(
        RpcClientConf::empty(),
        &CacheConfig::default(),
        builtin,
        &tenant,
    )
    .await?;
    let current = user_mgr(api.get_meta_store_client().as_ref().clone(), &tenant);
    Ok(Lagging {
        cached: user_mgr(watched, &tenant),
        current,
        api,
        tenant,
    })
}

impl Lagging {
    /// Write `cached` where the cache sees it and `current` to meta-service; return the user
    /// once the cache serves `cached`.
    async fn write(&self, cached: UserInfo, current: UserInfo) -> anyhow::Result<UserInfo> {
        assert!(
            self.cached
                .create_user(cached.clone(), false)
                .await?
                .is_ok()
        );
        assert!(self.current.create_user(current, false).await?.is_ok());
        let identity = UserIdentity::new(&cached.name, "%");
        for _ in 0..200 {
            let got = self.api.get_user(&self.tenant, identity.clone()).await;
            if let Ok(got) = got
                && got.auth_info == cached.auth_info
            {
                return Ok(got);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the cache does not serve {}", cached.name);
    }

    async fn refresh(
        &self,
        rejected: &UserInfo,
        ip: &str,
    ) -> databend_common_exception::Result<Option<UserInfo>> {
        let identity = UserIdentity::new(&rejected.name, "%");
        self.api
            .refresh_rejected_user(&self.tenant, identity, rejected, Some(ip))
            .await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_refresh_rejected_user() -> anyhow::Result<()> {
    let s = lagging().await?;

    // The password changed on another node: the fresh record is returned.
    let stale = s
        .write(
            user("u1", "old", UserOption::empty()),
            user("u1", "new", UserOption::empty()),
        )
        .await?;
    let fresh = s.refresh(&stale, "10.0.0.1").await?.unwrap();
    assert_eq!(fresh.auth_info, password("new"));

    // Unchanged in meta-service: nothing to retry with.
    assert!(s.refresh(&fresh, "10.0.0.1").await?.is_none());

    // The change also disabled the user: the new password must not log in.
    let stale = s
        .write(
            user("u2", "old", UserOption::empty()),
            user("u2", "new", UserOption::empty().with_disabled(Some(true))),
        )
        .await?;
    let err = s.refresh(&stale, "10.0.0.1").await.unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");

    // The change attached a network policy that does not exist, so no client may log in.
    let stale = s
        .write(
            user("u3", "old", UserOption::empty()),
            user(
                "u3",
                "new",
                UserOption::empty().with_network_policy(Some("missing".to_string())),
            ),
        )
        .await?;
    assert!(s.refresh(&stale, "10.0.0.1").await.is_err());
    Ok(())
}

/// A configured user is never retried against a meta user with the same name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_refresh_skips_configured_user() -> anyhow::Result<()> {
    let s = lagging().await?;
    let meta_user = user(CONFIGURED, "meta-pwd", UserOption::empty());
    assert!(s.current.create_user(meta_user, false).await?.is_ok());

    let identity = UserIdentity::new(CONFIGURED, "%");
    let configured = s.api.get_user(&s.tenant, identity).await?;
    assert_eq!(configured.auth_info, password("configured-pwd"));
    assert!(s.refresh(&configured, "10.0.0.1").await?.is_none());
    Ok(())
}
