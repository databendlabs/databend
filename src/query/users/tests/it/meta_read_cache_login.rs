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

//! Login when the user answered from the meta read cache is stale.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_management::UserApi;
use databend_common_management::UserMgr;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::PublicKeyEntry;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::UserApiProvider;
use databend_common_users::builtin::BuiltIn;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::MetaError;

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

struct Cached {
    api: Arc<UserApiProvider>,
    tenant: Tenant,
    /// Writes to meta-service as another node does: this node's cache is not told.
    other_node: UserMgr,
}

/// A [`UserApiProvider`] that keeps what it reads for an hour.
async fn cached() -> anyhow::Result<Cached> {
    let thread_name = std::thread::current().name().unwrap().to_string();
    GlobalInstance::init_testing(&thread_name);
    GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
    let tenant = Tenant::new_literal("test");

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
    api.set_meta_read_cache_ttl(&tenant, Duration::from_secs(3600));
    let kv: Arc<dyn KVApi<Error = MetaError>> = api.get_meta_store_client();
    Ok(Cached {
        other_node: UserMgr::create(kv, &tenant),
        api,
        tenant,
    })
}

impl Cached {
    /// Create `old` and read it, so that it is kept; then another node replaces it by `new`.
    async fn change_on_other_node(&self, old: UserInfo, new: UserInfo) -> anyhow::Result<()> {
        assert!(
            self.other_node
                .create_user(old.clone(), false)
                .await?
                .is_ok()
        );
        let identity = UserIdentity::new(&old.name, "%");
        let kept = self.api.get_user(&self.tenant, identity.clone()).await?;
        assert_eq!(kept.auth_info, old.auth_info);

        assert!(self.other_node.create_user(new, true).await?.is_ok());
        let stale = self.api.get_user(&self.tenant, identity).await?;
        assert_eq!(stale.auth_info, old.auth_info, "the kept user answers");
        Ok(())
    }

    /// Log in with `p` from `ip`, under the global network policy `global_policy`.
    async fn login(
        &self,
        name: &str,
        p: &str,
        ip: &str,
        global_policy: &str,
    ) -> databend_common_exception::Result<(UserInfo, databend_common_exception::Result<bool>)>
    {
        let hash = PasswordHashMethod::Sha256.hash(p.as_bytes());
        let verify = |user: &UserInfo| match &user.auth_info {
            AuthInfo::Password { hash_value, .. } => Ok(*hash_value == hash),
            _ => Ok(false),
        };
        self.api
            .login_with_password(
                &self.tenant,
                UserIdentity::new(name, "%"),
                Some(ip),
                global_policy,
                verify,
            )
            .await
    }

    /// Whether logging in with `p` succeeds.
    async fn logs_in(&self, name: &str, p: &str) -> anyhow::Result<bool> {
        let (_, verified) = self.login(name, p, "10.0.0.1", "").await?;
        Ok(verified?)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_login_after_change_on_other_node() -> anyhow::Result<()> {
    let s = cached().await?;

    // The password changed on another node: the new one logs in, checked on meta-service.
    s.change_on_other_node(
        user("u1", "old", UserOption::empty()),
        user("u1", "new", UserOption::empty()),
    )
    .await?;
    let (fresh, verified) = s.login("u1", "new", "10.0.0.1", "").await?;
    assert!(verified?);
    assert_eq!(fresh.auth_info, password("new"));
    assert!(!s.logs_in("u1", "wrong").await?);

    // The change also disabled the user: the new password must not log in.
    s.change_on_other_node(
        user("u2", "old", UserOption::empty()),
        user("u2", "new", UserOption::empty().with_disabled(Some(true))),
    )
    .await?;
    let err = s.login("u2", "new", "10.0.0.1", "").await.unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");

    // The change attached a network policy that does not exist, so no client may log in.
    s.change_on_other_node(
        user("u3", "old", UserOption::empty()),
        user(
            "u3",
            "new",
            UserOption::empty().with_network_policy(Some("missing".to_string())),
        ),
    )
    .await?;
    assert!(s.login("u3", "new", "10.0.0.1", "").await.is_err());

    // The change revoked account admin, so the global network policy, which does not exist,
    // now applies: no client may log in. The kept account admin was exempt.
    let mut admin = user("u4", "old", UserOption::empty());
    admin
        .grants
        .grant_role(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
    s.change_on_other_node(admin, user("u4", "new", UserOption::empty()))
        .await?;
    assert!(s.login("u4", "old", "10.0.0.1", "missing").await?.1?);
    assert!(s.login("u4", "new", "10.0.0.1", "missing").await.is_err());

    // With the cache off, the user is read from meta-service.
    s.change_on_other_node(
        user("u5", "old", UserOption::empty()),
        user("u5", "new", UserOption::empty()),
    )
    .await?;
    s.api.set_meta_read_cache_ttl(&s.tenant, Duration::ZERO);
    assert!(s.logs_in("u5", "new").await?);
    assert!(!s.logs_in("u5", "old").await?);
    Ok(())
}

/// A kept user that may not log in is checked again on meta-service: another node may have
/// enabled it. This holds for every way of logging in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_login_after_user_enabled_on_other_node() -> anyhow::Result<()> {
    let s = cached().await?;
    let ip = Some("10.0.0.1");

    s.change_on_other_node(
        user("u1", "pwd", UserOption::empty().with_disabled(Some(true))),
        user("u1", "pwd", UserOption::empty()),
    )
    .await?;
    let got = s
        .api
        .get_user_with_client_ip(&s.tenant, UserIdentity::new("u1", "%"), ip)
        .await?;
    assert_eq!(got.option.disabled(), None);
    assert!(s.logs_in("u1", "pwd").await?);

    // Still disabled in meta-service: rejected, with the reason.
    let disabled = user("u2", "pwd", UserOption::empty().with_disabled(Some(true)));
    s.change_on_other_node(disabled.clone(), disabled).await?;
    let err = s
        .api
        .get_user_with_client_ip(&s.tenant, UserIdentity::new("u2", "%"), ip)
        .await
        .unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");

    // Dropped on another node: unknown, as a read of meta-service tells.
    let identity = UserIdentity::new("u2", "%");
    assert!(s.other_node.drop_user(&identity).await?.is_ok());
    let err = s
        .api
        .get_user_with_client_ip(&s.tenant, identity, ip)
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        databend_common_exception::ErrorCode::UNKNOWN_USER
    );
    Ok(())
}

/// The key-pair login asks for the record in meta-service after the kept public keys rejected
/// the token: another node may have replaced them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_refresh_after_public_key_changed_on_other_node() -> anyhow::Result<()> {
    let s = cached().await?;
    let key_pair = |name: &str, key: &str| {
        UserInfo::new(name, "%", AuthInfo::KeyPair {
            public_keys: vec![PublicKeyEntry {
                key: key.to_string(),
                label: String::new(),
                created_at: 0,
            }],
        })
    };
    let ip = Some("10.0.0.1");

    let (old, new) = (key_pair("u1", "old-key"), key_pair("u1", "new-key"));
    s.change_on_other_node(old.clone(), new.clone()).await?;
    let identity = UserIdentity::new("u1", "%");
    let fresh = s
        .api
        .refresh_rejected_user(&s.tenant, identity.clone(), &old, ip)
        .await?;
    assert_eq!(fresh.map(|u| u.auth_info), Some(new.auth_info.clone()));

    // The rejected keys are the ones in meta-service: nothing else to check.
    let fresh = s
        .api
        .refresh_rejected_user(&s.tenant, identity, &new, ip)
        .await?;
    assert_eq!(fresh, None);

    // The change also disabled the user: the new keys must not log in.
    let mut disabled = key_pair("u2", "new-key");
    disabled.update_auth_option(None, Some(UserOption::empty().with_disabled(Some(true))));
    let old = key_pair("u2", "old-key");
    s.change_on_other_node(old.clone(), disabled).await?;
    let err = s
        .api
        .refresh_rejected_user(&s.tenant, UserIdentity::new("u2", "%"), &old, ip)
        .await
        .unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");
    Ok(())
}

/// A user with a password policy is read from meta-service: a lockout recorded by another
/// node rejects the right password here too, and is not cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lockout_on_other_node_is_seen() -> anyhow::Result<()> {
    let s = cached().await?;
    let with_policy = UserOption::empty().with_password_policy(Some("p1".to_string()));
    let lockout_time = Utc::now() + chrono::Duration::hours(1);
    let mut locked = user("u1", "pwd", with_policy.clone());
    locked.update_lockout_time(lockout_time);
    s.change_on_other_node(user("u1", "pwd", with_policy), locked)
        .await?;

    let err = s.login("u1", "pwd", "10.0.0.1", "").await.unwrap_err();
    assert!(err.message().contains("Disable login before"), "{err}");

    let identity = UserIdentity::new("u1", "%");
    let stored = s.other_node.get_user(&identity).await?.unwrap().data;
    assert_eq!(stored.lockout_time, Some(lockout_time));
    Ok(())
}

/// A configured user is never checked against a meta user with the same name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_login_skips_meta_user_for_configured_user() -> anyhow::Result<()> {
    let s = cached().await?;
    let meta_user = user(CONFIGURED, "meta-pwd", UserOption::empty());
    assert!(s.other_node.create_user(meta_user, false).await?.is_ok());

    assert!(!s.logs_in(CONFIGURED, "meta-pwd").await?);
    assert!(s.logs_in(CONFIGURED, "configured-pwd").await?);
    Ok(())
}
