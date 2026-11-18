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

use std::time::Duration;

use databend_common_config::CacheConfig;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_config::MetaServiceCache;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_common_users::builtin::BuiltIn;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_manager() -> anyhow::Result<()> {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Init with default.
    {
        GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
    }

    let conf = RpcClientConf::empty();
    let tenant_name = "test";
    let tenant = Tenant::new_literal(tenant_name);

    let user_mgr = UserApiProvider::try_create_simple(conf, &tenant).await?;
    let username = "test-user1";
    let hostname = "%";
    let pwd = "test-pwd";

    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(pwd),
        hash_method: PasswordHashMethod::Sha256,
        need_change: false,
    };

    // add user hostname.
    {
        let user_info = UserInfo::new(username, hostname, auth_info.clone());
        user_mgr
            .create_user(&tenant, user_info, &CreateOption::Create)
            .await?;
    }

    // add user hostname again, error.
    {
        let user_info = UserInfo::new(username, hostname, auth_info.clone());
        let res = user_mgr
            .create_user(&tenant, user_info, &CreateOption::Create)
            .await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::USER_ALREADY_EXISTS);
    }

    // add user hostname again, ok.
    {
        let user_info = UserInfo::new(username, hostname, auth_info.clone());
        user_mgr
            .create_user(&tenant, user_info, &CreateOption::CreateIfNotExists)
            .await?;
    }

    // get all users.
    {
        let users = user_mgr.get_users(&tenant).await?;
        assert_eq!(1, users.len());
        assert_eq!(pwd.as_bytes(), users[0].auth_info.get_password().unwrap());
    }

    // get user hostname.
    {
        let user_info = user_mgr
            .get_user(&tenant, UserIdentity::new(username, hostname))
            .await?;
        assert_eq!(hostname, user_info.hostname);
        assert_eq!(pwd.as_bytes(), user_info.auth_info.get_password().unwrap());
    }

    // drop.
    {
        user_mgr
            .drop_user(&tenant, UserIdentity::new(username, hostname), false)
            .await?;
        let users = user_mgr.get_users(&tenant).await?;
        assert_eq!(0, users.len());
    }

    // repeat drop same user not with if exist.
    {
        let res = user_mgr
            .drop_user(&tenant, UserIdentity::new(username, hostname), false)
            .await;
        assert!(res.is_err());
    }

    // repeat drop same user with if exist.
    {
        let res = user_mgr
            .drop_user(&tenant, UserIdentity::new(username, hostname), true)
            .await;
        assert!(res.is_ok());
    }

    // grant privileges
    {
        let user_info: UserInfo = UserInfo::new(username, hostname, auth_info.clone());
        user_mgr
            .create_user(&tenant, user_info.clone(), &CreateOption::Create)
            .await?;
        let old_user = user_mgr.get_user(&tenant, user_info.identity()).await?;
        assert_eq!(old_user.grants, UserGrantSet::empty());

        let mut add_priv = UserPrivilegeSet::empty();
        add_priv.set_privilege(UserPrivilegeType::Set);
        user_mgr
            .grant_privileges_to_user(&tenant, user_info.identity(), GrantObject::Global, add_priv)
            .await?;
        let new_user = user_mgr.get_user(&tenant, user_info.identity()).await?;
        assert!(
            new_user
                .grants
                .verify_privilege(&GrantObject::Global, UserPrivilegeType::Set)
        );
        assert!(
            !new_user
                .grants
                .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create)
        );
        user_mgr
            .drop_user(&tenant, new_user.identity(), true)
            .await?;
    }

    // revoke privileges
    {
        let user_info: UserInfo = UserInfo::new(username, hostname, auth_info.clone());
        user_mgr
            .create_user(&tenant, user_info.clone(), &CreateOption::Create)
            .await?;
        user_mgr
            .grant_privileges_to_user(
                &tenant,
                user_info.identity(),
                GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            )
            .await?;
        let user_info = user_mgr.get_user(&tenant, user_info.identity()).await?;
        assert_eq!(user_info.grants.entries().len(), 1);

        user_mgr
            .revoke_privileges_from_user(
                &tenant,
                user_info.identity(),
                GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            )
            .await?;
        let user_info = user_mgr.get_user(&tenant, user_info.identity()).await?;
        assert_eq!(user_info.grants.entries().len(), 0);
        user_mgr
            .drop_user(&tenant, user_info.identity(), true)
            .await?;
    }

    // alter.
    {
        let user = "test";
        let hostname = "%";
        let pwd = "test";
        let auth_info = AuthInfo::Password {
            hash_value: Vec::from(pwd),
            hash_method: PasswordHashMethod::Sha256,
            need_change: false,
        };
        let user_info: UserInfo = UserInfo::new(user, hostname, auth_info.clone());
        user_mgr
            .create_user(&tenant, user_info.clone(), &CreateOption::Create)
            .await?;

        let old_user = user_mgr
            .get_meta_user(&tenant, user_info.identity())
            .await?;
        let seq = old_user.seq;
        let mut old_user = old_user.data;
        assert_eq!(old_user.auth_info.get_password().unwrap(), Vec::from(pwd));

        // alter both password & password_type
        let new_pwd = "test1";
        let auth_info = AuthInfo::Password {
            hash_value: Vec::from(new_pwd),
            hash_method: PasswordHashMethod::Sha256,
            need_change: false,
        };
        old_user.update_auth_option(Some(auth_info.clone()), None);
        old_user.update_user_time();
        old_user.update_auth_history(Some(auth_info));
        user_mgr.alter_user(&tenant, &old_user, seq).await?;
        let new_user = user_mgr
            .get_meta_user(&tenant, user_info.identity())
            .await?;
        let seq = new_user.seq;
        let mut new_user = new_user.data;
        assert_eq!(
            new_user.auth_info.get_password().unwrap(),
            Vec::from(new_pwd)
        );
        assert_eq!(
            new_user.auth_info.get_password_type().unwrap(),
            PasswordHashMethod::Sha256
        );

        // alter password only
        let new_new_pwd = "test2";
        let auth_info = AuthInfo::Password {
            hash_value: Vec::from(new_new_pwd),
            hash_method: PasswordHashMethod::Sha256,
            need_change: false,
        };

        new_user.update_auth_option(Some(auth_info.clone()), None);
        new_user.update_user_time();
        new_user.update_auth_history(Some(auth_info.clone()));
        user_mgr.alter_user(&tenant, &new_user, seq).await?;
        let new_new_user = user_mgr
            .get_meta_user(&tenant, user_info.identity())
            .await?;
        let new_new_user = new_new_user.data;
        assert_eq!(
            new_new_user.auth_info.get_password().unwrap(),
            Vec::from(new_new_pwd)
        );

        let not_exist = user_mgr
            .alter_user(&tenant, &UserInfo::new("user", hostname, auth_info), 1)
            .await;
        // ErrorCode::UnknownUser
        assert_eq!(not_exist.err().unwrap().code(), 2201)
    }

    Ok(())
}

/// With `user` and `setting` in `cache.meta_service_caches`, a login finds a user and the new
/// sessions find the global settings, as direct reads do, as they are created and dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_login_with_watch_cache() -> anyhow::Result<()> {
    check_login_and_session_settings(&[MetaServiceCache::User, MetaServiceCache::Setting]).await
}

/// With only the users watched, a login and the new sessions find the user and the global
/// settings: the settings come from meta-service.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_login_with_user_cache_only() -> anyhow::Result<()> {
    check_login_and_session_settings(&[MetaServiceCache::User]).await
}

/// With only the global settings watched, a login and the new sessions find the user and the
/// global settings: the user comes from meta-service.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_login_with_setting_cache_only() -> anyhow::Result<()> {
    check_login_and_session_settings(&[MetaServiceCache::Setting]).await
}

/// A login and a session-token request find a user, and the new sessions find the global
/// settings, as they are created and dropped, with `caches` as `cache.meta_service_caches`.
async fn check_login_and_session_settings(caches: &[MetaServiceCache]) -> anyhow::Result<()> {
    let thread_name = std::thread::current()
        .name()
        .unwrap_or("test_login_with_watch_cache")
        .to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);
    GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO)?;
    let tenant = Tenant::new_literal("test");
    let cache = CacheConfig {
        meta_service_caches: caches.to_vec(),
        ..Default::default()
    };
    let provider =
        UserApiProvider::try_create(RpcClientConf::empty(), &cache, BuiltIn::default(), &tenant)
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
    let mut found = false;
    for _ in 0..100 {
        let login = provider
            .get_login_user_with_client_ip(&tenant, user.identity(), None)
            .await;
        // What a request authenticated by a session token reads.
        let token = provider.get_auth_user(&tenant, user.identity()).await;
        let settings = provider.get_session_settings(&tenant).await?;
        if login.ok().as_ref() == Some(&user)
            && token.ok().as_ref() == Some(&user)
            && settings == vec![setting.clone()]
        {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        found,
        "the login, the session-token request and the session did not find the user and the setting"
    );

    provider.drop_user(&tenant, user.identity(), false).await?;
    let unknown = |r: &databend_common_exception::Result<UserInfo>| {
        r.as_ref()
            .is_err_and(|e| e.code() == ErrorCode::UNKNOWN_USER)
    };
    let mut dropped = false;
    for _ in 0..100 {
        let login = provider
            .get_login_user_with_client_ip(&tenant, user.identity(), None)
            .await;
        let token = provider.get_auth_user(&tenant, user.identity()).await;
        if unknown(&login) && unknown(&token) {
            dropped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        dropped,
        "the login or the session-token request still found the dropped user"
    );
    Ok(())
}
