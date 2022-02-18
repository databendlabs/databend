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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserGrantSet;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use databend_query::users::User;
use databend_query::users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_manager() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();

    let tenant = "test";
    let user = "test-user1";
    let hostname = "localhost";
    let hostname2 = "%";
    let pwd = "test-pwd";
    let user_mgr = UserApiProvider::create_global(conf).await?;

    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(pwd),
        hash_method: PasswordHashMethod::PlainText,
    };

    // add user hostname.
    {
        let user_info = User::new(user, hostname, auth_info.clone());
        user_mgr.add_user(tenant, user_info.into()).await?;
    }

    // add user hostname2.
    {
        let user_info = User::new(user, hostname2, auth_info.clone());
        user_mgr.add_user(tenant, user_info.into()).await?;
    }

    // get all users.
    {
        let users = user_mgr.get_users(tenant).await?;
        assert_eq!(2, users.len());
        assert_eq!(pwd.as_bytes(), users[0].auth_info.get_password().unwrap());
    }

    // get user hostname.
    {
        let user = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(hostname, user.hostname);
        assert_eq!(pwd.as_bytes(), user.auth_info.get_password().unwrap());
    }

    // get user hostname2.
    {
        let user = user_mgr.get_user(tenant, user, hostname2).await?;
        assert_eq!(hostname2, user.hostname);
        assert_eq!(pwd.as_bytes(), user.auth_info.get_password().unwrap());
    }

    // drop.
    {
        user_mgr.drop_user(tenant, user, hostname, false).await?;
        let users = user_mgr.get_users(tenant).await?;
        assert_eq!(1, users.len());
    }

    // repeat drop same user not with if exist.
    {
        let res = user_mgr.drop_user(tenant, user, hostname, false).await;
        assert!(res.is_err());
    }

    // repeat drop same user with if exist.
    {
        let res = user_mgr.drop_user(tenant, user, hostname, true).await;
        assert!(res.is_ok());
    }

    // grant privileges
    {
        let user_info = User::new(user, hostname, auth_info.clone());
        user_mgr.add_user(tenant, user_info.into()).await?;
        let old_user = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(old_user.grants, UserGrantSet::empty());

        let mut add_priv = UserPrivilegeSet::empty();
        add_priv.set_privilege(UserPrivilegeType::Set);
        user_mgr
            .grant_user_privileges(tenant, user, hostname, GrantObject::Global, add_priv)
            .await?;
        let new_user = user_mgr.get_user(tenant, user, hostname).await?;
        assert!(new_user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Set));
        assert!(!new_user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
        user_mgr.drop_user(tenant, user, hostname, true).await?;
    }

    // revoke privileges
    {
        let user_info = User::new(user, hostname, auth_info.clone());
        user_mgr.add_user(tenant, user_info.into()).await?;
        user_mgr
            .grant_user_privileges(
                tenant,
                user,
                hostname,
                GrantObject::Global,
                UserPrivilegeSet::all_privileges(),
            )
            .await?;
        let user_info = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(user_info.grants.entries().len(), 1);

        user_mgr
            .revoke_user_privileges(
                tenant,
                user,
                hostname,
                GrantObject::Global,
                UserPrivilegeSet::all_privileges(),
            )
            .await?;
        let user_info = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(user_info.grants.entries().len(), 0);
        user_mgr.drop_user(tenant, user, hostname, true).await?;
    }

    // alter.
    {
        let user = "test";
        let hostname = "localhost";
        let pwd = "test";
        let auth_info = AuthInfo::Password {
            hash_value: Vec::from(pwd),
            hash_method: PasswordHashMethod::PlainText,
        };
        let user_info = User::new(user, hostname, auth_info.clone());
        user_mgr.add_user(tenant, user_info.into()).await?;

        let old_user = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(old_user.auth_info.get_password().unwrap(), Vec::from(pwd));

        // alter both password & password_type
        let new_pwd = "test1";
        let auth_info = AuthInfo::Password {
            hash_value: Vec::from(new_pwd),
            hash_method: PasswordHashMethod::Sha256,
        };
        user_mgr
            .update_user(tenant, user, hostname, auth_info)
            .await?;
        let new_user = user_mgr.get_user(tenant, user, hostname).await?;
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
        };
        user_mgr
            .update_user(tenant, user, hostname, auth_info.clone())
            .await?;
        let new_new_user = user_mgr.get_user(tenant, user, hostname).await?;
        assert_eq!(
            new_new_user.auth_info.get_password().unwrap(),
            Vec::from(new_new_pwd)
        );

        let not_exist = user_mgr
            .update_user(tenant, "user", hostname, auth_info.clone())
            .await;
        // ErrorCode::UnknownUser
        assert_eq!(not_exist.err().unwrap().code(), 2201)
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_manager_with_root_user() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();

    let tenant = "test";
    let username1 = "default";
    let username2 = "root";

    let hostname1 = "127.0.0.1";
    let hostname2 = "localhost";
    let hostname3 = "otherhost";

    let user_mgr = UserApiProvider::create_global(conf).await?;

    // Get user via username `default` and hostname `127.0.0.1`.
    {
        let user = user_mgr.get_user(tenant, username1, hostname1).await?;
        assert_eq!(user.name, username1);
        assert_eq!(user.hostname, hostname1);
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Insert));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Super));
    }

    // Get user via username `default` and hostname `localhost`.
    {
        let user = user_mgr.get_user(tenant, username1, hostname2).await?;
        assert_eq!(user.name, username1);
        assert_eq!(user.hostname, hostname2);
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Insert));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Super));
    }

    // Get user via username `default` and hostname `otherhost`.
    {
        let user = user_mgr.get_user(tenant, username1, hostname3).await?;
        assert_eq!(user.name, username1);
        assert_eq!(user.hostname, hostname3);
        assert!(user.grants.entries().is_empty());
    }

    // Get user via username `root` and hostname `127.0.0.1`.
    {
        let user = user_mgr.get_user(tenant, username2, hostname1).await?;
        assert_eq!(user.name, username2);
        assert_eq!(user.hostname, hostname1);
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Insert));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Super));
    }

    // Get user via username `root` and hostname `localhost`.
    {
        let user = user_mgr.get_user(tenant, username2, hostname2).await?;
        assert_eq!(user.name, username2);
        assert_eq!(user.hostname, hostname2);
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Create));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Insert));
        assert!(user
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Super));
    }

    // Get user via username `root` and hostname `otherhost`.
    {
        let user = user_mgr.get_user(tenant, username2, hostname3).await?;
        assert_eq!(user.name, username2);
        assert_eq!(user.hostname, hostname3);
        assert!(user.grants.entries().is_empty());
    }

    Ok(())
}
