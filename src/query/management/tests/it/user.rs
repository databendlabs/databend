// Copyright 2021 Datafuse Labs

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

use databend_common_management::*;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MetaError;

fn default_test_auth_info() -> AuthInfo {
    AuthInfo::Password {
        hash_value: Vec::from("test_password"),
        hash_method: PasswordHashMethod::DoubleSha1,
        need_change: false,
    }
}

mod add {
    use std::sync::Arc;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_add_user() -> Result<(), MetaError> {
        let test_user_name = "test_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Test normal case - should succeed
        let res = user_mgr.create_user(user_info.clone(), false).await?;
        assert!(res.is_ok());

        // Test already exists case - should fail
        let res = user_mgr.create_user(user_info.clone(), false).await?;
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_add_user_create_or_replace() -> Result<(), MetaError> {
        let test_user_name = "test_user_replace";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // First creation should succeed
        let res = user_mgr.create_user(user_info.clone(), false).await?;
        assert!(res.is_ok());

        // create_user with overriding=true should succeed even if user exists
        let res = user_mgr.create_user(user_info.clone(), true).await?;
        assert!(res.is_ok());

        Ok(())
    }
}

mod get {
    use std::sync::Arc;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_existing() -> Result<(), MetaError> {
        let test_user_name = "test_get_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Get user should succeed
        let res = user_mgr.get_user(&user_info.identity()).await?;
        assert!(res.is_some());

        let retrieved_user = res.unwrap();
        assert_eq!(retrieved_user.data.name, test_user_name);
        assert_eq!(retrieved_user.data.hostname, test_hostname);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_not_exist() -> Result<(), MetaError> {
        let test_user_name = "nonexistent_user";
        let test_hostname = "localhost";

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        let res = user_mgr
            .get_user(&UserIdentity::new(test_user_name, test_hostname))
            .await?;

        // Should be None for non-existent user
        assert!(res.is_none());

        Ok(())
    }
}

mod get_users {
    use std::sync::Arc;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_users_empty() -> Result<(), MetaError> {
        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        let users = user_mgr.get_users().await?;
        assert!(users.is_empty());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_users_multiple() -> Result<(), MetaError> {
        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add multiple users
        for i in 0..5 {
            let name = format!("test_user_{}", i);
            let hostname = format!("test_hostname_{}", i);
            let user_info = UserInfo::new(&name, &hostname, default_test_auth_info());
            user_mgr.create_user(user_info, false).await?.unwrap();
        }

        let users = user_mgr.get_users().await?;
        assert_eq!(users.len(), 5);

        // Verify all users are present
        let mut user_names: Vec<String> = users.iter().map(|u| u.data.name.clone()).collect();
        user_names.sort();

        let expected_names: Vec<String> = (0..5).map(|i| format!("test_user_{}", i)).collect();
        assert_eq!(user_names, expected_names);

        Ok(())
    }
}

mod drop {
    use std::sync::Arc;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_drop_user_normal() -> Result<(), MetaError> {
        let test_user = "test_drop_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Verify user exists
        let res = user_mgr.get_user(&user_info.identity()).await?;
        assert!(res.is_some());

        // Drop user
        let res = user_mgr.drop_user(&user_info.identity()).await?;
        assert!(res.is_ok());

        // Verify user no longer exists
        let res = user_mgr.get_user(&user_info.identity()).await?;
        assert!(res.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_drop_user_unknown() -> Result<(), MetaError> {
        let test_user = "unknown_user";
        let test_hostname = "localhost";

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        let res = user_mgr
            .drop_user(&UserIdentity::new(test_user, test_hostname))
            .await?;

        // Inner result should be Err(UnknownError)
        assert!(res.is_err());

        Ok(())
    }
}

mod update {
    use std::sync::Arc;

    use databend_common_meta_app::principal::AuthInfo;
    use databend_common_meta_app::principal::PasswordHashMethod;

    use super::*;

    fn new_test_auth_info() -> AuthInfo {
        AuthInfo::Password {
            hash_value: Vec::from("test_password_new"),
            hash_method: PasswordHashMethod::Sha256,
            need_change: true,
        }
    }

    fn new_test_auth_info_full() -> AuthInfo {
        AuthInfo::Password {
            hash_value: Vec::from("test_password_full"),
            hash_method: PasswordHashMethod::Sha256,
            need_change: false,
        }
    }

    fn new_test_auth_info_partial() -> AuthInfo {
        AuthInfo::Password {
            hash_value: Vec::from("test_password_partial"),
            hash_method: PasswordHashMethod::DoubleSha1,
            need_change: true,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_normal() -> Result<(), MetaError> {
        let test_user_name = "test_update_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Update user auth info
        let res = user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.update_auth_option(Some(new_test_auth_info()), None)
            })
            .await?;

        assert!(res.is_ok());

        // Verify the update by getting the user
        let updated_user = user_mgr.get_user(&user_info.identity()).await?.unwrap();
        match &updated_user.data.auth_info {
            AuthInfo::Password {
                hash_method,
                need_change,
                ..
            } => {
                assert_eq!(*hash_method, PasswordHashMethod::Sha256);
                assert!(*need_change);
            }
            _ => panic!("Expected password auth info"),
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_normal_update_full() -> Result<(), MetaError> {
        let test_user_name = "test_update_full";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Full update - change everything
        let res = user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.update_auth_option(Some(new_test_auth_info_full()), None)
            })
            .await?;

        assert!(res.is_ok());

        // Verify the full update
        let updated_user = user_mgr.get_user(&user_info.identity()).await?.unwrap();
        match &updated_user.data.auth_info {
            AuthInfo::Password {
                hash_method,
                need_change,
                ..
            } => {
                assert_eq!(*hash_method, PasswordHashMethod::Sha256);
                assert!(!(*need_change));
            }
            _ => panic!("Expected password auth info"),
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_normal_update_partial() -> Result<(), MetaError> {
        let test_user_name = "test_update_partial";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Partial update - only change some fields
        let res = user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.update_auth_option(Some(new_test_auth_info_partial()), None)
            })
            .await?;

        assert!(res.is_ok());

        // Verify the partial update
        let updated_user = user_mgr.get_user(&user_info.identity()).await?.unwrap();
        match &updated_user.data.auth_info {
            AuthInfo::Password {
                hash_method,
                need_change,
                ..
            } => {
                assert_eq!(*hash_method, PasswordHashMethod::DoubleSha1);
                assert!(*need_change);
            }
            _ => panic!("Expected password auth info"),
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_unknown() -> Result<(), MetaError> {
        let test_user_name = "unknown_update_user";
        let test_hostname = "localhost";

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        let res = user_mgr
            .update_user_with(
                &UserIdentity::new(test_user_name, test_hostname),
                |ui: &mut UserInfo| ui.update_auth_option(Some(new_test_auth_info()), None),
            )
            .await?;

        // Inner result should be Err(UnknownError)
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_with_complete() -> Result<(), MetaError> {
        let test_user_name = "test_complete_update";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Complete update with no-op function to test the flow
        let res = user_mgr
            .update_user_with(&user_info.identity(), |_ui: &mut UserInfo| {
                // No-op update to test complete flow
            })
            .await?;

        assert!(res.is_ok());

        Ok(())
    }
}

mod set_user_privileges {
    use std::sync::Arc;

    use databend_common_meta_app::principal::GrantObject;
    use databend_common_meta_app::principal::UserPrivilegeSet;
    use databend_common_meta_app::principal::UserPrivilegeType;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_grant_user_privileges() -> Result<(), MetaError> {
        let test_user_name = "test_privilege_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Grant privileges
        let mut privileges = UserPrivilegeSet::empty();
        privileges.set_privilege(UserPrivilegeType::Select);

        let res = user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.grants.grant_privileges(&GrantObject::Global, privileges);
            })
            .await?;

        assert!(res.is_ok());

        // Verify privileges were granted
        let updated_user = user_mgr.get_user(&user_info.identity()).await?.unwrap();
        let global_privileges = updated_user
            .data
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select);
        assert!(global_privileges);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_revoke_user_privileges() -> Result<(), MetaError> {
        let test_user_name = "test_revoke_user";
        let test_hostname = "localhost";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let meta_store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let user_mgr = UserMgr::create(Arc::new(meta_store), &Tenant::new_literal("tenant1"));

        // Add user first
        user_mgr
            .create_user(user_info.clone(), false)
            .await?
            .unwrap();

        // Grant privileges first
        let mut privileges = UserPrivilegeSet::empty();
        privileges.set_privilege(UserPrivilegeType::Select);
        privileges.set_privilege(UserPrivilegeType::Insert);

        user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.grants.grant_privileges(&GrantObject::Global, privileges);
            })
            .await?
            .unwrap();

        // Revoke one privilege
        let mut revoke_privileges = UserPrivilegeSet::empty();
        revoke_privileges.set_privilege(UserPrivilegeType::Select);

        let res = user_mgr
            .update_user_with(&user_info.identity(), |ui: &mut UserInfo| {
                ui.grants
                    .revoke_privileges(&GrantObject::Global, revoke_privileges);
            })
            .await?;

        assert!(res.is_ok());

        // Verify only insert privilege remains
        let updated_user = user_mgr.get_user(&user_info.identity()).await?.unwrap();
        let has_select = updated_user
            .data
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Select);
        let has_insert = updated_user
            .data
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Insert);

        assert!(!has_select);
        assert!(has_insert);

        Ok(())
    }
}
