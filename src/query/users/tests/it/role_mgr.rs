// Copyright 2022 Datafuse Labs.
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

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::CreateOption::Create;
use databend_common_meta_app::schema::CreateOption::CreateIfNotExists;
use databend_common_meta_app::schema::CreateOption::CreateOrReplace;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::BUILTIN_ROLE_PUBLIC;
use databend_common_users::UserApiProvider;
use databend_common_version::BUILD_INFO;
use databend_meta_client::RpcClientConf;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_role_manager() -> anyhow::Result<()> {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Init with default.
    {
        GlobalConfig::init(&InnerConfig::default(), &BUILD_INFO).unwrap();
    }
    let conf = RpcClientConf::empty();
    let tenant = Tenant::new_literal("tenant1");

    let role_mgr = UserApiProvider::try_create_simple(conf, &tenant).await?;

    let role_name = "test-role1".to_string();

    // add role
    {
        let role_info = RoleInfo::new(&role_name, None);
        role_mgr
            .add_role(&tenant, role_info, &CreateOrReplace)
            .await?;
    }

    // add role again, error
    {
        let role_info = RoleInfo::new(&role_name, None);
        let res = role_mgr.add_role(&tenant, role_info, &Create).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::ROLE_ALREADY_EXISTS,);
    }

    // add role
    {
        let role_info = RoleInfo::new(&role_name, None);
        role_mgr
            .add_role(&tenant, role_info, &CreateIfNotExists)
            .await?;
    }

    // get role
    {
        let role = role_mgr.get_role(&tenant, role_name.clone()).await?;
        assert_eq!(role.name, "test-role1");
    }

    // get all roles
    {
        let mut role_names = role_mgr
            .get_roles(&tenant)
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect::<Vec<_>>();
        role_names.sort();
        assert_eq!(role_names, vec![
            BUILTIN_ROLE_ACCOUNT_ADMIN,
            BUILTIN_ROLE_PUBLIC,
            "test-role1"
        ]);
    }

    // grant and verify privilege to role
    {
        role_mgr
            .grant_privileges_to_role(
                &tenant,
                &role_name,
                GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            )
            .await?;
        let role = role_mgr.get_role(&tenant, role_name.clone()).await?;
        assert!(
            role.grants
                .verify_privilege(&GrantObject::Global, UserPrivilegeType::Alter)
        );
    }

    // revoke privilege from role
    {
        role_mgr
            .revoke_privileges_from_role(
                &tenant,
                &role_name,
                GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            )
            .await?;

        let role = role_mgr.get_role(&tenant, role_name.clone()).await?;
        assert_eq!(role.grants.entries().len(), 0);
    }

    Ok(())
}
