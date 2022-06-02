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

use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use databend_query::users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_role_manager() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();

    let tenant = "tenant1";
    let role_name = "test-role1".to_string();
    let role_mgr = UserApiProvider::create_global(conf).await?;

    // add role
    {
        let role_info = RoleInfo::new(&role_name);
        role_mgr.add_role(tenant, role_info, false).await?;
    }

    // add role again, error
    {
        let role_info = RoleInfo::new(&role_name);
        let res = role_mgr.add_role(tenant, role_info, false).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::user_already_exists_code(),
        );
    }

    // add role
    {
        let role_info = RoleInfo::new(&role_name);
        role_mgr.add_role(tenant, role_info, true).await?;
    }

    // get role
    {
        let role = role_mgr.get_role(tenant, role_name.clone()).await?;
        assert_eq!(role.name, "test-role1");
    }

    // get all roles
    {
        let roles = role_mgr.get_roles(tenant).await?;
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].name, "test-role1");
    }

    // grant and verify privilege to role
    {
        role_mgr
            .grant_privileges_to_role(
                tenant,
                role_name.clone(),
                GrantObject::Global,
                UserPrivilegeSet::all_privileges(),
            )
            .await?;
        let role = role_mgr.get_role(tenant, role_name.clone()).await?;
        assert!(role
            .grants
            .verify_privilege(&GrantObject::Global, UserPrivilegeType::Alter));
    }

    // revoke privilege from role
    {
        role_mgr
            .revoke_privileges_from_role(
                tenant,
                role_name.clone(),
                GrantObject::Global,
                UserPrivilegeSet::all_privileges(),
            )
            .await?;

        let role = role_mgr.get_role(tenant, role_name.clone()).await?;
        assert_eq!(role.grants.entries().len(), 0);
    }

    Ok(())
}
