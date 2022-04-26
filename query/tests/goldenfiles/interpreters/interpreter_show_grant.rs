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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::RoleInfo;
use common_meta_types::UserIdentity;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;
use goldenfile::Mint;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_show_grant_interpreter() -> Result<()> {
    let mut mint = Mint::new("tests/goldenfiles/data");
    let mut file = mint.new_goldenfile("show-grant.txt").unwrap();

    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();
    let role_cache_mgr = ctx.get_role_cache_manager();
    user_mgr
        .add_user(&tenant, UserInfo::new_no_auth("test", "localhost"), false)
        .await?;

    user_mgr
        .add_role(&tenant, RoleInfo::new("role1"), false)
        .await?;

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR 'test'@'localhost'"#,
        )
        .await?;
    }

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR ROLE 'role1'"#,
        )
        .await?;
    }

    let mut role_info = RoleInfo::new("role2");
    let mut privileges = UserPrivilegeSet::empty();
    privileges.set_privilege(UserPrivilegeType::Select);
    role_info
        .grants
        .grant_privileges(&GrantObject::Database("mydb".into()), privileges);
    user_mgr.add_role(&tenant, role_info, false).await?;
    role_cache_mgr.invalidate_cache(&tenant);

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR ROLE 'role2'"#,
        )
        .await?;
    }

    user_mgr
        .grant_role_to_user(
            &tenant,
            UserIdentity::new("test", "localhost"),
            "role2".to_string(),
        )
        .await?;
    // role_cache_mgr.invalidate_cache(&tenant);

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR 'test'@'localhost'"#,
        )
        .await?;
    }

    let mut privileges = UserPrivilegeSet::empty();
    privileges.set_privilege(UserPrivilegeType::Create);
    user_mgr
        .grant_privileges_to_user(
            &tenant,
            UserIdentity::new("test", "localhost"),
            GrantObject::Database("mydb".into()),
            privileges,
        )
        .await?;

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR 'test'@'localhost'"#,
        )
        .await?;
    }

    user_mgr
        .grant_role_to_role(&tenant, "role1".to_string(), "role2".to_string())
        .await?;

    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR ROLE 'role1'"#,
        )
        .await?;
    }

    user_mgr
        .grant_privileges_to_role(
            &tenant,
            "role1".to_string(),
            GrantObject::Database("mydb1".into()),
            privileges,
        )
        .await?;
    {
        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "ShowGrantsInterpreter",
            r#"SHOW GRANTS FOR ROLE 'role1'"#,
        )
        .await?;
    }

    Ok(())
}
