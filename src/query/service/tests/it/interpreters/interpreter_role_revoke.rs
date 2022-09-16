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

use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::RoleInfo;
use common_meta_types::UserIdentity;
use common_meta_types::UserInfo;
use databend_query::interpreters::InterpreterFactoryV2;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_revoke_role_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();

    // Revoke role from unknown user.
    {
        let query = "REVOKE ROLE 'test' FROM 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "RevokeRoleInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownUser("").code())
    }

    // Revoke role from normal user.
    {
        let mut test_user = UserInfo::new_no_auth("test_user", "%");
        test_user.grants.grant_role("test".to_string());
        user_mgr.add_user(&tenant, test_user.clone(), false).await?;
        let user_info = user_mgr.get_user(&tenant, test_user.identity()).await?;
        assert_eq!(user_info.grants.roles().len(), 1);

        let query = "REVOKE ROLE 'test' FROM 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute(ctx.clone()).await?;

        let user_info = user_mgr.get_user(&tenant, test_user.identity()).await?;
        let roles = user_info.grants.roles();
        assert_eq!(roles.len(), 0);
    }

    // Revoke role again
    {
        let query = "REVOKE ROLE 'test' FROM 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute(ctx.clone()).await?;

        let user_info = user_mgr
            .get_user(&tenant, UserIdentity::new("test_user", "%"))
            .await?;
        let roles = user_info.grants.roles();
        assert_eq!(roles.len(), 0);
    }

    // Revoke role from unknown role.
    {
        let query = "REVOKE ROLE 'test' FROM ROLE 'test_role'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        assert_eq!(executor.name(), "RevokeRoleInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownRole("").code())
    }

    let mut test_role = RoleInfo::new("test_role");
    test_role.grants.grant_role("test".to_string());
    user_mgr.add_role(&tenant, test_role.clone(), false).await?;
    let role_info = user_mgr
        .get_role(&tenant, test_role.identity().to_string())
        .await?;
    assert_eq!(role_info.grants.roles().len(), 1);

    // Revoke role from normal role.
    {
        let query = "REVOKE ROLE 'test' FROM ROLE 'test_role'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute(ctx.clone()).await?;

        let role_info = user_mgr
            .get_role(&tenant, test_role.identity().to_string())
            .await?;
        let roles = role_info.grants.roles();
        assert_eq!(roles.len(), 0);
    }

    // Revoke role again
    {
        let query = "REVOKE ROLE 'test' FROM ROLE 'test_role'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan)?;
        let _ = executor.execute(ctx.clone()).await?;

        let role_info = user_mgr
            .get_role(&tenant, test_role.identity().to_string())
            .await?;
        let roles = role_info.grants.roles();
        assert_eq!(roles.len(), 0);
    }

    Ok(())
}
