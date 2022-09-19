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
use common_meta_types::UserInfo;
use common_users::UserApiProvider;
use databend_query::interpreters::InterpreterFactoryV2;
use databend_query::sessions::TableContext;
use databend_query::sql::Planner;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grant_role_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());
    let tenant = ctx.get_tenant();
    let user_mgr = UserApiProvider::instance();

    // Grant a unknown role
    {
        let query = "GRANT ROLE 'test' TO 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownRole("").code())
    }

    user_mgr
        .add_role(&tenant, RoleInfo::new("test"), false)
        .await?;

    // Grant role to unknown user.
    {
        let query = "GRANT ROLE 'test' TO 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownUser("").code())
    }

    // Grant role to normal user.
    {
        let user_info = UserInfo::new_no_auth("test_user", "%");
        user_mgr.add_user(&tenant, user_info.clone(), false).await?;
        let user_info = user_mgr.get_user(&tenant, user_info.identity()).await?;
        assert_eq!(user_info.grants.roles().len(), 0);

        let query = "GRANT ROLE 'test' TO 'test_user'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;

        let user_info = user_mgr.get_user(&tenant, user_info.identity()).await?;
        let roles = user_info.grants.roles();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0], "test".to_string());
    }

    // Grant role to unknown role.
    {
        let query = "GRANT ROLE 'test' TO ROLE 'test_role'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownRole("").code())
    }

    let test_role = RoleInfo::new("test_role");

    // Grant role to normal role.
    {
        user_mgr.add_role(&tenant, test_role.clone(), false).await?;
        let role_info = user_mgr
            .get_role(&tenant, test_role.identity().into())
            .await?;
        assert_eq!(role_info.grants.roles().len(), 0);

        let query = "GRANT ROLE 'test' TO ROLE 'test_role'";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;

        let role_info = user_mgr
            .get_role(&tenant, test_role.identity().into())
            .await?;
        let roles = role_info.grants.roles();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0], "test".to_string());
    }
    Ok(())
}
