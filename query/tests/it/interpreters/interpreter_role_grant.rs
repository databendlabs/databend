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
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::PlanParser;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_grant_role_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();

    // Grant a unknown role
    {
        let query = "GRANT ROLE 'test' TO 'test_user'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownRole("").code())
    }

    user_mgr
        .add_role(&tenant, RoleInfo::new("test"), false)
        .await?;

    // Grant role to unknown user.
    {
        let query = "GRANT ROLE 'test' TO 'test_user'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(None).await;
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
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;

        let user_info = user_mgr.get_user(&tenant, user_info.identity()).await?;
        let roles = user_info.grants.roles();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0], "test".to_string());
    }

    // Grant role to unknown role.
    {
        let query = "GRANT ROLE 'test' TO ROLE 'test_role'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "GrantRoleInterpreter");
        let res = executor.execute(None).await;
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().code(), ErrorCode::UnknownRole("").code())
    }

    let test_role = RoleInfo::new("test_role");

    // Grant role to normal role.
    {
        user_mgr.add_role(&tenant, test_role.clone(), false).await?;
        let role_info = user_mgr.get_role(&tenant, test_role.identity()).await?;
        assert_eq!(role_info.grants.roles().len(), 0);

        let query = "GRANT ROLE 'test' TO ROLE 'test_role'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;

        let role_info = user_mgr.get_role(&tenant, test_role.identity()).await?;
        let roles = role_info.grants.roles();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0], "test".to_string());
    }
    Ok(())
}
