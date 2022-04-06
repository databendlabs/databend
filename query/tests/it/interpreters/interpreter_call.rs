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
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserGrantSet;
use common_meta_types::UserIdentity;
use common_meta_types::UserOptionFlag;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    let plan = PlanParser::parse(ctx.clone(), "call system$test()").await?;
    let executor = InterpreterFactory::get(ctx, plan.clone())?;
    assert_eq!(executor.name(), "CallInterpreter");
    let res = executor.execute(None).await;
    assert_eq!(res.is_err(), true);
    assert_eq!(
        res.err().unwrap().code(),
        ErrorCode::UnknownFunction("").code()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_fuse_history_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$fuse_history()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `FUSE_HISTORY` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$fuse_history(default, test)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::UnknownTable("").code()
        );
    }

    // BadArguments
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$fuse_history(system, tables)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1006, displayText = expecting fuse table, but got table of engine type: SystemTables.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Create table
    {
        let query = "\
            CREATE TABLE default.a(a bigint)\
        ";

        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // FuseHistory
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$fuse_history(default, a)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_bootstrap_tenant_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$bootstrap_tenant()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `BOOTSTRAP_TENANT` expect to have 5 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Access denied
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call admin$bootstrap_tenant(tenant1, test_user, '%', sha256_password, test_passwd)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1062, displayText = Access denied: 'BOOTSTRAP_TENANT' only used in management-mode.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    let ctx = crate::tests::create_query_context_with_config(conf.clone(), None).await?;

    // Management Mode, without user option
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call admin$bootstrap_tenant(tenant1, test_user, '%', sha256_password, test_passwd)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1063, displayText = Access denied: 'BOOTSTRAP_TENANT' requires user TENANTSETTING option flag.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    let mut user_info = ctx.get_current_user()?;
    user_info
        .option
        .set_option_flag(UserOptionFlag::TenantSetting);
    let ctx = crate::tests::create_query_context_with_config(conf.clone(), Some(user_info)).await?;

    // Management Mode, with user option
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call admin$bootstrap_tenant(tenant1, test_user, '%', sha256_password, test_passwd)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        executor.execute(None).await?;

        let user_mgr = ctx.get_user_manager();
        let user_info = user_mgr
            .get_user("tenant1", UserIdentity::new("test_user", "%"))
            .await?;
        assert_eq!(user_info.grants.roles().len(), 1);
        let role = &user_info.grants.roles()[0];
        let role_info = user_mgr.get_role("tenant1", role.clone()).await?;
        let mut grants = UserGrantSet::empty();
        grants.grant_privileges(
            &GrantObject::Global,
            GrantObject::Global.available_privileges(),
        );
        assert_eq!(role_info.grants, grants);
    }

    // Call again
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call admin$bootstrap_tenant(tenant1, test_user, '%', sha256_password, test_passwd)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        executor.execute(None).await?;
    }

    Ok(())
}
