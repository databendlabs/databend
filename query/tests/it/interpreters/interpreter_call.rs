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
use common_meta_types::GrantObject;
use common_meta_types::UserGrantSet;
use common_meta_types::UserOptionFlag;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::TryStreamExt;
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
async fn test_call_fuse_snapshot_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$fuse_snapshot()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `FUSE_SNAPSHOT` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$fuse_snapshot(default, test)").await?;
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
            PlanParser::parse(ctx.clone(), "call system$fuse_snapshot(system, tables)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1015, displayText = expects table of engine FUSE, but got SystemTables.";
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
        let plan = PlanParser::parse(ctx.clone(), "call system$fuse_snapshot(default, a)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_clustering_information_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$clustering_information()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `CLUSTERING_INFORMATION` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$clustering_information(default, test)",
        )
        .await?;
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
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$clustering_information(system, tables)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1015, displayText = expects table of engine FUSE, but got SystemTables.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Create table a
    {
        let query = "\
            CREATE TABLE default.a(a bigint)\
        ";

        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // Unclustered.
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$clustering_information(default, a)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1070, displayText = Invalid clustering keys or table a is not clustered.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Create table b
    {
        let query = "\
        CREATE TABLE default.b(a bigint) cluster by(a)\
    ";

        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // FuseHistory
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$clustering_information(default, b)",
        )
        .await?;
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
        let expect = "Code: 1028, displayText = Function `BOOTSTRAP_TENANT` expect to have 1 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Access denied
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$bootstrap_tenant(tenant1)").await?;
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
        let plan = PlanParser::parse(ctx.clone(), "call admin$bootstrap_tenant(tenant1)").await?;
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
        let plan = PlanParser::parse(ctx.clone(), "call admin$bootstrap_tenant(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        executor.execute(None).await?;

        let user_mgr = ctx.get_user_manager();
        // should create account admin role
        let role = "account_admin".to_string();
        let role_info = user_mgr.get_role("tenant1", role.clone()).await?;
        let mut grants = UserGrantSet::empty();
        grants.grant_privileges(
            &GrantObject::Global,
            GrantObject::Global.available_privileges(),
        );
        assert_eq!(role_info.grants, grants);
    }

    // Idempotence on call bootstrap tenant
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$bootstrap_tenant(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        executor.execute(None).await?;
        let user_mgr = ctx.get_user_manager();
        // should create account admin role
        let role = "account_admin".to_string();
        let role_info = user_mgr.get_role("tenant1", role.clone()).await?;
        let mut grants = UserGrantSet::empty();
        grants.grant_privileges(
            &GrantObject::Global,
            GrantObject::Global.available_privileges(),
        );
        assert_eq!(role_info.grants, grants);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_tenant_quota_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$tenant_quota()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `TENANT_QUOTA` expect to have [1, 5] arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Access denied
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$tenant_quota(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1062, displayText = Access denied: 'TENANT_QUOTA' only used in management-mode.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    let mut user_info = ctx.get_current_user()?;
    user_info
        .option
        .set_option_flag(UserOptionFlag::TenantSetting);
    let ctx = crate::tests::create_query_context_with_config(conf.clone(), Some(user_info)).await?;
    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$tenant_quota(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+-------------------------+------------+---------------------+",
            "| max_databases | max_tables_per_database | max_stages | max_files_per_stage |",
            "+---------------+-------------------------+------------+---------------------+",
            "| 0             | 0                       | 0          | 0                   |",
            "+---------------+-------------------------+------------+---------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    {
        let plan =
            PlanParser::parse(ctx.clone(), "call admin$tenant_quota(tenant1, 7, 5, 3, 3)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+-------------------------+------------+---------------------+",
            "| max_databases | max_tables_per_database | max_stages | max_files_per_stage |",
            "+---------------+-------------------------+------------+---------------------+",
            "| 7             | 5                       | 3          | 3                   |",
            "+---------------+-------------------------+------------+---------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$tenant_quota(tenant1, 8)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+-------------------------+------------+---------------------+",
            "| max_databases | max_tables_per_database | max_stages | max_files_per_stage |",
            "+---------------+-------------------------+------------+---------------------+",
            "| 8             | 5                       | 3          | 3                   |",
            "+---------------+-------------------------+------------+---------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    {
        let plan = PlanParser::parse(ctx.clone(), "call admin$tenant_quota(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+---------------+-------------------------+------------+---------------------+",
            "| max_databases | max_tables_per_database | max_stages | max_files_per_stage |",
            "+---------------+-------------------------+------------+---------------------+",
            "| 8             | 5                       | 3          | 3                   |",
            "+---------------+-------------------------+------------+---------------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_stats_tenant_tables_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context().await?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call stats$tenant_tables()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `TENANT_TABLES` expect to have [1, 18446744073709551614] arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Access denied
    {
        let plan = PlanParser::parse(ctx.clone(), "call stats$tenant_tables(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1062, displayText = Access denied: 'TENANT_TABLES' only used in management-mode.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();
    let mut user_info = ctx.get_current_user()?;
    user_info
        .option
        .set_option_flag(UserOptionFlag::TenantSetting);
    let ctx = crate::tests::create_query_context_with_config(conf.clone(), Some(user_info)).await?;

    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call stats$tenant_tables(tenant1, tenant2, tenant3)",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-----------+-------------+",
            "| tenant_id | table_count |",
            "+-----------+-------------+",
            "| tenant1   | 0           |",
            "| tenant2   | 0           |",
            "| tenant3   | 0           |",
            "+-----------+-------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
