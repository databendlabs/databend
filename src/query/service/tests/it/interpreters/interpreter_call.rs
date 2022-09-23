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
use common_meta_types::AuthInfo;
use common_meta_types::GrantObject;
use common_meta_types::PasswordHashMethod;
use common_meta_types::UserInfo;
use common_meta_types::UserOptionFlag;
use common_meta_types::UserPrivilegeSet;
use databend_query::interpreters::*;
use databend_query::sql::Planner;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    let query = "call system$test()";
    let (plan, _, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    assert_eq!(executor.name(), "CallInterpreter");
    let res = executor.execute(ctx.clone()).await;
    assert_eq!(res.is_err(), true);
    assert_eq!(
        res.err().unwrap().code(),
        ErrorCode::UnknownFunction("").code()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_call_fuse_snapshot_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // NumberArgumentsNotMatch
    {
        let query = "call system$fuse_snapshot()";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `FUSE_SNAPSHOT` expect to have [2, 3] arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let query = "call system$fuse_snapshot(default, test)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::UnknownTable("").code()
        );
    }

    // BadArguments
    {
        let query = "call system$fuse_snapshot(system, tables)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
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

        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // FuseHistory
    {
        let query = "call system$fuse_snapshot(default, a)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_call_fuse_block_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // NumberArgumentsNotMatch
    {
        let query = "call system$fuse_block()";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `FUSE_BLOCK` expect to have [2, 3] arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let query = "call system$fuse_block(default, test)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::UnknownTable("").code()
        );
    }

    // BadArguments
    {
        let query = "call system$fuse_block(system, tables)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
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

        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // fuse_block
    {
        let query = "call system$fuse_block(default, a)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_clustering_information_interpreter() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // NumberArgumentsNotMatch
    {
        let query = "call system$clustering_information()";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `CLUSTERING_INFORMATION` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // UnknownTable
    {
        let query = "call system$clustering_information(default, test)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().code(),
            ErrorCode::UnknownTable("").code()
        );
    }

    // BadArguments
    {
        let query = "call system$clustering_information(system, tables)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(ctx.clone()).await;
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

        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // Unclustered.
    {
        let query = "call system$clustering_information(default, a)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1081, displayText = Invalid clustering keys or table a is not clustered.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Create table b
    {
        let query = "\
        CREATE TABLE default.b(a bigint) cluster by(a)\
    ";

        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    // clustering_information
    {
        let query = "call system$clustering_information(default, b)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let _ = executor.execute(ctx.clone()).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_tenant_quota_interpreter() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create()
        .with_management_mode()
        .config();

    let mut user_info = UserInfo::new("root", "127.0.0.1", AuthInfo::Password {
        hash_method: PasswordHashMethod::Sha256,
        hash_value: Vec::from("pass"),
    });

    user_info.grants.grant_privileges(
        &GrantObject::Global,
        UserPrivilegeSet::available_privileges_on_global(),
    );

    user_info
        .option
        .set_option_flag(UserOptionFlag::TenantSetting);

    let (_guard, ctx) =
        crate::tests::create_query_context_with_config(conf.clone(), Some(user_info)).await?;

    let mut planner = Planner::new(ctx.clone());

    // current tenant
    {
        let query = "call admin$tenant_quota()";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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

    // query other tenant
    {
        let query = "call admin$tenant_quota(tenant1)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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

    // set other tenant quota
    {
        let query = "call admin$tenant_quota(tenant1, 7, 5, 3, 3)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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
        let query = "call admin$tenant_quota(tenant1, 8)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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
        let query = "call admin$tenant_quota(tenant1)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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

    // current tenant again
    {
        let query = "call admin$tenant_quota()";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = executor.execute(ctx.clone()).await?;
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

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_tenant_quote_without_management_mode() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    // Access denied
    {
        let query = "call admin$tenant_quota(tenant1)";
        let (plan, _, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let res = executor.execute(ctx.clone()).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 1062, displayText = Access denied: 'TENANT_QUOTA' only used in management-mode.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    Ok(())
}
