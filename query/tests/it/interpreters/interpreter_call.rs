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
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_call_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();

    let ctx = crate::tests::create_query_context()?;

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

    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$fuse_history()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$fuse_history` expect to have 2 arguments, but got 0.";
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
async fn test_call_warehouse_metadata_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$create_warehouse_meta()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$create_warehouse_meta` expect to have 3 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Create Warehouse
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_warehouse_metadata_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch. Case 1
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$get_warehouse_meta()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$get_warehouse_meta` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // NumberArgumentsNotMatch. Case 2
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$get_warehouse_meta(tenant1)").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$get_warehouse_meta` expect to have 2 arguments, but got 1.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Get on empty
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$get_warehouse_meta('tenant1', '🐸🐸@@11')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect =
            "Code: 2901, displayText = unknown warehouse __fd_warehouses/tenant1/🐸🐸@@11.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Ok
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$get_warehouse_meta('tenant1', '🐸🐸@@11')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_list_warehouse_metadata_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch.
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$list_warehouse_meta()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$list_warehouse_meta` expect to have 1 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Ok
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$list_warehouse_meta('tenant1')").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);

        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);

        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@1212', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);

        let plan =
            PlanParser::parse(ctx.clone(), "call system$list_warehouse_meta('tenant1')").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_update_warehouse_metadata_size_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch.
    {
        let plan =
            PlanParser::parse(ctx.clone(), "call system$update_warehouse_meta_size()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$update_warehouse_meta_size` expect to have 3 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // cannot update on non-exist warehouse
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$update_warehouse_meta_size('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 2901, displayText = unknown warehouse __fd_warehouses/tenant1/🐸🐸@@11(while update warehouse size).";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Ok
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);

        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$update_warehouse_meta_size('tenant1', '🐸🐸@@11', 'XXXLarge')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_warehouse_metadata_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context()?;

    // NumberArgumentsNotMatch.
    {
        let plan = PlanParser::parse(ctx.clone(), "call system$drop_warehouse_meta()").await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), true);
        let expect = "Code: 1028, displayText = Function `system$drop_warehouse_meta` expect to have 2 arguments, but got 0.";
        assert_eq!(expect, res.err().unwrap().to_string());
    }

    // Drop on nil should work
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$drop_warehouse_meta('tenant1', 'non-exists')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }

    // Regular case
    {
        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$create_warehouse_meta('tenant1', '🐸🐸@@11', 'Small')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);

        let plan = PlanParser::parse(
            ctx.clone(),
            "call system$drop_warehouse_meta('tenant1',  '🐸🐸@@11')",
        )
        .await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "CallInterpreter");
        let res = executor.execute(None).await;
        assert_eq!(res.is_err(), false);
    }
    Ok(())
}
