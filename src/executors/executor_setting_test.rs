// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_executor() -> crate::error::FuseQueryResult<()> {
    use crate::contexts::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::testdata;
    use futures::stream::StreamExt;

    let test_source = testdata::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    if let PlanNode::SetVariable(plan) =
        Planner::new().build_from_sql(ctx.clone(), "set max_block_size=1")?
    {
        let executor = SettingExecutor::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "SetVariableExecutor");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_executor_error() -> crate::error::FuseQueryResult<()> {
    use crate::contexts::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    if let PlanNode::SetVariable(plan) = Planner::new().build_from_sql(ctx.clone(), "set xx=1")? {
        let executor = SettingExecutor::try_create(ctx, plan)?;
        if let Err(e) = executor.execute().await {
            let expect = "Internal(\"Unknown variable: \\\"xx\\\"\")";
            let actual = format!("{:?}", e);
            assert_eq!(expect, actual);
        } else {
            assert!(false);
        }
    }

    Ok(())
}
