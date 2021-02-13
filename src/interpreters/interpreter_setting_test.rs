// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_interpreter() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;

    use crate::interpreters::*;
    use crate::planners::*;
    use crate::sql::*;

    let ctx = crate::sessions::FuseQueryContext::try_create_ctx()?;

    if let PlanNode::SetVariable(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("set max_block_size=1")?
    {
        let executor = SettingInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "SettingInterpreter");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_setting_interpreter_error() -> crate::error::FuseQueryResult<()> {
    use crate::interpreters::*;
    use crate::planners::*;
    use crate::sql::*;

    let ctx = crate::sessions::FuseQueryContext::try_create_ctx()?;

    if let PlanNode::SetVariable(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("set xx=1")?
    {
        let executor = SettingInterpreter::try_create(ctx, plan)?;
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
