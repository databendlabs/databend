// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_select_executor() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use std::sync::Arc;

    use crate::contexts::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::testdata;

    let test_source = testdata::NumberTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        test_source.number_source_for_test()?,
    ));

    if let PlanNode::Select(plan) = Planner::new().build_from_sql(
        ctx.clone(),
        "select number from system.numbers_mt(10) where (number+2)<2",
    )? {
        let executor = SelectExecutor::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "SelectExecutor");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
