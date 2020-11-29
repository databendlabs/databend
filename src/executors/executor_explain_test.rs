// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_executor() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::contexts::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::testdata::*;

    let testdata = CsvTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        0,
        testdata.csv_table_datasource_for_test(),
    ));
    if let PlanNode::Explain(plan) = Planner::new().build_from_sql(
        ctx.clone(),
        "explain select c2+1 as c21, c3 from t1 where (c21+2)<2",
    )? {
        let executor = ExplainExecutor::try_create(ctx, plan.as_ref().clone())?;
        assert_eq!(executor.name(), "ExplainExecutor");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
