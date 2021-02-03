// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_executor() -> crate::error::FuseQueryResult<()> {
    use crate::tests;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::contexts::*;
    use crate::executors::*;
    use crate::planners::*;
    use crate::sql::*;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    if let PlanNode::Explain(plan) = PlanParser::new().build_from_sql(
        ctx.clone(),
        "explain select number from system.numbers_mt(10) where (number+1)=4",
    )? {
        let executor = ExplainExecutor::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "ExplainExecutor");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
