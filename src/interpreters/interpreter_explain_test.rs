// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_interpreter() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::interpreters::*;
    use crate::planners::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::Explain(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("explain select number from system.numbers_mt(10) where (number+1)=4")?
    {
        let executor = ExplainInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "ExplainInterpreter");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
