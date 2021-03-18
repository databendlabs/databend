// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_explain_interpreter() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::interpreters::*;
    use crate::planners::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::Create(plan) = PlanParser::create(ctx.clone())
        .build_from_sql("create table default.a(a bigint, b int) Engine = Null")?
    {
        let executor = CreateInterpreter::try_create(ctx, plan)?;
        assert_eq!(executor.name(), "CreateInterpreter");

        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
