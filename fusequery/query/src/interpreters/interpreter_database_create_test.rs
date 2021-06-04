// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_database_interpreter() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::interpreters::*;
    use crate::sql::*;

    common_tracing::init_simple_tracing();

    let ctx = crate::tests::try_create_context()?;

    if let PlanNode::CreateDatabase(plan) =
        PlanParser::create(ctx.clone()).build_from_sql("create database db1 Engine = Local")?
    {
        let executor = CreateDatabaseInterpreter::try_create(ctx, plan.clone())?;
        assert_eq!(executor.name(), "CreateDatabaseInterpreter");
        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }

    Ok(())
}
