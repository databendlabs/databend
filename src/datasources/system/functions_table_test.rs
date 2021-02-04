// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_functions_table() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;
    use crate::planners::*;

    let test_source = crate::tests::NumberTestData::create();
    let ctx =
        crate::contexts::FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    let table = FunctionsTable::create();
    table.read_plan(ctx.clone(), PlanBuilder::empty(ctx.clone()).build()?)?;
    let stream = table.read(ctx).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert!(rows > 18);
    Ok(())
}
