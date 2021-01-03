// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_functions_table() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;

    use crate::contexts::*;
    use crate::datasources::system::*;
    use crate::datasources::*;
    use crate::planners::*;
    use crate::tests;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;

    let table = FunctionsTable::create();
    table.read_plan(ctx.clone(), PlanBuilder::empty().build()?)?;
    let stream = table.read(ctx, vec![]).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(17, rows);
    Ok(())
}
