// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_functions_table() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;
    use crate::planners::*;

    let table = FunctionsTable::create();
    table.read_plan(PlanBuilder::empty(true).build()?)?;
    let stream = table.read(vec![]).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(16, rows);
    Ok(())
}
