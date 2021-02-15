// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_settings_table() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;
    use crate::planners::*;

    let ctx = crate::sessions::FuseQueryContext::try_create_ctx()?;

    let table = SettingsTable::create();
    table.read_plan(ctx.clone(), PlanBuilder::empty(ctx.clone()).build()?)?;
    let stream = table.read(ctx).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(3, rows);
    Ok(())
}
