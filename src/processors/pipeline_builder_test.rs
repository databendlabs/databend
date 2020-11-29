// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_builder() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata::CsvTestData;

    let testdata = CsvTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        0,
        testdata.csv_table_datasource_for_test(),
    ));
    let plan = Planner::new().build_from_sql(
        ctx.clone(),
        "select c2+1 as c21, c3 from t1 where (c21+2)<2",
    )?;
    let mut stream = PipelineBuilder::create(ctx, plan)
        .build()?
        .execute()
        .await?;
    while let Some(_block) = stream.next().await {}
    Ok(())
}
