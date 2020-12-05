// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::NumberTestData::create();
    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.merge_processor()?;

    if let PlanNode::Limit(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .limit(2)?
        .build()?
    {
        pipeline.add_simple_transform(|| Ok(Box::new(LimitTransform::try_create(plan.n)?)))?;
    }

    let mut stream = pipeline.execute().await?;
    let mut rows = 0;
    while let Some(v) = stream.next().await {
        let row = v?.num_rows();
        rows += row;
    }
    assert_eq!(2, rows);
    Ok(())
}
