// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit() -> crate::error::FuseQueryResult<()> {
    use futures::TryStreamExt;
    use std::sync::Arc;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::tests;
    use crate::transforms::*;

    let test_source = tests::NumberTestData::create();
    let ctx = FuseQueryContext::try_create_ctx(test_source.number_source_for_test()?)?;
    let mut pipeline = Pipeline::create();

    let a = test_source.number_source_transform_for_test(ctx, 8)?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.merge_processor()?;

    if let PlanNode::Limit(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .limit(2)?
        .build()?
    {
        pipeline.add_simple_transform(|| Ok(Box::new(LimitTransform::try_create(plan.n)?)))?;
    }

    let stream = pipeline.execute().await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(2, rows);
    Ok(())
}
