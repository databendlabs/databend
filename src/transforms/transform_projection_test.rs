// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_projection() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;
    use tokio::stream::StreamExt;

    use crate::contexts::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::testdata;
    use crate::transforms::*;

    let test_source = testdata::NumberTestData::create();
    let ctx = Arc::new(FuseQueryContext::create_ctx(
        test_source.number_source_for_test()?,
    ));

    let mut pipeline = Pipeline::create();
    let a = test_source.number_source_transform_for_test(ctx, 8)?;
    pipeline.add_source(Arc::new(a))?;

    if let PlanNode::Projection(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .project(vec![field("number"), field("number")])?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.schema.clone(),
                plan.expr.clone(),
            )?))
        })?;
    }

    let mut stream = pipeline.execute().await?;
    let v = stream.next().await.unwrap().unwrap();
    let actual = v.num_columns();
    let expect = 2;
    assert_eq!(expect, actual);
    Ok(())
}
