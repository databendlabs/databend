// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_projection() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use std::sync::Arc;

    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create();
    let a = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    if let PlanNode::Projection(plan) =
        PlanBuilder::create(ctx.clone(), test_source.number_schema_for_test()?)
            .project(vec![field("number"), field("number")])?
            .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                ctx.clone(),
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
