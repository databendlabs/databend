// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::processors::*;
    use crate::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

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

    let stream = pipeline.execute().await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(2, rows);
    Ok(())
}
