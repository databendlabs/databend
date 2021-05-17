// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());

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
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 7      |",
        "| 6      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
