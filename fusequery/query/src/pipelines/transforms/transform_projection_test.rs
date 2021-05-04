// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_projection() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create();
    let source = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Projection(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .project(vec![col("number"), col("number")])?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.schema.clone(),
                plan.expr.clone()
            )?))
        })?;
    }

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+--------+--------+",
        "| number | number |",
        "+--------+--------+",
        "| 7      | 7      |",
        "| 6      | 6      |",
        "| 5      | 5      |",
        "| 4      | 4      |",
        "| 3      | 3      |",
        "| 2      | 2      |",
        "| 1      | 1      |",
        "| 0      | 0      |",
        "+--------+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
