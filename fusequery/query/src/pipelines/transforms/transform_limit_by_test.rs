// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit_by() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::DataSchemaRefExt;
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());

    let a = test_source.number_source_transform_for_test(12)?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.merge_processor()?;

    if let PlanNode::Expression(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .expression(&[modular(col("number"), lit(3)), col("number")], "")?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone(),
            )?))
        })?;

        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitByTransform::create(2, vec![col(
                "(number % 3)",
            )])))
        })?;

        // make col("number % 3") be the first column, then we will have a well-sorted block to test
        // col("number") is useless, and it may have unstable results so we don't need it any more
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.schema(),
                DataSchemaRefExt::create(vec![col("(number % 3)").to_data_field(&plan.schema())?]),
                vec![col("(number % 3)"), col("number")],
            )?))
        })?;
    }

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------------+",
        "| (number % 3) |",
        "+--------------+",
        "| 0            |",
        "| 0            |",
        "| 1            |",
        "| 1            |",
        "| 2            |",
        "| 2            |",
        "+--------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
