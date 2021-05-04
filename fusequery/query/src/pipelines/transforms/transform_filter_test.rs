// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create();

    let source = test_source.number_source_transform_for_test(10000)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Filter(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .filter(col("number").eq(lit(2021)))?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                plan.predicate.clone(),
                false
            )?))
        })?;
    }
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 2021   |",
        "+--------+",
    ];
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter_error() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create();

    let source = test_source.number_source_transform_for_test(10000)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Filter(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .filter(col("not_found_filed").eq(lit(2021)))?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(FilterTransform::try_create(
                plan.predicate.clone(),
                false
            )?))
        })?;
    }
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await;
    let actual = format!("{:?}", result);
    let expect = "Err(Code: 1002, displayText = InvalidArgumentError(\"Unable to get field named \\\"not_found_filed\\\". Valid fields: [\\\"number\\\"]\").)";
    assert_eq!(expect, actual);

    Ok(())
}
