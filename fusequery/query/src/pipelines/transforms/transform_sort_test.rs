// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_sort() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use common_planners::{self};
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // Pipeline.
    let mut pipeline = Pipeline::create();
    let a = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(a))?;

    let sort_expression = vec![sort("number", false, false)];
    let plan = PlanBuilder::create(test_source.number_schema_for_test()?)
        .sort(&sort_expression)?
        .build()?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(SortPartialTransform::try_create(
            plan.schema().clone(),
            sort_expression.clone(),
            None,
        )?))
    })?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(SortMergeTransform::try_create(
            plan.schema().clone(),
            sort_expression.clone(),
            None,
        )?))
    })?;

    if pipeline.last_pipe()?.nums() > 1 {
        pipeline.merge_processor()?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(SortMergeTransform::try_create(
                plan.schema().clone(),
                sort_expression.clone(),
                None,
            )?))
        })?;
    }

    // Result.
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
        "| 5      |",
        "| 4      |",
        "| 3      |",
        "| 2      |",
        "| 1      |",
        "| 0      |",
        "+--------+",
    ];
    crate::assert_blocks_eq!(expected, result.as_slice());

    Ok(())
}
