// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_final_aggregator() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use common_planners::{self};
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // sum(number)+1, avg(number)
    let aggr_exprs = vec![add(sum(col("number")), lit(2u64)), avg(col("number"))];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(aggr_exprs.clone(), vec![])?
        .build()?;
    let aggr_final = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_final(aggr_exprs.clone(), vec![])?
        .build()?;

    // Pipeline.
    let source = test_source.number_source_transform_for_test(200000)?;
    let mut pipeline = Pipeline::create();
    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorPartialTransform::try_create(
            aggr_partial.schema(),
            aggr_exprs.clone()
        )?))
    })?;
    pipeline.merge_processor()?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorFinalTransform::try_create(
            aggr_final.schema(),
            aggr_exprs.clone()
        )?))
    })?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+----------------------+-------------+",
        "| plus(sum(number), 2) | avg(number) |",
        "+----------------------+-------------+",
        "| 19999900002          | 99999.5     |",
        "+----------------------+-------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
