// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_final_groupby() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_planners::*;
    use common_planners::{self};
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // sum(number), avg(number)
    let aggr_exprs = &[sum(col("number")), avg(col("number"))];

    let group_exprs = &[col("number")];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(aggr_exprs, group_exprs)?
        .build()?;

    let aggr_final = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_final(
            test_source.number_schema_for_test()?,
            aggr_exprs,
            group_exprs,
        )?
        .build()?;

    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(5)?;
    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(GroupByPartialTransform::create(
            aggr_partial.schema(),
            aggr_exprs.to_vec(),
            group_exprs.to_vec(),
        )))
    })?;
    pipeline.merge_processor()?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(GroupByFinalTransform::create(
            aggr_final.schema(),
            aggr_exprs.to_vec(),
            group_exprs.to_vec(),
        )))
    })?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    // SELECT SUM(number), AVG(number), number ... GROUP BY numbers(5);
    let expected = vec![
        "+-------------+-------------+--------+",
        "| sum(number) | avg(number) | number |",
        "+-------------+-------------+--------+",
        "| 0           | 0           | 0      |",
        "| 1           | 1           | 1      |",
        "| 2           | 2           | 2      |",
        "| 3           | 3           | 3      |",
        "| 4           | 4           | 4      |",
        "+-------------+-------------+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
