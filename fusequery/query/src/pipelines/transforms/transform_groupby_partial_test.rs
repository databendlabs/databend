// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_partial_groupby() -> anyhow::Result<()> {
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
    let aggr_exprs = vec![sum(col("number")), avg(col("number"))];
    let group_exprs = vec![col("number")];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(&aggr_exprs, &group_exprs)?
        .build()?;

    // Pipeline.
    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(5)?;
    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(GroupByPartialTransform::create(
            aggr_partial.schema(),
            aggr_exprs.clone(),
            group_exprs.clone(),
        )))
    })?;
    pipeline.merge_processor()?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);

    // SELECT SUM(number), AVG(number), number ... GROUP BY number;
    let expected = vec![
        "+---------------------------+-----------------------------------------------------+---------------------------+------------------+",
        "| sum(number)               | avg(number)                                         | _group_keys               | _group_by_key    |",
        "+---------------------------+-----------------------------------------------------+---------------------------+------------------+",
        "| {\"Struct\":[{\"UInt64\":0}]} | {\"Struct\":[{\"Struct\":[{\"UInt64\":0},{\"UInt64\":1}]}]} | {\"Struct\":[{\"UInt64\":0}]} | 0000000000000000 |",
        "| {\"Struct\":[{\"UInt64\":1}]} | {\"Struct\":[{\"Struct\":[{\"UInt64\":1},{\"UInt64\":1}]}]} | {\"Struct\":[{\"UInt64\":1}]} | 0100000000000000 |",
        "| {\"Struct\":[{\"UInt64\":2}]} | {\"Struct\":[{\"Struct\":[{\"UInt64\":2},{\"UInt64\":1}]}]} | {\"Struct\":[{\"UInt64\":2}]} | 0200000000000000 |",
        "| {\"Struct\":[{\"UInt64\":3}]} | {\"Struct\":[{\"Struct\":[{\"UInt64\":3},{\"UInt64\":1}]}]} | {\"Struct\":[{\"UInt64\":3}]} | 0300000000000000 |",
        "| {\"Struct\":[{\"UInt64\":4}]} | {\"Struct\":[{\"Struct\":[{\"UInt64\":4},{\"UInt64\":1}]}]} | {\"Struct\":[{\"UInt64\":4}]} | 0400000000000000 |",
        "+---------------------------+-----------------------------------------------------+---------------------------+------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
