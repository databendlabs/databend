// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::*;
use common_planners::{self};
use common_runtime::tokio;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::pipelines::processors::*;
use crate::pipelines::transforms::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_final_aggregator() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // sum(number)+1, avg(number)
    let aggr_exprs = &[sum(col("number")), avg(col("number"))];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(aggr_exprs, &[])?
        .build()?;
    let aggr_final = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_final(test_source.number_schema_for_test()?, aggr_exprs, &[])?
        .build()?;

    // Pipeline.
    let source = test_source.number_source_transform_for_test(200000)?;
    let source_schema = test_source.number_schema_for_test()?;

    let mut pipeline = Pipeline::create(ctx.clone());
    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorPartialTransform::try_create(
            aggr_partial.schema(),
            source_schema.clone(),
            aggr_exprs.to_vec(),
        )?))
    })?;
    pipeline.merge_processor()?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorFinalTransform::try_create(
            aggr_final.schema(),
            source_schema.clone(),
            aggr_exprs.to_vec(),
        )?))
    })?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+-------------+-------------+",
        "| sum(number) | avg(number) |",
        "+-------------+-------------+",
        "| 19999900000 | 99999.5     |",
        "+-------------+-------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
