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
async fn test_transform_partial_group_by() -> Result<()> {
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
    let source_schema = test_source.number_schema_for_test()?;

    pipeline.add_source(Arc::new(source))?;
    pipeline.add_simple_transform(|| {
        Ok(Box::new(GroupByPartialTransform::create(
            aggr_partial.schema(),
            source_schema.clone(),
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
    // binary-state
    let expected = vec![
        "+--------------------------+------------------------------------------------------------------------------+--------+------------------+",
        "| sum(number)              | avg(number)                                                                  | number | _group_by_key      |",
        "+--------------------------+------------------------------------------------------------------------------+--------+------------------+",
        "| 7b2255496e743634223a307d | 7b22537472756374223a5b7b2255496e743634223a307d2c7b2255496e743634223a317d5d7d | 0      | 0000000000000000 |",
        "| 7b2255496e743634223a317d | 7b22537472756374223a5b7b2255496e743634223a317d2c7b2255496e743634223a317d5d7d | 1      | 0100000000000000 |",
        "| 7b2255496e743634223a327d | 7b22537472756374223a5b7b2255496e743634223a327d2c7b2255496e743634223a317d5d7d | 2      | 0200000000000000 |",
        "| 7b2255496e743634223a337d | 7b22537472756374223a5b7b2255496e743634223a337d2c7b2255496e743634223a317d5d7d | 3      | 0300000000000000 |",
        "| 7b2255496e743634223a347d | 7b22537472756374223a5b7b2255496e743634223a347d2c7b2255496e743634223a317d5d7d | 4      | 0400000000000000 |",
        "+--------------------------+------------------------------------------------------------------------------+--------+------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
