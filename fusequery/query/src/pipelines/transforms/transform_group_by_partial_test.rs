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
    assert_eq!(block.num_columns(), 3);

    // SELECT SUM(number), AVG(number), number ... GROUP BY number;
    // binary-state
    let expected = vec![
        "+--------------------+----------------------------------+---------------+",
        "| sum(number)        | avg(number)                      | _group_by_key |",
        "+--------------------+----------------------------------+---------------+",
        "| 010000000000000000 | 00000000000000000100000000000000 | 0             |",
        "| 010100000000000000 | 01000000000000000100000000000000 | 1             |",
        "| 010200000000000000 | 02000000000000000100000000000000 | 2             |",
        "| 010300000000000000 | 03000000000000000100000000000000 | 3             |",
        "| 010400000000000000 | 04000000000000000100000000000000 | 4             |",
        "+--------------------+----------------------------------+---------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
