// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use common_planners::*;
use common_planners::{self};
use databend_query::pipelines::processors::*;
use databend_query::pipelines::transforms::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_partial_aggregator() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    // sum(number), avg(number)
    let aggr_exprs = &[sum(col("number")), avg(col("number"))];
    let aggr_partial = PlanBuilder::create(test_source.number_schema_for_test()?)
        .aggregate_partial(aggr_exprs, &[])?
        .build()?;

    // Pipeline.
    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(200000)?;
    let source_schema = test_source.number_schema_for_test()?;
    pipeline.add_source(Arc::new(source))?;

    pipeline.add_simple_transform(|| {
        Ok(Box::new(AggregatorPartialTransform::try_create(
            aggr_partial.schema(),
            source_schema.clone(),
            aggr_exprs.to_vec(),
        )?))
    })?;
    pipeline.merge_processor()?;

    // Result.
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    // BinaryState
    let expected = vec![
        "+------------------+----------------------------------+",
        "| sum(number)      | avg(number)                      |",
        "+------------------+----------------------------------+",
        "| 604116a804000000 | 604116a804000000400d030000000000 |",
        "+------------------+----------------------------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
