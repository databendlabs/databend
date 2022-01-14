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
use databend_query::pipelines::processors::*;
use databend_query::pipelines::transforms::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());

    let source = test_source.number_source_transform_for_test(10000)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Filter(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .filter(col("number").eq(lit(2021)))?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(WhereTransform::try_create(
                plan.input.schema(),
                plan.predicate.clone(),
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
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_filter_error() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx);

    let source = test_source.number_source_transform_for_test(10000)?;
    pipeline.add_source(Arc::new(source))?;

    let plan = PlanBuilder::create(test_source.number_schema_for_test()?)
        .filter(col("not_found_filed").eq(lit(2021)))
        .and_then(|x| x.build())?;

    if let PlanNode::Filter(plan) = plan {
        let result = WhereTransform::try_create(plan.schema(), plan.predicate);
        let actual = format!("{}", result.err().unwrap());
        let expect = "Code: 1006, displayText = Unable to get field named \"not_found_filed\". Valid fields: [\"number\"].";
        assert_eq!(expect, actual);
    }
    Ok(())
}
