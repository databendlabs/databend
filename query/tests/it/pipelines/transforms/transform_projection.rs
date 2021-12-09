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
async fn test_transform_projection() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Projection(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .project(&[col("number"), col("number")])?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.expr.clone(),
            )?))
        })?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.expr.clone(),
            )?))
        })?;
    }

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+--------+--------+",
        "| number | number |",
        "+--------+--------+",
        "| 7      | 7      |",
        "| 6      | 6      |",
        "| 5      | 5      |",
        "| 4      | 4      |",
        "| 3      | 3      |",
        "| 2      | 2      |",
        "| 1      | 1      |",
        "| 0      | 0      |",
        "+--------+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
