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
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::*;
use databend_query::pipelines::processors::*;
use databend_query::pipelines::transforms::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit_by() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());

    let a = test_source.number_source_transform_for_test(12)?;
    pipeline.add_source(Arc::new(a))?;

    pipeline.merge_processor()?;

    if let PlanNode::Expression(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .expression(&[modular(col("number"), lit(3)), col("number")], "")?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone(),
            )?))
        })?;

        pipeline.add_simple_transform(|| {
            Ok(Box::new(LimitByTransform::create(2, vec![col(
                "(number % 3)",
            )])))
        })?;

        // make col("number % 3") be the first column, then we will have a well-sorted block to test
        // col("number") is useless, and it may have unstable results so we don't need it any more
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                plan.schema(),
                DataSchemaRefExt::create(vec![col("(number % 3)").to_data_field(&plan.schema())?]),
                vec![col("(number % 3)"), col("number")],
            )?))
        })?;
    }

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------------+",
        "| (number % 3) |",
        "+--------------+",
        "| 0            |",
        "| 0            |",
        "| 1            |",
        "| 1            |",
        "| 2            |",
        "| 2            |",
        "+--------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
