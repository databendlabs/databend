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
async fn test_transform_expression() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Expression(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .expression(
            &[col("number"), col("number"), add(col("number"), lit(1u8))],
            "",
        )?
        .build()?
    {
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ExpressionTransform::try_create(
                plan.input.schema(),
                plan.schema.clone(),
                plan.exprs.clone(),
            )?))
        })?;
    }

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);

    let expected = vec![
        "+--------+--------------+",
        "| number | (number + 1) |",
        "+--------+--------------+",
        "| 0      | 1            |",
        "| 1      | 2            |",
        "| 2      | 3            |",
        "| 3      | 4            |",
        "| 4      | 5            |",
        "| 5      | 6            |",
        "| 6      | 7            |",
        "| 7      | 8            |",
        "+--------+--------------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_expression_error() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx);
    let source = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(source))?;

    let result = PlanBuilder::create(test_source.number_schema_for_test()?).project(&[
        col("xnumber"),
        col("number"),
        add(col("number"), lit(1u8)),
    ]);
    let actual = format!("{}", result.err().unwrap());
    let expect =
        "Code: 1006, displayText = Unable to get field named \"xnumber\". Valid fields: [\"number\"].";
    assert_eq!(expect, actual);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_expression_issue2857() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());
    let source = test_source.number_source_transform_for_test(8)?;
    pipeline.add_source(Arc::new(source))?;

    if let PlanNode::Projection(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
        .project(&[
            col("number").alias("number"),
            add(col("number"), lit(1u8)).alias("number1"),
        ])?
        .build()?
    {
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
        "+--------+---------+",
        "| number | number1 |",
        "+--------+---------+",
        "| 0      | 1       |",
        "| 1      | 2       |",
        "| 2      | 3       |",
        "| 3      | 4       |",
        "| 4      | 5       |",
        "| 5      | 6       |",
        "| 6      | 7       |",
        "| 7      | 8       |",
        "+--------+---------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
