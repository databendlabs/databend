// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::pipelines::processors::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_walker() -> anyhow::Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;

    // PreOrder.
    {
        let mut actual: Vec<String> = vec![];
        pipeline.walk_preorder(|pipe| {
            let processor = pipe.processor_by_index(0).clone();
            actual.push(processor.name().to_string() + " x " + &*format!("{}", pipe.nums()));
            Ok(true)
        })?;

        let expect = vec![
            "LimitTransform x 1".to_string(),
            "ProjectionTransform x 1".to_string(),
            "ExpressionTransform x 1".to_string(),
            "AggregatorFinalTransform x 1".to_string(),
            "MergeProcessor x 1".to_string(),
            "AggregatorPartialTransform x 8".to_string(),
            "ExpressionTransform x 8".to_string(),
            "FilterTransform x 8".to_string(),
            "SourceTransform x 8".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    // PostOrder.
    {
        let mut actual: Vec<String> = vec![];
        pipeline.walk_postorder(|pipe| {
            let processor = pipe.processor_by_index(0).clone();
            actual.push(processor.name().to_string() + " x " + &*format!("{}", pipe.nums()));
            Ok(true)
        })?;

        let expect = vec![
            "SourceTransform x 8".to_string(),
            "FilterTransform x 8".to_string(),
            "ExpressionTransform x 8".to_string(),
            "AggregatorPartialTransform x 8".to_string(),
            "MergeProcessor x 1".to_string(),
            "AggregatorFinalTransform x 1".to_string(),
            "ExpressionTransform x 1".to_string(),
            "ProjectionTransform x 1".to_string(),
            "LimitTransform x 1".to_string(),
        ];
        assert_eq!(expect, actual);
    }
    Ok(())
}
