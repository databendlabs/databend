// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_exception::Result;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::pipelines::processors::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_display() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "explain pipeline select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
    let pipeline_builder = PipelineBuilder::create(ctx.clone());
    let pipeline = pipeline_builder.build(plan.input(0).as_ref())?;
    let expect = "LimitTransform × 1 processor\
    \n  ProjectionTransform × 1 processor\
    \n    ExpressionTransform × 1 processor\
    \n      AggregatorFinalTransform × 1 processor\
    \n        Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
    \n          AggregatorPartialTransform × 8 processors\
    \n            ExpressionTransform × 8 processors\
    \n              FilterTransform × 8 processors\
    \n                SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
