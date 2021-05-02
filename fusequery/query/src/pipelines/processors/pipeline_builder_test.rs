// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_pipeline_build() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 having (number+1)=4 limit 1"
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "LimitTransform × 1 processor\
    \n  FilterTransform × 1 processor\
    \n    AggregatorFinalTransform × 1 processor\
    \n      Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
    \n        AggregatorPartialTransform × 8 processors\
    \n          FilterTransform × 8 processors\
    \n            SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_pipeline_build() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context_with_nodes(3).await?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;

    let plan = PlanParser::create(ctx_more_cpu.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1"
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "LimitTransform × 1 processor\
    \n  AggregatorFinalTransform × 1 processor\
    \n    Merge (RemoteTransform × 3 processors) to (AggregatorFinalTransform × 1)\
    \n      RemoteTransform × 3 processor(s): AggregatorPartialTransform × 8 processors -> FilterTransform × 8 processors ->   SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
