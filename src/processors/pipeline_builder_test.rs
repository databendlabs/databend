// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_pipeline_build() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from system.numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "LimitTransform × 1 processor\
    \n  AggregatorFinalTransform × 1 processor\
    \n    Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
    \n      AggregatorPartialTransform × 8 processors\
    \n        FilterTransform × 8 processors\
    \n          SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_pipeline_build() -> crate::error::FuseQueryResult<()> {
    use pretty_assertions::assert_eq;

    use crate::clusters::*;
    use crate::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context()?;
    let cpus = ctx.get_max_threads()?;

    // Add node1 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node1".to_string(),
        cpus: 4,
        address: "127.0.0.1:9001".to_string(),
    })?;

    // Add node2 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node2".to_string(),
        cpus: 4,
        address: "127.0.0.1:9002".to_string(),
    })?;

    // Add node3 to cluster.
    ctx.try_get_cluster()?.add_node(&Node {
        id: "node3".to_string(),
        cpus: 4,
        address: "127.0.0.1:9003".to_string(),
    })?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;

    let plan = PlanParser::create(ctx_more_cpu.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from system.numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "LimitTransform × 1 processor\
    \n  AggregatorFinalTransform × 1 processor\
    \n    Merge (RemoteTransform × 4 processors) to (AggregatorFinalTransform × 1)\
    \n      RemoteTransform × 4 processor(s): AggregatorPartialTransform × 8 processors -> FilterTransform × 8 processors ->   SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
