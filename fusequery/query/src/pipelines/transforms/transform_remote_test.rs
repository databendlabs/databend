// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_remote_with_local() -> anyhow::Result<()> {
    use common_datavalues::*;
    use common_planners::*;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::pipelines::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let remote_addr = crate::tests::try_start_service(1).await?[0].clone();

    let plan = PlanBuilder::from(&PlanNode::ReadSource(
        test_source.number_read_source_plan_for_test(100)?,
    ))
    .filter(col("number").eq(lit(99)))?
    .build()?;

    let remote = RemoteTransform::try_create(ctx.clone(), ctx.get_id()?, remote_addr, plan)?;
    let mut stream = remote.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        let expect = &UInt64Array::from(vec![99]);
        assert_eq!(expect.clone(), actual.clone());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_remote_with_cluster() -> anyhow::Result<()> {
    use common_datavalues::*;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::sql::*;

    let ctx_more_cpu_for_partitions_generate = crate::tests::try_create_context()?;
    ctx_more_cpu_for_partitions_generate.set_max_threads(40)?;

    let ctx = crate::tests::try_create_context_with_nodes(3).await?;
    let plan = PlanParser::create(ctx_more_cpu_for_partitions_generate.clone())
        .build_from_sql("select sum(number+1)+2 as sumx from numbers_mt(1000000)")?;

    // Check the distributed plan.
    let expect = "AggregatorFinal: groupBy=[[]], aggr=[[(sum([(number + 1)]) + 2) as sumx]]\
    \n  RedistributeStage[state: AggregatorMerge, id: 0]\
    \n    AggregatorPartial: groupBy=[[]], aggr=[[(sum([(number + 1)]) + 2) as sumx]]\
    \n      ReadDataSource: scan partitions: [40], scan schema: [number:UInt64], statistics: [read_rows: 1000000, read_bytes: 8000000]";
    let actual = format!("{:?}", plan);
    assert_eq!(expect, actual);

    let mut pipeline = PipelineBuilder::create(ctx, plan).build()?;

    // Check the distributed pipeline.
    let expect = format!("{:?}", pipeline);
    let actual = "AggregatorFinalTransform × 1 processor\
    \n  Merge (RemoteTransform × 3 processors) to (AggregatorFinalTransform × 1)\
    \n    RemoteTransform × 3 processor(s): AggregatorPartialTransform × 8 processors -> SourceTransform × 8 processors";
    assert_eq!(expect, actual);

    let mut stream = pipeline.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        let expect = &UInt64Array::from(vec![500000500002]);
        let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(expect.clone(), actual.clone());
    }
    Ok(())
}
