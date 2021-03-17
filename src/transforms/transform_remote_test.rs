// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_remote_with_local() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::datavalues::*;
    use crate::planners::*;
    use crate::processors::*;
    use crate::transforms::*;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let remote_addr = crate::tests::try_start_service(1).await?[0].clone();

    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(100)?),
    )
    .filter(col("number").eq(lit(99)))?
    .build()?;

    let remote = RemoteTransform::try_create(ctx.get_id()?, remote_addr, plan)?;
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
async fn test_transform_remote_with_cluster() -> crate::error::FuseQueryResult<()> {
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;

    use crate::datavalues::*;
    use crate::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context_with_nodes(3).await?;
    let plan = PlanParser::create(ctx.clone())
        .build_from_sql("select sum(number+1)+2 as sumx from system.numbers_mt(100000)")?;

    // Check the distributed plan.
    let expect = "AggregatorFinal: groupBy=[[]], aggr=[[(sum([(number + 1)]) + 2) as sumx]]\
    \n  RedistributeStage[state: AggregatorMerge, id: 0]\
    \n    AggregatorPartial: groupBy=[[]], aggr=[[(sum([(number + 1)]) + 2) as sumx]]\
    \n      ReadDataSource: scan parts [8](Read from system.numbers_mt table, Read Rows:100000, Read Bytes:800000)";
    let actual = format!("{:?}", plan);
    assert_eq!(expect, actual);

    let mut pipeline = PipelineBuilder::create(ctx, plan).build()?;

    // Check the distributed pipeline.
    let expect = format!("{:?}", pipeline);
    let actual = "AggregatorFinalTransform × 1 processor\
    \n  Merge (RemoteTransform × 3 processors) to (AggregatorFinalTransform × 1)\
    \n    RemoteTransform × 3 processors";
    assert_eq!(expect, actual);

    let mut stream = pipeline.execute().await?;
    while let Some(v) = stream.next().await {
        let v = v?;
        let expect = &UInt64Array::from(vec![5000050002]);
        let actual = v.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(expect.clone(), actual.clone());
    }
    Ok(())
}
