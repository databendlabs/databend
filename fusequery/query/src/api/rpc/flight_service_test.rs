// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_execute() -> anyhow::Result<()> {
    use common_flights::*;
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(&PlanNode::ReadSource(
        test_source.number_read_source_plan_for_test(5)?,
    ))
    .build()?;

    let mut client = QueryClient::try_create(addr.to_string()).await?;

    let stream = client
        .execute_remote_plan_action("xx".to_string(), &plan)
        .await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "| 3      |",
        "| 4      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_empty_source() -> anyhow::Result<()> {
    use common_flights::*;
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(&PlanNode::ReadSource(
        test_source.number_read_source_plan_for_test(0)?,
    ))
    .build()?;

    let mut client = QueryClient::try_create(addr.to_string()).await?;

    let stream = client
        .execute_remote_plan_action("xx".to_string(), &plan)
        .await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    assert_eq!(result.len(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_fetch_partition_action() -> anyhow::Result<()> {
    use common_flights::*;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::PipelineBuilder;
    use crate::sql::PlanParser;

    // 1. Service starts.
    let (addr, session_mgr) = crate::tests::try_start_service_with_session_mgr().await?;
    let ctx = session_mgr.try_create_context()?;

    // 2. Make some partitions for the current context.
    let plan = PlanParser::create(ctx.clone()).build_from_sql("select * from numbers_mt(80000)")?;
    let _pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;

    // 3. Fetch the partitions from the context by uuid.
    let mut client = QueryClient::try_create(addr.to_string()).await?;
    let actual = client.fetch_partition_action(ctx.get_id()?, 1).await?;

    // 4. Check.
    assert_eq!(1, actual.len());

    Ok(())
}

#[tokio::test]
async fn test_flight_client_timeout() -> anyhow::Result<()> {
    use common_flights::*;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::PipelineBuilder;
    use crate::sql::PlanParser;

    // 1. Service starts.
    let (addr, session_mgr) = crate::tests::try_start_service_with_session_mgr().await?;
    let ctx = session_mgr.try_create_context()?;

    // 2. Make some partitions for the current context.
    let plan = PlanParser::create(ctx.clone()).build_from_sql("select * from numbers_mt(80000)")?;
    let _pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;

    // 3. Fetch the partitions from the context by uuid.
    let mut client = QueryClient::try_create(addr.to_string()).await?;
    client.set_timeout(0);
    let actual = client.fetch_partition_action(ctx.get_id()?, 1).await;
    let expect = "Err(status: Cancelled, message: \"Timeout expired\", details: [], metadata: MetadataMap { headers: {} })";
    assert_eq!(expect, format!("{:?}", actual));

    Ok(())
}
