// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_execute() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::api::rpc::*;

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(&PlanNode::ReadSource(
        test_source.number_read_source_plan_for_test(111)?,
    ))
    .build()?;

    let mut client = FlightClient::try_create(addr.to_string()).await?;
    let stream = client.execute_remote_plan("xx".to_string(), &plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(111, rows);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_fetch_partition_action() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::api::rpc::*;
    use crate::pipelines::processors::PipelineBuilder;
    use crate::sql::PlanParser;

    // 1. Service starts.
    let (addr, session_mgr) = crate::tests::try_start_service_with_session_mgr().await?;
    let ctx = session_mgr.try_create_context()?;

    // 2. Make some partitions for the current context.
    let plan = PlanParser::create(ctx.clone()).build_from_sql("select * from numbers_mt(80000)")?;
    let _pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;

    // 3. Fetch the partitions from the context by uuid.
    let mut client = FlightClient::try_create(addr.to_string()).await?;
    let actual = client.fetch_partition_action(ctx.get_id()?, 1).await?;

    // 4. Check.
    assert_eq!(1, actual.len());

    Ok(())
}
