// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_service() -> Result<(), Box<dyn std::error::Error>> {
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::planners::*;
    use crate::rpcs::rpc::*;

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(111)?),
    )
    .build()?;

    let mut client = FlightClient::try_create(addr.to_string()).await?;
    let action = ExecuteAction::ExecutePlan(ExecutePlanAction::create("xx".to_string(), plan));
    let stream = client.execute(&action).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
    assert_eq!(111, rows);

    Ok(())
}
