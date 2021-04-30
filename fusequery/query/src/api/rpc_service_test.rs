// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_flight_timeout() -> anyhow::Result<()> {
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::api::rpc::*;
    use crate::configs::*;

    let mut conf = Config::default();
    conf.rpc_server_timeout_second = 1;

    // Test service starts.
    let addr = crate::tests::try_start_service_with_config(&conf, 3).await?[0].clone();

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(&PlanNode::ReadSource(
        test_source.number_read_source_plan_for_test(10000000000)?
    ))
    .build()?;

    let mut client = FlightClient::try_create(addr.to_string()).await?;

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
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}
