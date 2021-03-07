// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_service() -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream::StreamExt;
    use tonic::transport::Server;

    use crate::clusters::Cluster;
    use crate::configs::Config;
    use crate::planners::*;
    use crate::rpcs::rpc::*;
    use crate::sessions::Session;

    let addr = "127.0.0.1:50052";
    let socket = addr.parse::<std::net::SocketAddr>()?;

    let conf = Config::default();
    let cluster = Cluster::create(conf.clone());
    let session_manager = Session::create();
    let srv = FlightService::create(conf, cluster, session_manager);

    tokio::spawn(async move {
        Server::builder()
            .add_service(srv.make_server())
            .serve(socket)
            .await
            .unwrap()
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let ctx = crate::tests::try_create_context()?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());
    let plan = PlanBuilder::from(
        ctx.clone(),
        &PlanNode::ReadSource(test_source.number_read_source_plan_for_test(10000)?),
    )
    .build()?;

    let mut client = FlightClient::try_create(addr.to_string()).await?;
    let action = ExecuteAction::ExecutePlan(ExecutePlanAction::create("xx".to_string(), plan));
    let mut stream = client.execute(&action).await?;
    while let Some(v) = stream.next().await {
        print!("{:?}", v);
    }

    Ok(())
}
