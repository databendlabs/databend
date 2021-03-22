// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_executor_service_ping() -> Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    use crate::protobuf::executor_client::ExecutorClient;
    use crate::protobuf::{PingRequest, PingResponse};

    // Test service starts.
    let addr = crate::tests::try_start_service(1).await?[0].clone();

    let mut client = ExecutorClient::connect(format!("http://{}", addr)).await?;
    let request = tonic::Request::new(PingRequest {
        name: String::from("datafuse"),
    });
    let actual = client.ping(request).await?.into_inner();
    let expect = PingResponse {
        message: "Hello datafuse!".to_string(),
    };
    assert_eq!(actual, expect);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_executor_service_fetch_partition() -> Result<(), Box<dyn std::error::Error>> {
    use pretty_assertions::assert_eq;

    use crate::processors::PipelineBuilder;
    use crate::protobuf::executor_client::ExecutorClient;
    use crate::protobuf::{FetchPartitionRequest, FetchPartitionResponse, PartitionProto};
    use crate::sql::PlanParser;

    // 1. Service starts.
    let (addr, session_mgr) = crate::tests::try_start_service_with_session_mgr().await?;
    let ctx = session_mgr.try_create_context()?;

    // 2. Make some partitions for the current context.
    let plan =
        PlanParser::create(ctx.clone()).build_from_sql("select * from system.numbers_mt(80000)")?;
    let _pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;

    // 3. Fetch the partitions from the context by ID via the gRPC.
    let mut client = ExecutorClient::connect(format!("http://{}", addr)).await?;
    let request = tonic::Request::new(FetchPartitionRequest {
        uuid: ctx.get_id()?,
        nums: 1,
    });

    // 4. Check.
    let actual = client.fetch_partition(request).await?.into_inner();
    let expect = FetchPartitionResponse {
        partitions: vec![PartitionProto {
            name: "80000-75000-80000".to_string(),
            version: 0,
        }],
    };
    assert_eq!(actual, expect);

    Ok(())
}
